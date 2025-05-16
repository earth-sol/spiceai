/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::{collections::HashMap, sync::Arc};

use super::request::SearchRequest;
use super::util::get_embedding_table;
use super::{Error, Result};
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::search::DataFusionSnafu;
use crate::search::candidate::vector::VectorGeneration;
use crate::search::types::VectorSearchTableResult;
use crate::search::util::collect_batches;
use crate::{datafusion::DataFusion, model::EmbeddingModelStore};
use datafusion::sql::sqlparser::ast::{Expr, Ident};
use datafusion::{common::Constraint, sql::TableReference};
use itertools::Itertools;
use search::CandidateGeneration;
use snafu::{OptionExt, ResultExt};
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use super::types::VectorSearchResult;

/// A Component that can perform vector search operations.
pub struct VectorSearch {
    pub df: Arc<DataFusion>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,

    // For tables, explicitly defined primary keys for datasets.
    // Are in [`ResolvedTableReference`] format.
    // Before use, must be resolved with spice defaults, `.resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)`.
    explicit_primary_keys: HashMap<TableReference, Vec<String>>,
}

impl VectorSearch {
    pub fn new(
        df: Arc<DataFusion>,
        embeddings: Arc<RwLock<EmbeddingModelStore>>,
        explicit_primary_keys: HashMap<TableReference, Vec<String>>,
    ) -> Self {
        VectorSearch {
            df,
            embeddings,
            explicit_primary_keys,
        }
    }

    pub async fn search(&self, req: &SearchRequest) -> Result<VectorSearchResult> {
        let SearchRequest {
            text: query,
            datasets: data_source_opt,
            limit,
            where_cond,
            additional_columns,
            keywords,
        } = req;

        let tables = match data_source_opt {
            Some(ts) => ts.iter().map(TableReference::from).collect(),
            None => self.user_tables_with_embeddings().await?,
        };

        if tables.is_empty() {
            return Err(Error::NoTablesWithEmbeddingsFound {});
        }

        let span = match Span::current() {
            span if matches!(span.metadata(), Some(metadata) if metadata.name() == "vector_search") => {
                span
            }
            _ => {
                tracing::span!(target: "task_history", tracing::Level::INFO, "vector_search", input = query)
            }
        };

        let vector_search_result = async {
            tracing::info!(target: "task_history", tables = tables.iter().join(","), limit = %limit, "labels");
            let table_primary_keys = self
                .get_primary_keys_with_overrides(&tables)
                .await?;


            // Search for each table is independent, but done in parallel.
            let search_futures = tables.into_iter().map(|tbl| {
                let keywords = keywords.clone();
                let primary_keys = table_primary_keys.get(&tbl).map_or(&[] as &[String], |v| v.as_slice());

                async move {
                    tracing::debug!("Running vector search for table {:#?}", tbl);

                    // Only support one embedding column per table.
                    let table_provider = self
                        .df
                        .get_table(&tbl)
                        .await
                        .ok_or(Error::DataSourcesNotFound {
                            data_source: vec![tbl.clone()],
                        })?;

                    let Some(embedding_table) = get_embedding_table(&table_provider).await else {
                        return Err(Error::CannotVectorSearchDataset {
                            data_source: tbl.clone()
                        });
                    };

                    let Some(embedding_column) = embedding_table.get_embedding_columns().first().cloned() else {
                        return Err(Error::NoEmbeddingColumns {
                            data_source: tbl.clone(),
                        });
                    };
                    let Some(model_name) = embedding_table.get_embedding_models_used().first().cloned() else {
                        return Err(Error::CannotVectorSearchDataset {
                            data_source: tbl.clone()
                        });
                    };

                    let Some(embed) = self.embeddings
                        .read()
                        .await
                        .iter()
                        .find_map(|(name, model)| {
                            if *name == model_name {
                                return Some(Arc::clone(&model))
                            }
                            return None
                        }) else {
                            return Err(Error::CannotVectorSearchDataset {
                                data_source: tbl.clone()
                            });
                        };


                    let filters = keywords.iter().map(|k| {
                        SearchRequest::validate_keyword_to_ilike(k, embedding_column.as_str() )
                    }).collect::<Result<Vec<Expr>>>()?;
                    let mut filter_refs: Vec<&Expr> = filters.iter().collect();

                    if let Some(filter_expr) = where_cond {
                        filter_refs.push(filter_expr);
                    }

                    let projection: Vec<Expr> = primary_keys
                        .iter()
                        .cloned()
                        .chain(Some(embedding_column.to_string()))
                        .chain(additional_columns.iter().cloned())
                        .unique()
                        .map(|s| Expr::Identifier(Ident::new(s)))
                        .collect();
                    let projection_refs: Vec<&Expr> = projection.iter().collect();

                    let generator = VectorGeneration::new(&self.df, &tbl, &embed, primary_keys, embedding_column.as_str(), embedding_table.is_chunked(embedding_column.as_str()));

                    let search_result = generator.search(query.clone(), filter_refs.as_slice(), projection_refs.as_slice()).await.map_err(|e| Error::CandidateGenerationError{source: e})?;

                    // TODO: Do not prematurely collect all results. https://github.com/spiceai/spiceai/issues/5848
                    let data = collect_batches(search_result).await.boxed().context(DataFusionSnafu)?;

                    // TODO: Filter results after the fact for filters that aren't supported by [`CandidateGeneration::supports_filter_pushdown`]. https://github.com/spiceai/spiceai/issues/5849

                    // TODO: Retrieve columns from projection that aren't provided by candidate generator (see [`CandidateGeneration::supports_columns`]) https://github.com/spiceai/spiceai/issues/5850

                    Ok((tbl, VectorSearchTableResult{data, primary_keys: primary_keys.to_vec(), embedding_column: embedding_column.clone(), additional_columns: additional_columns.clone()}))
                }
            }).collect::<Vec<_>>();

            let results = futures::future::try_join_all(search_futures).await?;
            let response: VectorSearchResult = results.into_iter().collect();
            tracing::info!(target: "task_history", captured_output = ?response);
            Ok(response)
        }.instrument(span.clone()).await;

        match vector_search_result {
            Ok(result) => Ok(result),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    pub async fn user_tables_with_embeddings(&self) -> Result<Vec<TableReference>> {
        let tables = self.df.get_user_table_names();
        let mut tables_with_embeddings = Vec::new();

        for t in tables {
            let table_provider = self
                .df
                .get_table(&t)
                .await
                // we should not fail here, as we are iterating over the tables that we know exist
                .context(super::DataSourceNotFoundSnafu { table: t.clone() })?;
            if get_embedding_table(&table_provider).await.is_some() {
                tables_with_embeddings.push(t);
            }
        }
        Ok(tables_with_embeddings)
    }

    async fn get_primary_keys(&self, table: &TableReference) -> Result<Vec<String>> {
        let tbl_ref = self
            .df
            .get_table(table)
            .await
            .context(super::DataSourcesNotFoundSnafu {
                data_source: vec![table.clone()],
            })?;

        let constraint_idx = tbl_ref
            .constraints()
            .map(|c| c.iter())
            .unwrap_or_default()
            .find_map(|c| match c {
                Constraint::PrimaryKey(columns) => Some(columns),
                Constraint::Unique(_) => None,
            })
            .cloned()
            .unwrap_or(Vec::new());

        tbl_ref
            .schema()
            .project(&constraint_idx)
            .map(|schema_projection| {
                schema_projection
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
            })
            .boxed()
            .context(DataFusionSnafu)
    }

    /// For a set of tables, get their primary keys. Attempt to determine the primary key(s) of the
    /// table from the [`TableProvider`] constraints, and if not provided, use the explicit primary
    /// keys defined in the spicepod configuration.
    async fn get_primary_keys_with_overrides(
        &self,
        tables: &[TableReference],
    ) -> Result<HashMap<TableReference, Vec<String>>> {
        let mut tbl_to_pks: HashMap<TableReference, Vec<String>> = HashMap::new();

        for tbl in tables {
            // `explicit_primary_keys` are [`ResolvedTableReference`], must resolve with spice defaults first.
            // Equivalent to using [`TableReference::resolve_eq`] on `explicit_primary_keys` keys.
            let resolved_tbl: TableReference = tbl
                .clone()
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                .into();
            let pks = self.get_primary_keys(&resolved_tbl).await?;
            if !pks.is_empty() {
                tbl_to_pks.insert(tbl.clone(), pks);
            } else if let Some(explicit_pks) = explicit_primary_keys.get(&resolved_tbl) {
                tbl_to_pks.insert(tbl.clone(), explicit_pks.clone());
            }
        }
        Ok(tbl_to_pks)
    }
}
