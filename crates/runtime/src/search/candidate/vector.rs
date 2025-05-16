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
use std::sync::Arc;

use crate::search::Error as VectorSearchError;
use async_openai::types::EmbeddingInput;
use datafusion::logical_expr::sqlparser::ast::Expr;
use datafusion::{execution::SendableRecordBatchStream, sql::TableReference};
use llms::embeddings::Embed;
use search::CandidateGeneration;
use snafu::ResultExt;
use tract_core::tract_data::itertools::Itertools;

use crate::datafusion::DataFusion;

// Distance column name for the vector search query.
// static VECTOR_DISTANCE_COLUMN_NAME: &str = "dist";
// Surrogate unique identifier name to use when no primary keys are provided.
static VSS_TEMP_GEN_ID_COLUMN: &str = "vss_temp_gen_id";
// Temporary table name to provide surrogate unique id for vector search query when no primary keys are provided.
static VSS_TEMP_TABLE_NAME: &str = "vss_temp_table";

pub struct VectorGeneration {
    df: Arc<DataFusion>,
    tbl: TableReference,
    embed: Arc<dyn Embed>,
    primary_keys: Vec<String>,
    embedding_column: String,
    is_chunked: bool,
}

impl VectorGeneration {
    pub fn new(
        df: &Arc<DataFusion>,
        tbl: &TableReference,
        embed: &Arc<dyn Embed>,
        primary_keys: &[String],
        embedding_column: &str,
        is_chunked: bool,
    ) -> Self {
        Self {
            df: Arc::clone(df),
            tbl: tbl.clone(),
            embed: Arc::clone(embed),
            primary_keys: primary_keys.to_vec(),
            embedding_column: embedding_column.to_string(),
            is_chunked,
        }
    }

    /// Embed the input text using the specified embedding model.
    async fn embed_query(&self, query: &str) -> Result<Vec<f32>, VectorSearchError> {
        self.embed
            .embed(EmbeddingInput::String(query.to_string()))
            .await
            .boxed()
            .map_err(|e| VectorSearchError::EmbeddingError { source: e })?
            .first()
            .cloned()
            .ok_or(VectorSearchError::EmbeddingError {
                source: Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "No embeddings returned for input text '{query}'"
                )),
            })
    }
}

#[async_trait::async_trait]
impl CandidateGeneration for VectorGeneration {
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        projection: &[&Expr],
    ) -> Result<SendableRecordBatchStream, search::Error> {
        let embedding = self
            .embed_query(query.as_str())
            .await
            .boxed()
            .map_err(|e| search::Error::InternalError { source: e })?;

        let query = if self.is_chunked {
            return Err(search::Error::InternalError {
                source: Box::from(format!("We just haven't implemented this yet")),
            });
        } else {
            format!(
                "SELECT * FROM (
                        SELECT
                            {projection_str},
                            cosine_distance({embedding_column}_embedding, {embedding:?}) as 'score'
                        FROM {tbl}
                        {where_str}
                    ) subq
                    WHERE 'score' IS NOT NULL
                    ORDER BY 'score' DESC",
                projection_str = projection.iter().map(|e| format!("{}", *e)).join(", "),
                embedding_column = self.embedding_column,
                tbl = self.tbl,
                where_str = opt_filters.iter().map(|e| format!("{}", *e)).join(" AND "),
            )
        };
        tracing::trace!("running SQL: {query}");

        Ok(self
            .df
            .query_builder(&query)
            .build()
            .run()
            .await
            .boxed()
            .map_err(|e| search::Error::InternalError { source: e })?
            .data)
    }

    fn supports_filters_pushdown(&self, _filters: &[&Expr]) -> Result<Vec<bool>, search::Error> {
        Ok(vec![])
    }

    /// Whether additional columns of the underlying source can also be retrieved during generation.
    fn supports_columns(&self, _projection: &[&Expr]) -> Result<Vec<bool>, search::Error> {
        Ok(vec![])
    }
}
