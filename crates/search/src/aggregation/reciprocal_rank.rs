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

use crate::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME, collect_batches};

use super::{CandidateAggregation, DatafusionSnafu, InconsistentColumnsSnafu};
use super::{Error, Result};

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use snafu::ResultExt;

/// Reciprocal Rank Fusion (RRF) is a method for combining multiple ranked sets of search results.
/// The underlying score of the search results is not important, only the rank (per stream order).
/// The rank, for a given entry (for some primary key `a`) is converted to a score using the formula:
/// ```
/// score_a = 1 / (rank_i + offset) + 1 / (rank_j + offset) + ...
/// ```
/// Where `rank_i` is the rank of the i-th stream, and `offset` is a constant (e.g. 60).
pub struct ReciprocalRankFusion;

#[async_trait]
impl CandidateAggregation for ReciprocalRankFusion {
    async fn aggregate(
        &self,
        mut candidate_sets: Vec<SendableRecordBatchStream>,
        primary_key: Vec<String>,
        limit: usize,
    ) -> Result<SendableRecordBatchStream> {
        // Handle 0, or 1 candidates.
        if candidate_sets.len() <= 1 {
            return candidate_sets.pop().ok_or(Error::NoCandidatesGenerated);
        }

        let schema = verify_schema_compatibility(candidate_sets.as_slice())?;

        let ctx = SessionContext::new();
        let mut table_names: Vec<String> = Vec::with_capacity(candidate_sets.len());

        // Inefficient, but collect each stream, convert to [`MemTable`].
        for (i, s) in candidate_sets.into_iter().enumerate() {
            let data = collect_batches(s).await.context(DatafusionSnafu)?;
            let table =
                MemTable::try_new(Arc::clone(&schema), vec![data]).context(DatafusionSnafu)?;
            let table_name = format!("search_candidates_{i}");
            table_names.insert(i, table_name.clone());

            let _ = ctx
                .register_table(TableReference::bare(table_name), Arc::new(table))
                .context(DatafusionSnafu)?;
        }

        let additional_columns = additional_columns_of_schema(&schema, primary_key.as_slice());
        let sql = reciprocal_rank_fusion_sql(
            table_names.as_slice(),
            primary_key.as_slice(),
            additional_columns.as_slice(),
            60,
            limit,
        );
        tracing::debug!("Runnning SQL in standalone context: ```sql{sql}```");
        let df = ctx.sql(sql.as_str()).await.context(DatafusionSnafu)?;

        df.execute_stream().await.context(DatafusionSnafu)
    }
}

/// Returns a list of additional columns in the schema that are not part of the primary key or the expected
/// search columns (i.e. score or underlying value).
fn additional_columns_of_schema(schema: &SchemaRef, primary_key: &[String]) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter_map(|f| {
            let name = f.name();
            if [SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME].contains(&name.as_str())
                || primary_key.contains(f.name())
            {
                return None;
            }
            Some(name.clone())
        })
        .collect()
}

/// Verifies that all streams have the same schema and contain the required columns: [`SEARCH_VALUE_COLUMN_NAME`], [`SEARCH_SCORE_COLUMN_NAME`].
fn verify_schema_compatibility(streams: &[SendableRecordBatchStream]) -> Result<SchemaRef> {
    let Some(schema) = streams.first().map(|strm| strm.schema()) else {
        return Ok(Schema::empty().into());
    };

    for s in streams {
        if s.schema()
            .column_with_name(SEARCH_VALUE_COLUMN_NAME)
            .is_none()
        {
            return Err(Error::CandidateMissingRequiredColumn {
                col: SEARCH_VALUE_COLUMN_NAME.to_string(),
            });
        }

        if s.schema()
            .column_with_name(SEARCH_SCORE_COLUMN_NAME)
            .is_none()
        {
            return Err(Error::CandidateMissingRequiredColumn {
                col: SEARCH_SCORE_COLUMN_NAME.to_string(),
            });
        }

        if !schema.fields().contains(s.schema().fields())
            || !s.schema().fields().contains(schema.fields())
        {
            return InconsistentColumnsSnafu {
                s1: schema,
                s2: s.schema(),
            }
            .fail();
        }
    }

    Ok(schema)
}

/// Generates the SQL for the RRF aggregation.
fn reciprocal_rank_fusion_sql(
    tables: &[String],
    primary_key: &[String],
    additional_columns: &[String],
    offset: usize,
    limit: usize,
) -> String {
    // 1) Add explicit rank one CTE per table, ranking _only_ by the PK columns
    //
    // ```sql
    //    my_tbl AS (
    //      SELECT *,
    //             ROW_NUMBER() OVER (ORDER BY doc_id, section) AS rank
    //      FROM my_tbl
    //    ),
    // ```
    let pk_list = primary_key.join(", ");
    let cte_defs: String = tables
        .iter()
        .map(|tbl| {
            format!(
                "{tbl} AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (ORDER BY {pk_list}) AS rank
                    FROM {tbl}
                )"
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    // 2) Build the RRF sum. This is the rank for each row in each table. If a row (as defined by the PK) is missing, it contributes a score of 0.
    let fusion_sum: String = tables
        .iter()
        .map(|tbl| format!("coalesce(1.0/({tbl}.rank + {offset}), 0)"))
        .collect::<Vec<_>>()
        .join(" + ");

    // 3) Coalesce the PK columns and additional columns across all tables.
    //    Additional columns will be consistent due to join on primary keys
    //    (i.e. if two tables have a given column, the values for a row will be equal).
    let select_keys: String = coalesce_columns(
        [primary_key, additional_columns].concat().as_slice(),
        tables,
    );

    // 4) FULL OUTER JOINs across tables on all PK columns.
    let joins: String = tables[1..]
        .iter()
        .map(|tbl| {
            let cond = primary_key
                .iter()
                .map(|col| format!("{}.{} = {}.{}", tables[0], col, tbl, col))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("FULL OUTER JOIN {tbl} ON {cond}")
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "WITH
            {cte_defs}
         SELECT
           TRUNC({fusion_sum}, 6) AS {SEARCH_SCORE_COLUMN_NAME},
           {base}.{SEARCH_VALUE_COLUMN_NAME} as {SEARCH_VALUE_COLUMN_NAME},
           {select_keys}
         FROM {base}
         {joins}
         ORDER BY {SEARCH_SCORE_COLUMN_NAME} DESC
         LIMIT {limit};",
        base = tables[0]
    )
}

/// Coalesce the PK columns and additional columns across all tables:
///
/// ```sql
///    coalesce(bm25.doc_id, vector.doc_id, …) AS doc_id,
///    coalesce(bm25.section, vector.section, …) AS section
///  ```
fn coalesce_columns(cols: &[String], tables: &[String]) -> String {
    cols.iter()
        .map(|col| {
            format!(
                "coalesce({cols}) as {col}",
                cols = tables
                    .iter()
                    .map(|tbl| format!("{tbl}.{col}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join(",\n       ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_table_single_key() {
        let tables = vec!["bm25".to_string()];
        let key_cols = ["doc_id".to_string()];
        let sql = reciprocal_rank_fusion_sql(tables.as_slice(), key_cols.as_slice(), &[], 42, 3);
        let expected = "SELECT TRUNC((1.0 / (bm25.score + 42)), 6) as final_rank, bm25.score as bm25_rank, bm25.doc_id \
        FROM bm25  \
        ORDER BY final_rank DESC";
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_two_tables_single_key() {
        let tables = vec!["bm25".to_string(), "vector".to_string()];
        let key_cols = ["doc_id".to_string()];
        let sql = reciprocal_rank_fusion_sql(tables.as_slice(), key_cols.as_slice(), &[], 5, 3);
        let expected = "SELECT TRUNC((1.0 / (bm25.score + 5)) + (1.0 / (vector.score + 5)), 6) as final_rank, bm25.score as bm25_rank, vector.score as vector_rank, bm25.doc_id \
        FROM bm25 JOIN vector ON bm25.doc_id = vector.doc_id \
        ORDER BY final_rank DESC";
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_three_tables_composite_key() {
        let tables = vec!["t1".to_string(), "t2".to_string(), "t3".to_string()];
        let key_cols = ["doc_id".to_string(), "section".to_string()];
        let sql = reciprocal_rank_fusion_sql(tables.as_slice(), key_cols.as_slice(), &[], 100, 4);
        let expected = "SELECT TRUNC((1.0 / (t1.score + 100)) + (1.0 / (t2.score + 100)) + (1.0 / (t3.score + 100)), 6) as final_rank, t1.score as t1_rank, t2.score as t2_rank, t3.score as t3_rank, t1.doc_id, t1.section \
        FROM t1 JOIN t2 ON t1.doc_id = t2.doc_id AND t1.section = t2.section JOIN t3 ON t1.doc_id = t3.doc_id AND t1.section = t3.section \
        ORDER BY final_rank DESC";
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_multiple_keys_and_tables() {
        let tables = vec!["alpha".to_string(), "beta".to_string()];
        let key_cols = ["k1".to_string(), "k2".to_string(), "k3".to_string()];
        let sql = reciprocal_rank_fusion_sql(tables.as_slice(), key_cols.as_slice(), &[], 2, 4);
        let expected = "SELECT TRUNC((1.0 / (alpha.score + 2)) + (1.0 / (beta.score + 2)), 6) as final_rank, alpha.score as alpha_rank, beta.score as beta_rank, alpha.k1, alpha.k2, alpha.k3 \
        FROM alpha JOIN beta ON alpha.k1 = beta.k1 AND alpha.k2 = beta.k2 AND alpha.k3 = beta.k3 \
        ORDER BY final_rank DESC";
        assert_eq!(sql, expected);
    }
}
