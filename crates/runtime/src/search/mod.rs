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

use arrow_schema::Schema;
use datafusion::logical_expr::sqlparser::ast::Expr;
use datafusion::{
    execution::SendableRecordBatchStream, physical_plan::EmptyRecordBatchStream,
    sql::TableReference,
};
use llms::embeddings::Embed;
use search::CandidateGeneration;

use crate::datafusion::DataFusion;

pub mod vector_search;

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
}

#[async_trait::async_trait]
impl CandidateGeneration for VectorGeneration {
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        projection: &[&Expr],
    ) -> Result<SendableRecordBatchStream, search::Error> {
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::new(
            Schema::empty(),
        ))))
    }

    fn supports_filters_pushdown(&self, _filters: &[&Expr]) -> Result<Vec<bool>, search::Error> {
        Ok(vec![])
    }

    /// Whether additional columns of the underlying source can also be retrieved during generation.
    fn supports_columns(&self, _projection: &[&Expr]) -> Result<Vec<bool>, search::Error> {
        Ok(vec![])
    }
}
