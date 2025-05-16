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

use app::App;
use arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::{datasource::TableProvider, sql::TableReference};
use datafusion_federation::FederatedTableProviderAdaptor;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

use crate::accelerated_table::AcceleratedTable;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

use crate::embeddings::table::EmbeddingTable;

pub(super) async fn collect_batches(
    mut stream: SendableRecordBatchStream,
) -> std::result::Result<Vec<RecordBatch>, DataFusionError> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }

    Ok(batches)
}

/// If a [`TableProvider`] is an [`EmbeddingTable`], return the [`EmbeddingTable`].
/// This includes if the [`TableProvider`] is an [`AcceleratedTable`] with a [`EmbeddingTable`] underneath.
pub(super) async fn get_embedding_table(
    tbl: &Arc<dyn TableProvider>,
) -> Option<Arc<EmbeddingTable>> {
    if let Some(embedding_table) = tbl.as_any().downcast_ref::<EmbeddingTable>() {
        return Some(Arc::new(embedding_table.clone()));
    }

    let tbl = if let Some(adaptor) = tbl.as_any().downcast_ref::<FederatedTableProviderAdaptor>() {
        adaptor.table_provider.clone()?
    } else {
        Arc::clone(tbl)
    };

    if let Some(accelerated_table) = tbl.as_any().downcast_ref::<AcceleratedTable>() {
        let federated_table = accelerated_table
            .get_federated_table()
            .table_provider()
            .await;
        if let Some(embedding_table) = federated_table.as_any().downcast_ref::<EmbeddingTable>() {
            return Some(Arc::new(embedding_table.clone()));
        }
    }
    None
}

/// Compute the primary keys for each table in the app. Primary Keys can be explicitly defined in the Spicepod.yaml
pub async fn parse_explicit_primary_keys(
    app: Arc<RwLock<Option<Arc<App>>>>,
) -> HashMap<TableReference, Vec<String>> {
    app.read().await.as_ref().map_or(HashMap::new(), |app| {
        app.datasets
            .iter()
            .filter_map(|d| {
                d.embeddings.iter().find_map(|e| {
                    e.primary_keys.as_ref().map(|pks| {
                        (
                            TableReference::parse_str(&d.name)
                                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                                .into(),
                            pks.clone(),
                        )
                    })
                })
            })
            .collect::<HashMap<TableReference, Vec<_>>>()
    })
}
