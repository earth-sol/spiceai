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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{datasource::TableProvider, sql::TableReference};
use secrecy::SecretString;
use snafu::prelude::*;
use std::sync::Arc;
use uuid::Uuid;

use crate::databricks::auth::DatabricksM2MTokenProvider;
use crate::token_provider::TokenProvider;
use crate::{Read, spark_connect::SparkConnect};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to obtain Databricks service principal token for machine-to-machine authentication.\nVerify your client_id and client_secret are correct.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"
    ))]
    UnableToGetM2MToken {},
}

#[derive(Clone)]
pub struct DatabricksSparkConnect {
    spark_connect: Arc<SparkConnect>,
}

impl DatabricksSparkConnect {
    pub async fn new(
        endpoint: String,
        cluster_id: String,
        token: String,
        databricks_use_ssl: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let session_id = Uuid::new_v4();
        let connection = format!(
            "sc://{endpoint}:443/;use_ssl={databricks_use_ssl};user_id=spice.ai;session_id={session_id};token={token};x-databricks-cluster-id={cluster_id}"
        );
        Ok(Self {
            spark_connect: Arc::new(SparkConnect::from_connection(connection.as_str()).await?),
        })
    }

    pub async fn new_m2m(
        endpoint: String,
        cluster_id: String,
        client_id: String,
        client_secret: &SecretString,
        databricks_use_ssl: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let token_provider = DatabricksM2MTokenProvider::get_shared(
            endpoint.clone(),
            client_id,
            client_secret.clone(),
        )
        .await
        .map_err(|_| Error::UnableToGetM2MToken {})?;

        let token = token_provider.get_token().await?;

        let session_id = Uuid::new_v4();
        let connection = format!(
            "sc://{endpoint}:443/;use_ssl={databricks_use_ssl};user_id=spice.ai;session_id={session_id};token={token};x-databricks-cluster-id={cluster_id}"
        );

        let spark_connect = Arc::new(SparkConnect::from_connection(connection.as_str()).await?);

        if let Some(mut rx) = token_provider.subscribe() {
            let sc = Arc::clone(&spark_connect);
            tokio::spawn(async move {
                while rx.changed().await.is_ok() {
                    let new_token = rx.borrow().clone();
                    sc.set_token(&new_token).await;
                }
            });
        }

        Ok(Self { spark_connect })
    }
}

#[async_trait]
impl Read for DatabricksSparkConnect {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .spark_connect
            .table_provider(table_reference, schema)
            .await?)
    }
}
