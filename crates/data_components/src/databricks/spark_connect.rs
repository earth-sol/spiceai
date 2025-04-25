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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use std::{error::Error, sync::Arc};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::{Read, spark_connect::SparkConnect};

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
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
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
        client_secret: String,
        databricks_use_ssl: bool,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Extract the Databricks host from the endpoint
        let databricks_host = endpoint.clone();

        // get access token
        let token_response = get_m2m_access_token(
            client_id.clone(),
            client_secret.clone(),
            databricks_host.clone(),
        )
        .await?;
        let token = token_response.access_token;
        let expires_in = token_response.expires_in;

        tracing::debug!("Initial token acquired, expires in {} seconds", expires_in);

        let session_id = Uuid::new_v4();
        let connection = format!(
            "sc://{endpoint}:443/;use_ssl={databricks_use_ssl};user_id=spice.ai;session_id={session_id};token={token};x-databricks-cluster-id={cluster_id}"
        );

        let spark_connect = Arc::new(SparkConnect::from_connection(connection.as_str()).await?);

        // Create a key for this connection configuration
        let conn_key = format!("{}:{}", endpoint, cluster_id);

        // Check if we already have a refresh task for this connection config
        let mut refresh_tasks = REFRESH_TASKS.lock().unwrap();

        if !refresh_tasks.contains_key(&conn_key) {
            let sc_clone = Arc::clone(&spark_connect);
            let client_id_clone = client_id.clone();
            let client_secret_clone = client_secret.clone();
            let databricks_host_clone = databricks_host.clone();

            let handle = tokio::spawn(async move {
                // refresh after initial token expiry
                let mut current_expires_in = expires_in;
                loop {
                    // Schedule refresh for 90% of the token's lifetime to ensure we refresh before expiration
                    let refresh_in_seconds = (current_expires_in as f64 * 0.9) as u64;
                    tokio::time::sleep(Duration::from_secs(refresh_in_seconds)).await;

                    match get_m2m_access_token(
                        client_id_clone.clone(),
                        client_secret_clone.clone(),
                        databricks_host_clone.clone(),
                    )
                    .await
                    {
                        Ok(token_response) => {
                            sc_clone.set_token(&token_response.access_token).await;
                            current_expires_in = token_response.expires_in;

                            tracing::debug!(
                                "Token refreshed, expires in {} seconds",
                                current_expires_in
                            );
                        }
                        Err(e) => {
                            tracing::error!("Request error when refreshing token: {}", e);
                            // If request fails, try again in 60 seconds
                            current_expires_in = 60;
                        }
                    };
                }
            });

            // Store the task
            refresh_tasks.insert(
                conn_key.clone(),
                RefreshTask {
                    handle,
                    last_used: std::time::Instant::now(),
                },
            );
        } else {
            // Update the last_used time for existing task
            if let Some(task) = refresh_tasks.get_mut(&conn_key) {
                task.last_used = std::time::Instant::now();
            }
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
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        Ok(self
            .spark_connect
            .table_provider(table_reference, schema)
            .await?)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
    scope: String,
}

struct RefreshTask {
    handle: JoinHandle<()>,
    last_used: std::time::Instant,
}

static REFRESH_TASKS: LazyLock<Mutex<HashMap<String, RefreshTask>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

async fn get_m2m_access_token(
    client_id: String,
    client_secret: String,
    databricks_host: String,
) -> Result<TokenResponse, Box<dyn Error + Send + Sync>> {
    // Construct the token endpoint URL
    let token_endpoint_url = format!("https://{}/oidc/v1/token", databricks_host);

    // Create a reqwest client
    let client = reqwest::Client::new();

    // Make the request with basic auth, form data, and headers
    let response = client
        .post(&token_endpoint_url)
        .basic_auth(client_id, Some(client_secret))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&[("grant_type", "client_credentials"), ("scope", "all-apis")])
        .send()
        .await?;

    // Check if the request was successful
    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await?;
        return Err(format!(
            "Failed to get access token: HTTP {}, {}",
            status, error_text
        )
        .into());
    }

    // Parse the response to get the access token
    let token_response = response.json::<TokenResponse>().await?;

    // Log the expiration time to help with debugging token refresh issues
    tracing::debug!(
        "Got access token, expires in {} seconds",
        token_response.expires_in
    );

    Ok(token_response)
}
