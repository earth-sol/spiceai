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

use data_components::{
    databricks::auth::DatabricksM2MTokenProvider, token_provider::TokenProvider,
};
use secrecy::SecretString;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Default, Clone)]
pub struct TokenProviderRegistry {
    pub token_provider_registry: Arc<RwLock<HashMap<String, Arc<dyn TokenProvider>>>>,
}

impl TokenProviderRegistry {
    pub fn new() -> Self {
        Self {
            token_provider_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Gets or creates a DatabricksM2MTokenProvider
    ///
    /// This method will return an existing provider if one is already registered with the same client_id,
    /// otherwise it will create a new one and register it.
    pub async fn get_or_create_databricks_m2m(
        &self,
        endpoint: String,
        client_id: String,
        client_secret: SecretString,
    ) -> Result<Arc<dyn TokenProvider>, data_components::databricks::auth::Error> {
        let key = format!("databricks_m2m:{}", client_id);

        {
            let registry = self.token_provider_registry.read().await;
            if let Some(provider) = registry.get(&key) {
                tracing::debug!(
                    "Using existing Databricks M2M token provider for client_id: {}",
                    client_id
                );
                return Ok(Arc::clone(provider));
            }
        }

        let mut registry = self.token_provider_registry.write().await;

        if let Some(provider) = registry.get(&key) {
            tracing::debug!(
                "Using existing Databricks M2M token provider for client_id: {}",
                client_id
            );
            return Ok(Arc::clone(provider));
        }

        tracing::debug!(
            "Creating new Databricks M2M token provider for client_id: {}",
            client_id
        );
        let provider =
            DatabricksM2MTokenProvider::try_new(endpoint, client_id.clone(), client_secret).await?;

        registry.insert(key, provider.clone());

        Ok(provider)
    }
}
