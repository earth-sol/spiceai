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

use async_trait::async_trait;
use once_cell::sync::Lazy;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::collections::HashMap;
use std::time::Duration;
use std::{fmt, sync::Arc};
use tokio::time::Instant;
use tokio::{
    sync::{RwLock, watch},
    task::JoinHandle,
    time::sleep,
};

use crate::token_provider::{Result, TokenProvider};

type Key = (String, String);

static REGISTRY: Lazy<RwLock<HashMap<Key, (Arc<DatabricksM2MTokenProvider>, Instant)>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get token: {source}"))]
    UnableToGetToken { source: Box<dyn std::error::Error> },
}

#[derive(Clone)]
pub struct DatabricksM2MTokenProvider {
    endpoint: String,
    client_id: String,

    tx: watch::Sender<String>,
    rx: watch::Receiver<String>,

    _handle: Arc<JoinHandle<()>>,
}

impl fmt::Debug for DatabricksM2MTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("M2mTokenProvider")
            .field("client_id", &self.client_id)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl DatabricksM2MTokenProvider {
    async fn new(
        endpoint: String,
        client_id: String,
        client_secret: SecretString,
    ) -> Result<Self, Error> {
        // initial fetch
        let TokenResponse {
            access_token,
            expires_in,
            ..
        } = get_m2m_access_token(endpoint.clone(), client_id.clone(), client_secret.clone())
            .await
            .map_err(|e| Error::UnableToGetToken { source: e })?;

        // create watch channel
        let (tx, rx) = watch::channel(access_token.clone());

        // spawn background refresh loop
        let loop_id = client_id.clone();
        let loop_endpoint = endpoint.clone();
        let tx_cloned = tx.clone();

        let secret = client_secret.clone();

        let handle = tokio::spawn(async move {
            // schedule the first refresh at 90% of `expires_in`
            let mut next_wait = Duration::from_secs_f64(expires_in as f64 * 0.9);

            loop {
                sleep(next_wait).await;

                match get_m2m_access_token(loop_endpoint.clone(), loop_id.clone(), secret.clone())
                    .await
                {
                    Ok(TokenResponse {
                        access_token,
                        expires_in,
                        ..
                    }) => {
                        tracing::debug!("M2M token refreshed; expires in {}", expires_in);
                        let _ = tx_cloned.send(access_token.clone());
                        next_wait = Duration::from_secs_f64(expires_in as f64);
                    }
                    Err(e) => {
                        tracing::error!("M2M token refresh failed: {}", e);
                        // back‑off 60s on error
                        next_wait = Duration::from_secs(60);
                    }
                }
            }
        });

        Ok(Self {
            endpoint,
            client_id,
            tx,
            rx,
            _handle: Arc::new(handle),
        })
    }

    pub async fn get_shared(
        endpoint: String,
        client_id: String,
        client_secret: SecretString,
    ) -> Result<Arc<Self>, Error> {
        let key = (endpoint.clone(), client_id.clone());

        // Fast path: try an *async read* lock
        {
            let read_guard = REGISTRY.read().await;
            if let Some((existing, _last_used)) = read_guard.get(&key) {
                return Ok(Arc::clone(existing));
            }
        }

        // Not in map yet: acquire *write* lock to initialize
        let mut write_guard = REGISTRY.write().await;

        // 2a) re‑check in case someone else filled it while we were waiting for the write lock
        if let Some((existing, last_used)) = write_guard.get_mut(&key) {
            *last_used = Instant::now();
            return Ok(Arc::clone(existing));
        }

        // We are the first: actually build the provider (await the HTTP fetch)
        let provider = Arc::new(
            DatabricksM2MTokenProvider::new(
                endpoint.clone(),
                client_id.clone(),
                client_secret.clone(),
            )
            .await?,
        );

        write_guard.insert(key, (Arc::clone(&provider), Instant::now()));
        Ok(provider)
    }
}

#[async_trait]
impl TokenProvider for DatabricksM2MTokenProvider {
    async fn get_token(&self) -> Result<String> {
        Ok(self.rx.borrow().clone())
    }

    fn subscribe(&self) -> Option<watch::Receiver<String>> {
        Some(self.tx.subscribe())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
    scope: String,
}

async fn get_m2m_access_token(
    databricks_endpoint: String,
    client_id: String,
    client_secret: SecretString,
) -> Result<TokenResponse, Box<dyn std::error::Error + Send + Sync>> {
    let token_endpoint_url = format!("https://{databricks_endpoint}/oidc/v1/token");

    let client = reqwest::Client::new();

    let response = client
        .post(&token_endpoint_url)
        .basic_auth(client_id, Some(client_secret.expose_secret()))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&[("grant_type", "client_credentials"), ("scope", "all-apis")])
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await?;
        return Err(format!("Failed to get access token: HTTP {status}, {error_text}",).into());
    }

    let token_response = response.json::<TokenResponse>().await?;

    tracing::debug!(
        "Got access token, expires in {} seconds",
        token_response.expires_in
    );

    Ok(token_response)
}
