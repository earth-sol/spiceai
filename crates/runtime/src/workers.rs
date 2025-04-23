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
#![allow(clippy::implicit_hasher)]

use async_trait::async_trait;
use secrecy::SecretString;
use snafu::prelude::*;
use spicepod::component::worker::Worker as WorkerConfig;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;
use workers::SpiceWorker;

#[derive(Debug, Snafu)]
pub enum WorkerError {
    #[snafu(display("Unsupported worker type: {worker_type}"))]
    UnsupportedWorkerType { worker_type: String },

    #[snafu(display("Failed to create worker: {source}"))]
    WorkerCreationFailed { source: workers::Error },
}

pub type Result<T, E = WorkerError> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

type NewWorkerResult = AnyErrorResult<Box<dyn SpiceWorker>>;

static WORKER_FACTORY_REGISTRY: LazyLock<Mutex<HashMap<String, Arc<dyn WorkerFactory>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn register_worker_factory(name: &str, factory: Arc<dyn WorkerFactory>) {
    let mut registry = WORKER_FACTORY_REGISTRY.lock().await;
    registry.insert(name.to_string(), factory);
}

/// Create a new worker by type.
///
/// Returns `None` if the worker factory for the given type is not registered.
pub async fn create_worker(
    worker_config: &WorkerConfig,
    params: Arc<HashMap<String, SecretString>>,
) -> Option<AnyErrorResult<Box<dyn SpiceWorker>>> {
    let guard = WORKER_FACTORY_REGISTRY.lock().await;

    // Extract worker type from "from" field (e.g., "model:openai" -> "model")
    let parts: Vec<&str> = worker_config.from.splitn(2, ':').collect();
    let worker_type = parts.first().map_or("", |s| *s);

    let factory = guard.get(worker_type)?;
    let result = factory.create(worker_config, params).await;
    Some(result)
}

pub async fn register_all() {
    register_worker_factory("model", Arc::new(ModelWorkerFactory)).await;
    register_worker_factory("models", Arc::new(ModelWorkerFactory)).await;
}

pub async fn unregister_all() {
    let mut registry = WORKER_FACTORY_REGISTRY.lock().await;
    registry.clear();
}

/// Factory for creating workers
#[async_trait]
pub trait WorkerFactory: Send + Sync {
    fn create(
        &self,
        config: &WorkerConfig,
        params: Arc<HashMap<String, SecretString>>,
    ) -> Pin<Box<dyn Future<Output = NewWorkerResult> + Send>>;
}

/// Factory for creating model workers
#[derive(Default)]
pub struct ModelWorkerFactory;

#[async_trait]
impl WorkerFactory for ModelWorkerFactory {
    fn create(
        &self,
        config: &WorkerConfig,
        params: Arc<HashMap<String, SecretString>>,
    ) -> Pin<Box<dyn Future<Output = NewWorkerResult> + Send>> {
        let config = config.clone();

        Box::pin(async move {
            // Use the workers crate's initialize_worker function
            match workers::initialize_worker(&config, params).await {
                Ok(worker) => Ok(worker),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
    }
}

/// Load a worker based on configuration
pub async fn load_worker(
    worker_config: &WorkerConfig,
    params: Arc<HashMap<String, SecretString>>,
) -> Result<Box<dyn SpiceWorker>> {
    if let Some(result) = create_worker(worker_config, params).await { result.map_err(|e| WorkerError::WorkerCreationFailed {
        source: workers::Error::InvalidWorkerConfig {
            message: e.to_string(),
        },
    }) } else {
        // Extract worker type from "from" field
        let parts: Vec<&str> = worker_config.from.splitn(2, ':').collect();
        let worker_type = parts.first().map_or("", |s| *s);

        Err(WorkerError::UnsupportedWorkerType {
            worker_type: worker_type.to_string(),
        })
    }
}
