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

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use async_trait::async_trait;
use secrecy::SecretString;
use serde_json::Value;
use snafu::Snafu;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

pub mod model;
pub use model::ModelWorker;

/// Error types for worker operations
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported worker type: {worker_type}"))]
    UnsupportedWorkerType { worker_type: String },

    #[snafu(display("Invalid worker configuration: {message}"))]
    InvalidWorkerConfig { message: String },
}

/// Result type for worker operations
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Workers that implement the [`SpiceWorker`] trait.
#[async_trait]
pub trait SpiceWorker: Sync + Send {
    /// Get the source of the worker
    fn from(&self) -> Cow<'_, str>;

    /// Get the name of the worker
    fn name(&self) -> Cow<'_, str>;

    /// Get the role of the worker
    fn role(&self) -> Cow<'_, str>;

    /// Get the description of the worker, if any
    fn description(&self) -> Option<Cow<'_, str>>;

    /// Get the parameters of the worker
    fn params(&self) -> &HashMap<String, Value>;

    /// Get the dependencies of the worker
    fn depends_on(&self) -> &Vec<String>;

    /// Load the worker's resources
    async fn load(&self, _params: Arc<HashMap<String, SecretString>>) -> Result<()> {
        Ok(())
    }
}

/// Registry for managing workers
pub struct WorkerRegistry {
    workers: HashMap<String, Box<dyn SpiceWorker>>,
}

impl WorkerRegistry {
    /// Creates a new empty worker registry
    #[must_use]
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
        }
    }

    /// Add a worker to the registry
    pub fn add(&mut self, worker: Box<dyn SpiceWorker>) {
        let name = worker.name().to_string();
        self.workers.insert(name, worker);
    }

    /// Get a worker by name
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&dyn SpiceWorker> {
        self.workers.get(name).map(AsRef::as_ref)
    }

    /// Get all workers in the registry
    #[must_use]
    pub fn all(&self) -> Vec<(&String, &dyn SpiceWorker)> {
        self.workers.iter().map(|(k, v)| (k, v.as_ref())).collect()
    }

    /// Remove a worker by name
    pub fn remove(&mut self, name: &str) -> Option<Box<dyn SpiceWorker>> {
        self.workers.remove(name)
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory function to create a worker based on the "from" string
pub fn load_worker(
    worker_config: &spicepod::component::worker::Worker,
) -> Result<Box<dyn SpiceWorker>> {
    // Extract the worker type from the "from" string
    let parts: Vec<&str> = worker_config.from.splitn(2, ':').collect();
    let worker_type = parts.first().map_or("", |s| *s);

    match worker_type {
        "model" | "models" => {
            // For model workers, extract the actual model source
            let model_source = if parts.len() > 1 {
                parts[1]
            } else {
                return Err(Error::InvalidWorkerConfig {
                    message: "Model source not specified in 'from' field. Format should be 'models:<model-source>'".to_string() 
                });
            };

            // Create a new ModelWorker with the extracted model source and all parameters
            let model_worker = ModelWorker::new(
                model_source,
                worker_config.name.clone(),
                worker_config.role.clone(),
            )
            .with_description(worker_config.description.clone().unwrap_or_default())
            .with_params(worker_config.params.clone())
            .with_depends_on(worker_config.depends_on.clone());

            Ok(Box::new(model_worker))
        }
        // Add other worker types here as needed
        "" => Err(Error::InvalidWorkerConfig {
            message: "Worker type not specified in 'from' field".to_string(),
        }),
        _ => Err(Error::UnsupportedWorkerType {
            worker_type: worker_type.to_string(),
        }),
    }
}

/// Loads a worker from a spicepod worker configuration
#[allow(clippy::implicit_hasher)]
pub async fn initialize_worker(
    worker_config: &spicepod::component::worker::Worker,
    params: Arc<HashMap<String, SecretString>>,
) -> Result<Box<dyn SpiceWorker>> {
    let worker = load_worker(worker_config)?;

    // Initialize the worker
    worker.load(params).await?;

    Ok(worker)
}
