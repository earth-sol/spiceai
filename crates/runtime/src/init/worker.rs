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

// Allow dead code until next PR
#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use crate::{get_params_with_secrets, metrics, status, timing::TimeMeasurement, Runtime};
use opentelemetry::KeyValue;
use snafu::prelude::*;
use workers::initialize_worker;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load worker: {name}.\n{source}"))]
    FailedToLoadWorker {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Runtime {
    pub(crate) async fn load_workers(self: Arc<Self>) {
        let app_lock = self.app.read().await;

        if let Some(app) = app_lock.as_ref() {
            for worker in &app.workers {
                self.status
                    .update_worker(&worker.name, status::ComponentStatus::Initializing);
                if let Err(e) = self.load_worker(worker).await {
                    tracing::warn!("Failed to load worker [{}]: {}", worker.name, e);
                    self.status
                        .update_worker(&worker.name, status::ComponentStatus::Error);
                }
            }
        }
    }

    async fn load_worker(
        &self,
        worker_config: &spicepod::component::worker::Worker,
    ) -> Result<(), Error> {
        let source_str = worker_config.from.clone();
        let _guard = TimeMeasurement::new(
            &metrics::workers::LOAD_DURATION_MS,
            &[
                KeyValue::new("worker", worker_config.name.clone()),
                KeyValue::new("source", source_str.clone()),
            ],
        );

        tracing::info!(
            "Loading worker [{}] from {}...",
            worker_config.name,
            worker_config.from
        );

        // Get required secrets
        let p = worker_config
            .params
            .iter()
            .map(|(k, v)| {
                let k = k.clone();
                match v.as_str() {
                    Some(s) => (k, s.to_string()),
                    None => (k, v.to_string()),
                }
            })
            .collect::<HashMap<_, _>>();

        let params = get_params_with_secrets(self.secrets(), &p).await;

        // Initialize the worker
        match initialize_worker(worker_config, Arc::new(params)).await {
            Ok(worker) => {
                // Add worker to registry
                let mut registry = self.workers.write().await;
                registry.add(worker);

                tracing::info!("Worker [{}] loaded, ready for use", worker_config.name);
                metrics::workers::COUNT.add(
                    1,
                    &[
                        KeyValue::new("worker", worker_config.name.clone()),
                        KeyValue::new("source", source_str),
                    ],
                );
                self.status
                    .update_worker(&worker_config.name, status::ComponentStatus::Ready);
                Ok(())
            }
            Err(e) => {
                metrics::workers::LOAD_ERROR.add(1, &[]);
                self.status
                    .update_worker(&worker_config.name, status::ComponentStatus::Error);
                Err(Error::FailedToLoadWorker {
                    name: worker_config.name.clone(),
                    source: Box::new(e),
                })
            }
        }
    }

    async fn remove_worker(&self, worker_config: &spicepod::component::worker::Worker) {
        let mut registry = self.workers.write().await;
        registry.remove(&worker_config.name);

        tracing::info!("Worker [{}] has been unloaded", worker_config.name);
        metrics::workers::COUNT.add(
            -1,
            &[
                KeyValue::new("worker", worker_config.name.clone()),
                KeyValue::new("source", worker_config.from.clone()),
            ],
        );
    }

    async fn update_worker(&self, worker_config: &spicepod::component::worker::Worker) {
        self.status
            .update_worker(&worker_config.name, status::ComponentStatus::Refreshing);
        self.remove_worker(worker_config).await;
        if let Err(e) = self.load_worker(worker_config).await {
            tracing::warn!("Failed to update worker [{}]: {}", worker_config.name, e);
            self.status
                .update_worker(&worker_config.name, status::ComponentStatus::Error);
        }
    }

    pub(crate) async fn apply_worker_diff(
        &self,
        current_app: &Arc<app::App>,
        new_app: &Arc<app::App>,
    ) {
        for worker in &new_app.workers {
            if let Some(current_worker) = current_app.workers.iter().find(|w| w.name == worker.name)
            {
                if current_worker != worker {
                    self.update_worker(worker).await;
                }
            } else {
                self.status
                    .update_worker(&worker.name, status::ComponentStatus::Initializing);
                if let Err(e) = self.load_worker(worker).await {
                    tracing::warn!("Failed to load worker [{}]: {}", worker.name, e);
                    self.status
                        .update_worker(&worker.name, status::ComponentStatus::Error);
                }
            }
        }

        // Remove workers that are no longer in the app
        for worker in &current_app.workers {
            if !new_app.workers.iter().any(|w| w.name == worker.name) {
                self.status
                    .update_worker(&worker.name, status::ComponentStatus::Disabled);
                self.remove_worker(worker).await;
            }
        }
    }
}
