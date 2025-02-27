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
use secrecy::SecretString;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use crate::{Result, SpiceWorker};

/// A worker implementation for machine learning models.
pub struct ModelWorker {
    /// The source or provider of the model
    from: String,
    /// The name of the model worker
    name: String,
    /// The role of the model worker
    role: String,
    /// Optional description of the model worker
    description: Option<String>,
    /// Parameters for the model worker
    params: HashMap<String, Value>,
    /// Dependencies of the model worker
    depends_on: Vec<String>,
}

impl ModelWorker {
    /// Creates a new ModelWorker instance
    pub fn new(from: impl Into<String>, name: impl Into<String>, role: impl Into<String>) -> Self {
        ModelWorker {
            from: from.into(),
            name: name.into(),
            role: role.into(),
            description: None,
            params: HashMap::default(),
            depends_on: Vec::default(),
        }
    }

    /// Sets the description for the model worker
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Adds parameters to the model worker
    #[must_use]
    pub fn with_params(mut self, params: HashMap<String, Value>) -> Self {
        self.params = params;
        self
    }

    /// Sets dependencies for the model worker
    #[must_use]
    pub fn with_depends_on(mut self, depends_on: Vec<String>) -> Self {
        self.depends_on = depends_on;
        self
    }
}

#[async_trait]
impl SpiceWorker for ModelWorker {
    fn from(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.from)
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn role(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.role)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.description.as_ref().map(|s| Cow::Borrowed(s.as_str()))
    }

    fn params(&self) -> &HashMap<String, Value> {
        &self.params
    }

    fn depends_on(&self) -> &Vec<String> {
        &self.depends_on
    }

    async fn load(&self, _params: Arc<HashMap<String, SecretString>>) -> Result<()> {
        Ok(())
    }
}

impl From<&spicepod::component::worker::Worker> for ModelWorker {
    fn from(worker: &spicepod::component::worker::Worker) -> Self {
        // Extract model source if it's in the format "models:<source>"
        let from = if worker.from.starts_with("model:") || worker.from.starts_with("models:") {
            let parts: Vec<&str> = worker.from.splitn(2, ':').collect();
            if parts.len() > 1 {
                parts[1].to_string()
            } else {
                worker.from.clone()
            }
        } else {
            worker.from.clone()
        };

        ModelWorker {
            from,
            name: worker.name.clone(),
            role: worker.role.clone(),
            description: worker.description.clone(),
            params: worker.params.clone(),
            depends_on: worker.depends_on.clone(),
        }
    }
}
