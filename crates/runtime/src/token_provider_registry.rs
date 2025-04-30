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

use data_components::token_provider::TokenProvider;
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
}
