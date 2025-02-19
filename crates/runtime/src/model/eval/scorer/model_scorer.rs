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

use crate::model::eval::scorer::mean;
use crate::model::LLMModelStore;
use async_openai::types::{ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{DatasetInput, DatasetOutput, Scorer};
use serde_json::{json, Value};

pub struct LLMScorer {
    pub name: String,
    pub llm_store: Arc<RwLock<LLMModelStore>>,
}

#[async_trait]
impl Scorer for LLMScorer {
    async fn score(
        &self,
        input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32 {
        let store = self.llm_store.read().await;
        let scorer = store.get(&self.name).unwrap();
        let prompt = "YO".to_string();
        let req = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestUserMessageArgs::default()
                .content(prompt)
                .build()
                .unwrap()
                .into()])
            .store(true)
            .metadata(json!({
                "actual": format!("{:?}", actual),
                "ideal": format!("{:?}", ideal),
                "input": format!("{:?}", input),
            }))
            .build()
            .unwrap();
        match scorer.chat_request(req).await {
            Ok(response) => {
                let resp = response
                    .choices
                    .into_iter()
                    .next()
                    .and_then(|c| c.message.content);
                tracing::info!("LLM response: {:?}", resp);
                let json_resp: Value = serde_json::from_str(&resp.unwrap()).unwrap();
                let score = json_resp["score"].as_f64().unwrap();
                score as f32
            }
            Err(e) => {
                tracing::error!("Error running LLM model: {:?}", e);
                0.0
            }
        }
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("mean".to_string(), mean(scores))]
    }
}
