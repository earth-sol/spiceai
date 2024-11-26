/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use async_openai::error::OpenAIError;
use hf_hub::{api::tokio::ApiBuilder, Repo, RepoType};
use llms::{
    anthropic::{Anthropic, AnthropicConfig},
    chat::{create_local_model, Chat},
    openai::Openai,
};
use tokio::fs;

pub(crate) fn create_openai(model_id: &str) -> Arc<dyn Chat> {
    let api_key = std::env::var("SPICE_OPENAI_API_KEY").ok();
    Arc::new(Openai::new(model_id.to_string(), None, api_key, None, None))
}

pub(crate) fn create_anthropic(model_id: Option<&str>) -> Result<Arc<dyn Chat>, OpenAIError> {
    let cfg = AnthropicConfig::default()
        .with_api_key(std::env::var("SPICE_ANTHROPIC_API_KEY").ok())
        .with_auth_token(std::env::var("SPICE_ANTHROPIC_AUTH_TOKEN").ok());
    let model = Anthropic::new(cfg, model_id)?;

    Ok(Arc::new(model))
}

pub(crate) async fn create_local() -> Result<Arc<dyn Chat>, anyhow::Error> {
    let temp_dir = local_model_dir("Phi-3-mini-4k-instruct").await;

    download_hf_model_artifacts(
        "microsoft/Phi-3-mini-4k-instruct",
        None,
        std::env::var("SPICE_HF_API_KEY").ok(),
        vec![
            "model-00001-of-00002.safetensors",
            "model-00002-of-00002.safetensors",
            "config.json",
            "tokenizer.json",
            "tokenizer_config.json",
        ],
        &temp_dir,
    )
    .await
    .expect("Failed to download test model artifacts");

    let model_weights = [
        temp_dir
            .join("model-00001-of-00002.safetensors")
            .to_str()
            .unwrap_or_default()
            .to_string(),
        temp_dir
            .join("model-00002-of-00002.safetensors")
            .to_str()
            .unwrap_or_default()
            .to_string(),
    ];

    let model = create_local_model(
        &model_weights,
        temp_dir.join("config.json").to_str(),
        temp_dir.join("tokenizer.json").to_str(),
        temp_dir.join("tokenizer_config.json").to_str(),
        None,
    )
    .map_err(anyhow::Error::from)?;
    Ok(Arc::from(model))
}

/// Creates a directory for the specified model under `.spice/test_models`.
#[must_use]
pub async fn local_model_dir(model_name: &str) -> PathBuf {
    let working_dir = std::env::current_dir().unwrap_or_else(|_| ".".into());
    let model_dir = working_dir.join(".spice/test_models").join(model_name);

    // Remove the directory if it already exists
    if model_dir.exists() {
        fs::remove_dir_all(&model_dir)
            .await
            .expect("Failed to remove existing model directory");
    }
    fs::create_dir_all(&model_dir)
        .await
        .expect("Failed to create model directory");

    model_dir
}

/// For a given `HuggingFace` repo, downloads the specified file and save them into provided folder
async fn download_hf_model_artifacts(
    model_id: &str,
    revision: Option<&str>,
    hf_token: Option<String>,
    files: Vec<&str>,
    target_dir: &PathBuf,
) -> Result<(), anyhow::Error> {
    let api = ApiBuilder::new()
        .with_progress(false)
        .with_token(hf_token)
        .build()
        .context("Failed to instantiate API for downloading model artifacts")?;

    let repo = if let Some(revision) = revision {
        Repo::with_revision(model_id.to_string(), RepoType::Model, revision.to_string())
    } else {
        Repo::new(model_id.to_string(), RepoType::Model)
    };
    let api_repo = api.repo(repo.clone());

    for file_name in files {
        let file_path = target_dir.join(file_name);
        tracing::trace!("Downloading '{}' from {}", file_name, repo.url());

        let source_path = api_repo
            .get(file_name)
            .await
            .with_context(|| format!("Unable to download '{}' from {}", file_name, repo.url()))?;

        std::fs::copy(&source_path, &file_path).with_context(|| {
            format!("Failed to copy '{}' to {}", file_name, file_path.display())
        })?;
    }
    Ok(())
}
