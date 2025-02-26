use async_openai::{error::OpenAIError, types::CreateChatCompletionResponse};
use serde::{Deserialize, Serialize};
use spicepod::component::model::Model;

use super::{Artifact, Research};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactResponse {
    steps: Vec<Artifact>,
}

pub(crate) fn parse_response(
    response: &CreateChatCompletionResponse,
) -> Result<Vec<Artifact>, OpenAIError> {
    let raw = response
        .choices
        .first()
        .and_then(|c| c.message.content.clone())
        .unwrap_or_default();

    let artifacts: ArtifactResponse = serde_json::from_str(raw.as_str())
        .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

    Ok(artifacts.steps)
}

#[must_use]
pub fn researcher_model(underlying_model: Model) -> Model {
    tracing::info!("Initializing researcher model [{}]", underlying_model.name);

    let mut model = Model::new(underlying_model.from, "agentic_researcher");

    for param in underlying_model.params {
        model.params.insert(param.0, param.1);
    }

    let yaml_str = include_str!("response_format.yaml");
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

    model
        .params
        .insert("openai_response_format".to_string(), yaml_value);
    model
}

pub(crate) fn research_complete_msg(r: &Research) -> String {
    let Research { artifacts, .. } = r;
    let artifact_paths = artifacts
        .iter()
        .filter_map(|a| match a {
            Artifact::Document { path, .. } => Some(path.clone()),
            Artifact::TextSnippet(_) => None,
        })
        .collect::<Vec<_>>();
    let num_snippets = artifacts
        .iter()
        .filter(|a| matches!(a, Artifact::TextSnippet(_)))
        .count();
    format!(
        "Finished Research.\nFound {num_snippets} text snippets.\nFound the following documents: {}",
        artifact_paths.join(", ")
    )
}
