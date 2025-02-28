use async_openai::{error::OpenAIError, types::CreateChatCompletionResponse};
use serde::{Deserialize, Serialize};

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
    let total_artifacts = artifacts.len();
    format!(
        "Research completed.\n- Total artifacts: {total_artifacts}\n- Including {num_snippets} text snippets.\n- Including the following documents: {}",
        artifact_paths.join(", ")
    )
}
