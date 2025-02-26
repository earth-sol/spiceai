use async_openai::{error::OpenAIError, types::CreateChatCompletionResponse};
use serde::{Deserialize, Serialize};

use super::Artifact;

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
