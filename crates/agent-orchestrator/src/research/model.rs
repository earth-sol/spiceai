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

    // Count different artifact types in a single pass
    let mut document_artifacts = Vec::new();
    let mut num_snippets = 0;

    for artifact in artifacts {
        match artifact {
            Artifact::Document { path, .. } => document_artifacts.push(path.clone()),
            Artifact::TextSnippet(_) => num_snippets += 1,
        }
    }

    let total_artifacts = artifacts.len();

    // Build summary message
    let mut message = "✅ Research completed\n\n📊 Summary:".to_string();

    // Only add total artifacts if there are any
    if total_artifacts > 0 {
        message.push_str(&format!("\n• Total artifacts: {total_artifacts}"));

        // Add snippet count only if there are any snippets
        if num_snippets > 0 {
            message.push_str(&format!("\n• Text snippets: {num_snippets}"));
        }

        // Add document information only if there are documents
        if !document_artifacts.is_empty() {
            message.push_str(&format!("\n• Documents: {}", document_artifacts.len()));

            // List documents with better formatting
            for path in &document_artifacts {
                message.push_str(&format!("\n  - `{path}`"));
            }
        }
    }

    message
}
