use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

pub mod model;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Artifact {
    Document { path: String, content: String },
    TextSnippet(String),
}

impl Artifact {
    #[must_use]
    pub fn to_progress_message(&self, id: usize) -> String {
        let mut message = format!(r#"<artifact id="{id}" "#);
        match self {
            Artifact::Document { path, content } => {
                message.push_str(&format!(
                    r#"type="document" path="{path}" length="{}" truncated="{}" />"#,
                    content.len(),
                    truncate_escape_content(content)
                ));
            }
            Artifact::TextSnippet(text) => {
                message.push_str(&format!(
                    r#"type="text" length="{}" truncated="{}" />"#,
                    text.len(),
                    truncate_escape_content(text)
                ));
            }
        }
        message
    }
}

fn truncate_escape_content(content: &str) -> String {
    let mut truncated = content.to_string();
    if truncated.len() > 100 {
        truncated = format!("{}...(truncated)...", &truncated[..100]);
    }
    // Escape `"`
    truncated.replace('"', "\\\"")
}

impl Display for Artifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Artifact::Document { path, content } => write!(f, "Document: {path}\n{content}"),
            Artifact::TextSnippet(text) => write!(f, "{text}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Research {
    pub prompt: String,
    pub artifacts: Vec<Artifact>,
}
