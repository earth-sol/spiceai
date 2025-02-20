use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Artifact {
    Document { name: String, content: String },
    TextSnippet(String),
}

impl Display for Artifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Artifact::Document { name, content } => write!(f, "Document: {name}\n{content}"),
            Artifact::TextSnippet(text) => write!(f, "{text}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Research {
    pub prompt: String,
    pub artifacts: Vec<Artifact>,
}
