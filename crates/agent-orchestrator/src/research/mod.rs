use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

pub mod model;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Artifact {
    Document {
        path: String,
        content: String,
        url: Option<String>,
    },
    TextSnippet(String),
}

impl Display for Artifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Artifact::Document { path, content, url } => match url {
                Some(url) => write!(f, "Document: {path}\n{content}\nURL: {url}"),
                None => write!(f, "Document: {path}\n{content}"),
            },
            Artifact::TextSnippet(text) => write!(f, "{text}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Research {
    pub prompt: String,
    pub artifacts: Vec<Artifact>,
}
