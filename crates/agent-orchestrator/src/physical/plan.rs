use async_openai::types::CreateChatCompletionResponse;
use serde::{Deserialize, Serialize};

/// Represents a physical execution plan containing ordered groups of steps
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalPlan {
    pub groups: Vec<Group>,
}

impl PhysicalPlan {
    pub fn new(body: &str) -> Result<Self, serde_json::Error> {
        let plan: PhysicalPlan = serde_json::from_str(body)?;

        Ok(plan)
    }

    pub fn from_chat_completion(
        completion: &CreateChatCompletionResponse,
    ) -> Result<Self, anyhow::Error> {
        let body = completion
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .ok_or_else(|| anyhow::anyhow!("No content in the response"))?;

        Ok(Self::new(body)?)
    }
}

/// A group of related steps working towards a specific objective
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    /// The numerical order of this group, starting from 1
    pub position: i64,
    /// The objective this group of steps should achieve
    pub objective: String,
    /// The ordered list of steps to execute
    pub steps: Vec<Step>,
}

/// A single executable step in the physical plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    /// The numerical order of this step, starting from 1
    pub position: i64,
    /// Description of what this step does
    pub description: String,
    /// The type of tool to use for this step
    #[serde(rename = "type")]
    pub tool: ToolType,
    /// The specific action to perform
    /// For execute_terminal, this is the command to run
    /// For change_directory, this is the relative path
    pub action: String,
}

/// The available types of tools that can be used in a step
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolType {
    ChangeDirectory,
    CreateDirectory,
    ReadObject,
    WriteObject,
    ExecuteTerminal,
    Other,
    Response,
    RequestForInfo,
    RetrieveMetadata,
    Validation,
    Improvement,
}
