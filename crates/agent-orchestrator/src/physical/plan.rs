use async_openai::types::CreateChatCompletionResponse;
use serde::{Deserialize, Serialize};

use crate::logical::plan::LogicalPlan;

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

#[derive(Debug, Serialize, Deserialize)]
pub struct PhysicalPlan {
    pub groups: Vec<ActionGroup>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionGroup {
    pub position: u64,
    pub objective: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Step {
    Tool {
        position: u64,
        description: String,
        tool: String,
        body: String,
    },
    Prompt {
        position: u64,
        description: String,
        prompt: String,
        target_model: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolType {
    // Web tools
    Puppeteer,
    Fetch,
    Git,

    // File system tools
    CreateDirectory,
    DirectoryTree,
    EditFile,
    GetFileInfo,
    ListAllowedDirectories,
    ListDirectory,
    MoveFile,
    ReadFile,
    ReadMultipleFiles,
    SearchFiles,
    WriteFile,

    // Terminal tools
    #[serde(rename = "iterm-mcp::write_to_terminal")]
    ItermWriteToTerminal,
    #[serde(rename = "iterm-mcp::read_terminal_output")]
    ItermReadTerminalOutput,
    #[serde(rename = "iterm-mcp::send_control_character")]
    ItermSendControlCharacter,
    RunShellCommand,

    // Legacy/existing tools
    ChangeDirectory,
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

impl PhysicalPlan {
    pub fn plan(_logical_plan: LogicalPlan) -> Result<Self, async_openai::error::OpenAIError> {
        todo!();
    }
}
