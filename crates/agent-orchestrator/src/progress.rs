use std::fmt::Display;

use async_openai::{error::OpenAIError, types::CreateChatCompletionStreamResponse};

use crate::{create_working_stream_payload, pipeline::AgenticStage};

pub enum StageName {
    Research,
    LogicalPlan,
    PhysicalPlan,
    Execution,
    Reporting,
}

impl From<&AgenticStage> for StageName {
    fn from(stage: &AgenticStage) -> Self {
        match stage {
            AgenticStage::Research { .. } => Self::Research,
            AgenticStage::LogicalPlan(_) => Self::LogicalPlan,
            AgenticStage::PhysicalPlan(_) => Self::PhysicalPlan,
            AgenticStage::Execution(_) => Self::Execution,
            AgenticStage::Reporting(_) => Self::Reporting,
        }
    }
}

impl Display for StageName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Research => write!(f, "research"),
            Self::PhysicalPlan | Self::LogicalPlan => write!(f, "planning"),
            Self::Execution => write!(f, "execution"),
            Self::Reporting => write!(f, "reporting"),
        }
    }
}
pub struct Progress {
    idx: Index,
    sender: tokio::sync::mpsc::Sender<Result<CreateChatCompletionStreamResponse, OpenAIError>>,
}

impl Progress {
    pub fn new(
        idx: Index,
        sender: tokio::sync::mpsc::Sender<Result<CreateChatCompletionStreamResponse, OpenAIError>>,
    ) -> Self {
        Self { idx, sender }
    }

    /// Send a new `Working` start message for the current
    pub async fn start_working_stage(&self) -> bool {
        let content = self.idx.starting_message();
        let req = self.idx.with_starting(content.as_str());
        self.sender.send(req).await.is_ok()
    }

    pub async fn with_working_ending(&self, content: &str) -> bool {
        let req = Index::with_ending(content);
        self.sender.send(req).await.is_ok()
    }

    pub async fn send_message(&self, content: &str) -> bool {
        let req = create_working_stream_payload(content.to_string());
        self.sender.send(req).await.is_ok()
    }

    pub fn new_stage(&mut self, stage: StageName) {
        self.idx.stage = stage;
        self.idx.task = 0;
        self.idx.step = 0;
    }
}

/// Index points to a specific part with an [`AgenticStage`]. Some stages don't have subtasks or substeps.
pub struct Index {
    pub stage: StageName,
    pub task: usize,
    pub step: usize,
}

impl Index {
    pub(crate) fn title(&self) -> String {
        match self.stage {
            StageName::Research => "Research".to_string(),
            StageName::LogicalPlan | StageName::PhysicalPlan => "Planning".to_string(),
            StageName::Execution => "Execution".to_string(),
            StageName::Reporting => "Report Generation".to_string(),
        }
    }

    pub(crate) fn starting_message(&self) -> String {
        match self.stage {
            StageName::Research => "Starting research".to_string(),
            StageName::LogicalPlan => "Creating logical plan".to_string(),
            StageName::PhysicalPlan => "Creating physical plan".to_string(),
            StageName::Execution => "Executing physical plan".to_string(),
            StageName::Reporting => "Generating report".to_string(),
        }
    }

    pub(crate) fn with_starting(
        &self,
        content: &str,
    ) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
        create_working_stream_payload(format!(
            "<working stage=\"{stage}\" title=\"{title}\" task={task}, step={step}>{content}",
            stage = self.stage,
            title = self.title(),
            task = self.task,
            step = self.step
        ))
    }

    pub(crate) fn with_ending(
        content: &str,
    ) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
        create_working_stream_payload(format!("{content}</working>"))
    }
}
