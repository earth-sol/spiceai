use std::fmt::Display;

use async_openai::{error::OpenAIError, types::CreateChatCompletionStreamResponse};

use crate::{create_working_stream_payload, pipeline::AgenticStage};

#[derive(Clone, Debug)]
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

#[derive(Clone)]
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
        let content = format!("{}\n", self.idx.starting_message());
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

    pub async fn send_complete_message(&self, content: &str) -> bool {
        let req = self.idx.closed_message(content);
        self.sender.send(req).await.is_ok()
    }

    pub async fn send_open_message(&self, content: &str) -> bool {
        let req = self.idx.with_starting(content);
        self.sender.send(req).await.is_ok()
    }

    pub async fn send_close_message(&self, content: Option<&str>) -> bool {
        let req = Index::with_ending(content.unwrap_or_default());
        self.sender.send(req).await.is_ok()
    }

    pub fn step(&self) -> Option<usize> {
        self.idx.step
    }

    pub fn step_str(&self) -> String {
        self.idx.step.map(format_pos).unwrap_or_default()
    }

    pub fn task_str(&self) -> String {
        self.idx.task.map(format_pos).unwrap_or_default()
    }

    pub fn task(&self) -> Option<usize> {
        self.idx.task
    }

    pub fn new_stage(&mut self, stage: StageName) {
        self.idx.stage = stage;
        self.idx.task = None;
        self.idx.step = None;
    }

    pub fn with_new_task(&self, task: usize) -> Self {
        Self {
            idx: Index {
                stage: self.idx.stage.clone(),
                task: Some(task),
                step: None,
            },
            sender: self.sender.clone(),
        }
    }
    pub fn with_new_step(&self, step: usize) -> Self {
        Self {
            idx: Index {
                stage: self.idx.stage.clone(),
                task: self.idx.task,
                step: Some(step),
            },
            sender: self.sender.clone(),
        }
    }
}

/// Index points to a specific part with an [`AgenticStage`]. Some stages don't have subtasks or substeps.
#[derive(Clone, Debug)]
pub struct Index {
    pub stage: StageName,
    pub task: Option<usize>,
    pub step: Option<usize>,
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
            StageName::Research => "Starting research. ".to_string(),
            StageName::LogicalPlan => "Creating logical plan. ".to_string(),
            StageName::PhysicalPlan => "Creating physical plan. ".to_string(),
            StageName::Execution => "Executing physical plan. ".to_string(),
            StageName::Reporting => "Generating report. ".to_string(),
        }
    }

    pub(crate) fn with_starting(
        &self,
        content: &str,
    ) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
        create_working_stream_payload(format!(
            "<working stage=\"{stage}\" title=\"{title}\" {task}{step}>\n{content}",
            stage = self.stage,
            title = self.title(),
            task = self.task.map(|t| format!("task={t} ")).unwrap_or_default(),
            step = self.step.map(|s| format!("step={s} ")).unwrap_or_default(),
        ))
    }

    /// Creates a new message for the current task and step, but also closes it.
    ///
    /// Adding a closing bracket is to, currently, ensure multiple conflicting open tags are not sent in parallel.
    pub(crate) fn closed_message(
        &self,
        content: &str,
    ) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
        create_working_stream_payload(format!(
            "<working stage=\"{stage}\" title=\"{title}\" {task}{step}>\n{content}</working>\n",
            stage = self.stage,
            title = self.title(),
            task = self.task.map(|t| format!("task={t} ")).unwrap_or_default(),
            step = self.step.map(|s| format!("step={s} ")).unwrap_or_default(),
        ))
    }

    pub(crate) fn with_ending(
        content: &str,
    ) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
        create_working_stream_payload(format!("{content}</working>\n"))
    }
}

fn format_pos(i: usize) -> String {
    match i {
        1 => "1st".to_string(),
        2 => "2nd".to_string(),
        3 => "3rd".to_string(),
        _ => format!("{i}th"),
    }
}
