use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, RwLock},
};

use serde::Serialize;

use crate::pipeline::AgenticStage;

#[derive(Clone, Copy, Debug)]
pub enum StageName {
    Research,
    LogicalPlan,
    PhysicalPlan,
    Execution,
    Reporting,
}

impl StageName {
    pub fn id(self) -> &'static str {
        match self {
            Self::Research => "research",
            Self::LogicalPlan => "logical_plan",
            Self::PhysicalPlan => "physical_plan",
            Self::Execution => "execution",
            Self::Reporting => "reporting",
        }
    }
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
        write!(f, "{}", self.id())
    }
}

/// The type of progress message to send.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProgressType {
    /// The output of a stage, task, or step has been evaluated.
    #[serde(rename = "eval")]
    Evaluation,
    /// An error has occurred.
    #[serde(rename = "err")]
    Error,
    /// A warning has occurred.
    #[serde(rename = "warn")]
    Warning,
    /// A log message associated with a stage, task, or step.
    Log,
}

impl Display for ProgressType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ProgressType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Evaluation => "eval",
            Self::Error => "err",
            Self::Warning => "warn",
            Self::Log => "log",
        }
    }
}

#[derive(Clone, Serialize)]
#[allow(clippy::struct_field_names)]
pub struct Progress {
    #[serde(rename = "type")]
    progress_type: ProgressType,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    tags: HashMap<String, String>,
}

impl Progress {
    pub fn new(progress_type: ProgressType) -> Self {
        Self {
            progress_type,
            id: None,
            parent_id: None,
            title: None,
            content: None,
            tags: HashMap::new(),
        }
    }

    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn parent_id(mut self, parent_id: String) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    pub fn title(mut self, title: String) -> Self {
        self.title = Some(title);
        self
    }

    pub fn content(mut self, content: String) -> Self {
        self.content = Some(content);
        self
    }

    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn to_jsonl(&self) -> String {
        let raw_json = match serde_json::to_string(self) {
            Ok(json) => json,
            Err(_) => {
                r#"{ "type": "err", "content": "Unexpected error converting progress to JSONL" }"#
                    .to_string()
            }
        };
        format!("!---jsonl{raw_json}")
    }
}

/// Send an `Advance` message for the current stage.
pub fn advance_stage(idx: &Arc<RwLock<Index>>, next_stage: StageName) {
    let mut idx = match idx.write() {
        Ok(idx) => idx,
        Err(e) => e.into_inner(),
    };
    idx.stage = next_stage;
    let progress = Progress::new(ProgressType::Log)
        .id(idx.id().to_string())
        .content(idx.starting_message())
        .title(idx.title())
        .to_jsonl();
    tracing::info!(target: "task_history", progress = %progress);
}

pub fn current_stage(idx: &Arc<RwLock<Index>>) -> StageName {
    let idx = match idx.read() {
        Ok(idx) => idx,
        Err(e) => e.into_inner(),
    };
    idx.stage
}

/// Index points to a specific part with an [`AgenticStage`]. Some stages don't have subtasks or substeps.
#[derive(Clone, Debug)]
pub struct Index {
    pub stage: StageName,
}

impl Index {
    pub(crate) fn new(stage: StageName) -> Self {
        Self { stage }
    }

    pub(crate) fn log(idx: &Arc<RwLock<Index>>, log: String) -> String {
        let idx = match idx.read() {
            Ok(idx) => idx,
            Err(e) => e.into_inner(),
        };
        let progress = Progress::new(ProgressType::Log)
            .parent_id(idx.id().to_string())
            .content(log);
        progress.to_jsonl()
    }

    pub(crate) fn title(&self) -> String {
        match self.stage {
            StageName::Research => "Researching".to_string(),
            StageName::LogicalPlan => "Logical Planning".to_string(),
            StageName::PhysicalPlan => "Execution Planning".to_string(),
            StageName::Execution => "Executing".to_string(),
            StageName::Reporting => "Generating Report".to_string(),
        }
    }

    pub(crate) fn starting_message(&self) -> String {
        match self.stage {
            StageName::Research => "Starting research...".to_string(),
            StageName::LogicalPlan => "Creating logical plan...".to_string(),
            StageName::PhysicalPlan => "Creating execution plan...".to_string(),
            StageName::Execution => "Executing...".to_string(),
            StageName::Reporting => "Generating report...".to_string(),
        }
    }

    pub(crate) fn id(&self) -> &'static str {
        self.stage.id()
    }
}
