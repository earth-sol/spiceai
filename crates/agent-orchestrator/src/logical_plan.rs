use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct LogicalPlan {
    pub groups: Vec<Group>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Group {
    pub id: i64,
    pub description: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Step {
    pub id: i64,
    pub description: String,
    #[serde(rename = "type")]
    pub step_type: StepType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepType {
    ToolUse,
    Response,
    RequestForInfo,
    Validation,
    Improvement,
}
