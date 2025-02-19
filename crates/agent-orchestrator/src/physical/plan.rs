use async_openai::types::CreateChatCompletionResponse;
use serde::{Deserialize, Serialize};

use crate::logical::plan::{Action, LogicalPlan};

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
    pub tasks: Vec<Task>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    pub objective: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Step {
    Tool {
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

#[derive(Debug, Clone, Copy)]
pub enum StepType {
    Tool,
    Prompt,
}

impl From<Action> for StepType {
    fn from(action: Action) -> Self {
        match action {
            Action::ChangeDirectory
            | Action::CreateDirectory
            | Action::ReadObject
            | Action::WriteObject
            | Action::ExecuteTerminal => StepType::Tool,
            Action::Other
            | Action::Response
            | Action::RequestForInfo
            | Action::RetrieveMetadata
            | Action::Validation
            | Action::Improvement => StepType::Prompt,
        }
    }
}

impl PhysicalPlan {
    pub fn plan(logical_plan: &LogicalPlan) -> Result<Self, async_openai::error::OpenAIError> {
        // for each task, convert the list of steps from the logical plan based on their StepType
        let tasks: Vec<Task> = vec![];
        for task in &logical_plan.tasks {
            let steps: Vec<Step> = vec![];
            for step in &task.steps {
                match step.action.into() {
                    StepType::Tool => {
                        todo!(); // call the task physical planner
                    }
                    StepType::Prompt => {
                        todo!(); // call the prompt physical planner
                    }
                }
            }
        }
        todo!();
    }
}
