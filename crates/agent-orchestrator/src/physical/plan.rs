use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
    },
};
use llms::chat::Chat;
use serde::{Deserialize, Serialize};

use crate::logical::{
    self,
    plan::{Action, LogicalPlan},
};

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
    pub async fn tool_plan_request(
        preivous_steps: &[Step],
        step: &logical::plan::Step,
        model: &Box<dyn Chat>,
    ) -> Result<Step, async_openai::error::OpenAIError> {
        let previous_steps_body = serde_json::to_string(preivous_steps)?;
        let previous_steps_message = ChatCompletionRequestMessage::System(format!("The following steps have already been generated. For the purposes of this step, assume the previous steps have already been run successfully. Previous steps: {previous_steps_body}").into());

        let body = serde_json::to_string(step)?;
        let req = CreateChatCompletionRequestArgs::default()
            .messages(vec![
                previous_steps_message,
                ChatCompletionRequestMessage::User(body.into()),
            ])
            .build()?;

        let completion = model.chat_request(req).await?;

        let body = completion
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .ok_or_else(|| {
                OpenAIError::InvalidArgument("Could not find choice response".to_string())
            })?;

        let tool: Step = serde_json::from_str(body)?;

        // TODO: validate the tool is valid and retry if not

        Ok(tool)
    }

    pub async fn plan(
        logical_plan: &LogicalPlan,
        tool_planner: &Box<dyn Chat>,
        prompt_planner: &Box<dyn Chat>,
    ) -> Result<Self, async_openai::error::OpenAIError> {
        // for each task, convert the list of steps from the logical plan based on their StepType
        let mut tasks: Vec<Task> = vec![];
        for task in &logical_plan.tasks {
            let mut steps: Vec<Step> = vec![];
            for step in &task.steps {
                match step.action.into() {
                    StepType::Tool => {
                        steps.push(
                            Self::tool_plan_request(steps.as_slice(), step, tool_planner).await?,
                        );
                    }
                    StepType::Prompt => {
                        todo!(); // call the prompt physical planner
                    }
                }
            }

            tasks.push(Task {
                objective: task.objective.clone(),
                steps,
            });
        }

        Ok(Self { tasks })
    }
}
