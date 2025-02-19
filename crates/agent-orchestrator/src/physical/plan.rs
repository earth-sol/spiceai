use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
        CreateChatCompletionResponse,
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
pub struct ToolStep {
    pub tool: String,
    pub body: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PromptStep {
    pub prompt: String,
    pub target_model: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Step {
    Tool(ToolStep),
    Prompt(PromptStep),
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
    pub fn build_request(
        messages: Option<Vec<ChatCompletionRequestMessage>>,
        previous_steps: &[Step],
        step: &logical::plan::Step,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let mut messages = messages.unwrap_or_default();

        let previous_steps_body = serde_json::to_string(previous_steps)?;
        let previous_steps_message = ChatCompletionRequestMessage::System(format!("The following steps have already been generated. For the purposes of this step, assume the previous steps have already been run successfully. Previous steps: {previous_steps_body}").into());
        messages.push(previous_steps_message);

        let body = serde_json::to_string(step)?;
        messages.push(ChatCompletionRequestMessage::User(body.into()));
        let req = CreateChatCompletionRequestArgs::default()
            .messages(messages)
            .build()?;

        Ok(req)
    }

    pub async fn plan_request(
        req: CreateChatCompletionRequest,
        step_type: StepType,
        model: &Box<dyn Chat>,
    ) -> Result<Step, async_openai::error::OpenAIError> {
        let completion = model.chat_request(req).await?;

        let body = completion
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .ok_or_else(|| {
                OpenAIError::InvalidArgument("Could not find choice response".to_string())
            })?;

        let step: Step = match step_type {
            StepType::Tool => {
                // TODO: validate the tool is valid and retry if not
                Step::Tool(serde_json::from_str::<ToolStep>(body).map_err(|e| {
                    OpenAIError::InvalidArgument(format!("Failed to parse tool step: {e}"))
                })?)
            }
            StepType::Prompt => {
                // TODO: validate the selected model is valid and retry if not
                Step::Prompt(serde_json::from_str::<PromptStep>(body).map_err(|e| {
                    OpenAIError::InvalidArgument(format!("Failed to parse prompt step: {e}"))
                })?)
            }
        };

        Ok(step)
    }

    pub async fn plan(
        logical_plan: &LogicalPlan,
        tool_planner: &Box<dyn Chat>,
        prompt_planner: &Box<dyn Chat>,
        model_names: Vec<String>,
    ) -> Result<Self, async_openai::error::OpenAIError> {
        // for each task, convert the list of steps from the logical plan based on their StepType
        let mut tasks: Vec<Task> = vec![];
        for task in &logical_plan.tasks {
            let mut steps: Vec<Step> = vec![];
            for step in &task.steps {
                println!("Generating physical plan for step: {:?}", step.uuid);
                match step.action.into() {
                    StepType::Tool => {
                        let req = Self::build_request(None, steps.as_slice(), step)?;
                        steps.push(Self::plan_request(req, StepType::Tool, tool_planner).await?);
                    }
                    StepType::Prompt => {
                        let message = vec![ChatCompletionRequestMessage::System(
                            "The following models are available for selection: o3-mini".into(), // update to the actual list of models, trimming the agentic models
                        )];
                        let req = Self::build_request(Some(message), steps.as_slice(), step)?;
                        steps
                            .push(Self::plan_request(req, StepType::Prompt, prompt_planner).await?);
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
