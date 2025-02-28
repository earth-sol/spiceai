use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
        CreateChatCompletionResponse,
    },
};
use llms::chat::Chat;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tools::{ListToolElement, SpiceModelTool};
use tracing::{Instrument, Span};
use uuid::Uuid;

use crate::{
    logical::{
        self,
        plan::{Action, LogicalPlan},
    },
    progress::Progress,
    validate_structured_output, ConversionError,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PhysicalPlan {
    pub tasks: Vec<Task>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub objective: String,
    pub steps: Vec<Step>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolStep {
    pub task_uuid: Option<Uuid>,
    pub tool: String,
    pub body: String,
    pub model: String,
    pub success_criteria: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptStep {
    pub task_uuid: Option<Uuid>,
    pub prompt: String,
    pub model: String,
    pub success_criteria: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Step {
    Tool(ToolStep),
    Prompt(PromptStep),
}

impl Step {
    #[must_use]
    pub fn task_id(&self) -> Option<Uuid> {
        match self {
            Step::Tool(tool_step) => tool_step.task_uuid,
            Step::Prompt(prompt_step) => prompt_step.task_uuid,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StepType {
    Tool,
    Prompt,
}

impl Step {
    #[must_use]
    pub fn with_task_id(mut self, task_uuid: Option<Uuid>) -> Self {
        match &mut self {
            Step::Tool(tool_step) => tool_step.task_uuid = task_uuid,
            Step::Prompt(prompt_step) => prompt_step.task_uuid = task_uuid,
        }
        self
    }
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

fn get_tools(tools: &HashMap<String, Arc<dyn SpiceModelTool>>) -> String {
    let mut tools_elements = vec![];

    for (key, value) in tools {
        let tool_element = ListToolElement {
            name: key.to_string(),
            description: value.description().map(|d| d.to_string()),
            parameters: value.parameters(),
            is_catalog: false,
        };
        tools_elements.push(tool_element);
    }

    format!("{tools_elements:?}")
}

impl PhysicalPlan {
    pub(crate) fn summary() -> String {
        "Physical Plan created.".to_string()
    }

    pub fn build_request(
        messages: Option<Vec<ChatCompletionRequestMessage>>,
        previous_steps: &[Step],
        step: &logical::plan::Step,
        objective: &str,
        tools: &String,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let mut messages = messages.unwrap_or_default();
        let previous_steps_body = serde_json::to_string(previous_steps)?;
        let previous_steps_message = ChatCompletionRequestMessage::System(
            format!("# Previous Steps\n\nThe following steps have been planned already: {previous_steps_body}.").into(),
        );
        messages.push(previous_steps_message);

        let body = serde_json::to_string(step)?;
        messages.push(ChatCompletionRequestMessage::User(
            format!(
                "## Goal
                Create a detailed physical plan step that implements the logical plan step below.

                ## Task Objective
                {objective}

                ## Logical Plan Step
                {body}

                ## Available Tools
                {tools}

                # Instructions
                1. Select the most appropriate tool for this step based on the logical plan requirements
                2. Define clear, executable parameters for your chosen tool
                3. Include specific success criteria that can be evaluated
                4. Ensure your plan directly supports the overall task objective

                Your response will be validated against a strict schema, so maintain proper formatting."
            )
            .into(),
        ));
        let req = CreateChatCompletionRequestArgs::default()
            .messages(messages)
            .build()?;

        Ok(req)
    }

    pub async fn plan_request(
        req: CreateChatCompletionRequest,
        step_type: StepType,
        model: &dyn Chat,
    ) -> Result<Step, async_openai::error::OpenAIError> {
        let mut iteration = 0;
        loop {
            let completion = model.chat_request(req.clone()).await?;

            let step: Step = match step_type {
                StepType::Tool => {
                    // TODO: validate the tool is valid and retry if not
                    let step: Result<ToolStep, ConversionError> = validate_structured_output(
                        include_str!("tool_response_format.yaml"),
                        &completion,
                    );
                    match step {
                        Ok(mut step) => {
                            step.model = "orchestrator-o3-mini".to_string();
                            Step::Tool(step)
                        }
                        Err(ConversionError::SerdeYaml(e)) => {
                            return Err(OpenAIError::InvalidArgument(format!(
                                "Failed to parse tool step: {e}"
                            )));
                        }
                        Err(ConversionError::SerdeJson(e)) => {
                            return Err(OpenAIError::InvalidArgument(format!(
                                "Failed to parse tool step: {e}"
                            )));
                        }
                        Err(ConversionError::JsonSchema(e)) => {
                            if iteration > 3 {
                                return Err(OpenAIError::InvalidArgument(format!(
                                    "Failed to validate tool step: {e}"
                                )));
                            }

                            tracing::warn!(
                                "Structured output for physical planning was invalid. Retrying..."
                            );
                            iteration += 1;
                            continue;
                        }
                    }
                }
                StepType::Prompt => {
                    // TODO: validate the selected model is valid and retry if not
                    let step: Result<PromptStep, ConversionError> = validate_structured_output(
                        include_str!("prompt_response_format.yaml"),
                        &completion,
                    );
                    match step {
                        Ok(mut step) => {
                            step.model = "orchestrator-o3-mini".to_string();
                            Step::Prompt(step)
                        }
                        Err(ConversionError::SerdeYaml(e)) => {
                            return Err(OpenAIError::InvalidArgument(format!(
                                "Failed to parse prompt step: {e}"
                            )));
                        }
                        Err(ConversionError::SerdeJson(e)) => {
                            return Err(OpenAIError::InvalidArgument(format!(
                                "Failed to parse prompt step: {e}"
                            )));
                        }
                        Err(ConversionError::JsonSchema(e)) => {
                            if iteration > 3 {
                                return Err(OpenAIError::InvalidArgument(format!(
                                    "Failed to validate prompt step: {e}"
                                )));
                            }

                            tracing::warn!(
                                "Structured output for physical planning was invalid. Retrying..."
                            );
                            iteration += 1;
                            continue;
                        }
                    }
                }
            };

            return Ok(step);
        }
    }

    pub async fn plan_task(
        task: &logical::plan::Task,
        tool_planner: &dyn Chat,
        prompt_planner: &dyn Chat,
        executor: String,
        progress: Progress,
        tools: &HashMap<String, Arc<dyn SpiceModelTool>>,
    ) -> Result<Task, async_openai::error::OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "orchestrator::physical_plan_task", input = %serde_json::to_string(&task).unwrap_or_default(), executor=executor); // No
        let result: Result<Task, OpenAIError> = async {
            tracing::info!("Generating physical plan for task: {}", task.objective);
            let mut steps: Vec<Step> = vec![];
        let tool_list = get_tools(tools);
            for (i, step) in task.steps.iter().enumerate() {
                let step_span = tracing::span!(
                    target: "task_history", tracing::Level::INFO, "orchestrator::physical_plan_step", // Yes
                    input = %serde_json::to_string(&step).unwrap_or_default(),
                    step_number=i
                );
                let output: Result<(), async_openai::error::OpenAIError> = async {
                    tracing::info!("Generating physical plan for step: {:?}", step.uuid);
                    let result = match step.action.into() {
                        StepType::Tool => {
                            let message = vec![ChatCompletionRequestMessage::System(
                                format!("The following models are available for selection: {executor}")
                                    .into(),
                            )];
                            let req = Self::build_request(
                                Some(message),
                                steps.as_slice(),
                                step,
                                &task.objective,
                        &tool_list,
                            ).inspect_err(|e| tracing::error!(target: "task_history", parent: &step_span, "{e}"))?;
                            Self::plan_request(req, StepType::Tool, tool_planner).await
                        }
                        StepType::Prompt => {
                            let message = vec![ChatCompletionRequestMessage::System(
                                format!("The following models are available for selection: {executor}")
                                    .into(),
                            )];
                            let req = Self::build_request(
                                Some(message),
                                steps.as_slice(),
                                step,
                                &task.objective,
                        &tool_list,
                            )?;
                            Self::plan_request(req, StepType::Prompt, prompt_planner).await
                        }
                    }.inspect_err(|e| tracing::error!(target: "task_history", parent: &step_span, "{e}"))?
                    .with_task_id(task.uuid);

                    tracing::info!(target: "task_history", parent: &step_span, captured_output = %serde_json::to_string(&result).unwrap_or_default());
                    steps.push(result);

                    let s_progress = progress.with_new_step(i + 1);
                    s_progress
                        .send_complete_message(
                            format!(
                                "Finished {} step in {} task.\n",
                                s_progress.step_str(),
                                s_progress.task_str()
                            )
                            .as_str(),
                        )
                        .await;

                    Ok(())
                }.instrument(step_span.clone()).await;
                output?;
            }

            progress
                .send_complete_message(format!("Finished {} task.\n", progress.task_str()).as_str())
                .await;

            Ok(Task {
                objective: task.objective.clone(),
                steps,
            })
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                tracing::info!(target: "task_history", captured_output = %serde_json::to_string(&value).unwrap_or_default());
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    pub async fn plan(
        logical_plan: &LogicalPlan,
        tool_planner: &dyn Chat,
        prompt_planner: &dyn Chat,
        executor: String,
        progress: &Progress,
        tools: &HashMap<String, Arc<dyn SpiceModelTool>>,
    ) -> Result<Self, async_openai::error::OpenAIError> {
        // for each task, convert the list of steps from the logical plan based on their StepType
        let futs = logical_plan.tasks.iter().enumerate().map(|(i, t)| {
            Self::plan_task(
                t,
                tool_planner,
                prompt_planner,
                executor.clone(),
                progress.with_new_task(i + 1),
                tools,
            )
            .instrument(Span::current())
        });
        let tasks = futures::future::try_join_all(futs)
            .instrument(Span::current())
            .await?;

        Ok(Self { tasks })
    }
}
