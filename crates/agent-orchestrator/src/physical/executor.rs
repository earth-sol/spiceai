use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    io::Write,
    sync::Arc,
};

use crate::{progress::Progress, validate_structured_output, ConversionError};

use super::plan::{PhysicalPlan, PromptStep, Step, ToolStep};
use async_openai::types::{
    ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
    ChatCompletionRequestUserMessageContent, CreateChatCompletionRequest,
    CreateChatCompletionRequestArgs, ResponseFormat,
};
use llms::chat::Chat;
use serde::Deserialize;
use tokio::sync::RwLock;
use tools::SpiceModelTool;
use tracing::Instrument;
pub struct PhysicalJobExecutor {
    // INPUTS
    plan: PhysicalPlan,
    llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
    tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    verifier_model: String,

    // JOB STATE
    execution_history: Vec<Vec<ChatCompletionRequestMessage>>,
}

#[allow(dead_code)]
enum ToolCallResult {
    Success,
    Failure(String),
}

impl PhysicalJobExecutor {
    #[must_use]
    pub fn new(
        plan: PhysicalPlan,
        llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
        tools: HashMap<String, Arc<dyn SpiceModelTool>>,
        verifier_model: String,
    ) -> Self {
        Self {
            plan,
            llms,
            tools,
            execution_history: vec![],
            verifier_model,
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
#[allow(dead_code)]
enum ExecuteToolError {
    ToolCallFailed {
        reason: String,
        tool_output: ChatCompletionRequestMessage,
    },
    Other(anyhow::Error),
}

impl std::error::Error for ExecuteToolError {}

impl std::fmt::Display for ExecuteToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ToolCallFailed {
                reason,
                tool_output,
            } => {
                write!(
                    f,
                    "Tool call failed: {reason}\nTool output: {tool_output:?}"
                )
            }
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

impl From<anyhow::Error> for ExecuteToolError {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}

impl PhysicalJobExecutor {
    pub async fn execute(&mut self, progress: &Progress) -> Result<String, anyhow::Error> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "orchestrator::physical_plan_execution", input = %serde_json::to_string(&self.plan).unwrap_or_default());

        let result: Result<String, anyhow::Error> = async {
            // reset physical plan execution log
            reset_execution_log();

            let mut task_history = vec![];
            let mut step_history = vec![];

            for (t, task) in self.plan.tasks.iter().enumerate() {
                let task_span = tracing::span!(target: "task_history", parent: &span, tracing::Level::INFO, "orchestrator::physical_task_execution", input = %serde_json::to_string(&task).unwrap_or_default(), task = t); // Yes

                let step_history: Vec<ChatCompletionRequestMessage> = async {
                    let t_progress = progress.with_new_task(t + 1);
                    tracing::info!("Executing task: {}", task.objective);

                    tracing::info!("Previous steps summary: {steps:?}", steps = step_history);
                    t_progress
                        .send_open_message(format!("Executing {} task", t_progress.task_str()).as_str())
                        .await;
                    for (i, step) in task.steps.iter().enumerate() {
                        let step_span = tracing::span!(target: "task_history",  parent: &task_span, tracing::Level::INFO, "orchestrator::physical_step_execution", input = %serde_json::to_string(&task).unwrap_or_default(), task = t);  // Yes
                        async {
                            let output = self
                                .execute_step(&mut step_history, step)
                                .await
                                .inspect_err(|err| {
                                    trace_execution_progress(step, &err.to_string());
                                    tracing::error!(target: "task_history", parent: &step_span, "{err}");
                                    tracing::error!(target: "task_history", parent: &task_span, "{err}");
                                })?;
                            tracing::info!(
                                target: "task_history",
                                parent: &step_span,
                                captured_output = %serde_json::to_string(&output).unwrap_or_default()
                            );
                            tracing::info!("Step output: {output:?}");
                            step_history.push(output);
                            let s_progress = t_progress.with_new_step(i + 1);
                            s_progress
                                .send_complete_message(
                                    format!(
                                        "Finished executing {} step in {} task.",
                                        s_progress.step_str(),
                                        s_progress.task_str()
                                    )
                                    .as_str(),
                                )
                                .await;
                            Ok::<(), anyhow::Error>(())
                        }
                        .instrument(step_span.clone())
                        .await?;
                    };
                    t_progress
                        .send_close_message(Some(
                            format!("Completed executing {} task.", t_progress.task_str()).as_str(),
                        ))
                        .await;

                    self
                        .summarize_executed_steps(&self.verifier_model, &step_history)
                        .await
                        .inspect_err(|e| tracing::error!(target: "task_history", parent: &task_span, "{e}"))

                }.instrument(task_span.clone()).await?;
                self.execution_history.push(step_history.clone());

                task_history.extend(step_history.clone());
            }

            let summary = self
                .final_summary(&self.verifier_model, &task_history)
                .await?;

            Ok(summary)
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                tracing::info!(target: "task_history", parent: &span, captured_output = %value);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    async fn execute_step(
        &self,
        step_history: &mut Vec<ChatCompletionRequestMessage>,
        step: &Step,
    ) -> Result<ChatCompletionRequestMessage, anyhow::Error> {
        match step {
            Step::Tool(tool_step) => self
                .execute_tool(step_history, tool_step)
                .await
                .map_err(|e| anyhow::anyhow!("Error executing tool: {e}")),
            Step::Prompt(prompt_step) => self.execute_prompt(step_history, prompt_step).await,
        }
    }

    async fn execute_prompt(
        &self,
        step_history: &mut Vec<ChatCompletionRequestMessage>,
        step: &PromptStep,
    ) -> Result<ChatCompletionRequestMessage, anyhow::Error> {
        let prompt = step.prompt.clone();
        let llms = &*self.llms.read().await;
        let model = llms
            .get(&step.model)
            .ok_or_else(|| anyhow::anyhow!("Model {} not found", step.model))?;

        step_history.push(ChatCompletionRequestMessage::User(prompt.into()));

        let messages = step_history.clone();

        let req = CreateChatCompletionRequestArgs::default()
            .messages(messages)
            .model(step.model.clone())
            .build()
            .map_err(|e| {
                anyhow::anyhow!("Error building chat completion request: {}", e.to_string())
            })?;
        let response = model.chat_request(req).await?;

        let Some(message) = response.choices[0].message.content.clone() else {
            return Err(anyhow::anyhow!("No message content found"));
        };

        trace_execution_progress(&Step::Prompt(step.clone()), &message);

        let tool_message = ChatCompletionRequestUserMessageArgs::default()
            .content(ChatCompletionRequestUserMessageContent::Text(message))
            .build()
            .map_err(|e| anyhow::anyhow!("Error building tool message: {}", e.to_string()))?;
        Ok(ChatCompletionRequestMessage::User(tool_message))
    }

    async fn execute_tool(
        &self,
        step_history: &[ChatCompletionRequestMessage],
        step: &ToolStep,
    ) -> Result<ChatCompletionRequestMessage, ExecuteToolError> {
        let tool = self
            .tools
            .get(&step.tool)
            .ok_or_else(|| anyhow::anyhow!("Tool {} not found", step.tool))?;

        let response = tool
            .call(step.body.as_str())
            .await
            .map_err(|e| anyhow::anyhow!("Error calling tool {}: {}", step.tool, e.to_string()))?;

        let response_str = response.to_string();

        trace_execution_progress(&Step::Tool(step.clone()), &response_str);

        let tool_message_content = ChatCompletionRequestUserMessageContent::Text(response_str);

        let tool_message = ChatCompletionRequestUserMessageArgs::default()
            .content(tool_message_content)
            .build()
            .map_err(|e| anyhow::anyhow!("Error building tool message: {}", e.to_string()))?;
        let request_message = ChatCompletionRequestMessage::User(tool_message);

        let result = self
            .tool_call_succeeded(
                step_history,
                request_message.clone(),
                &self.verifier_model,
                &step.success_criteria,
                None,
                None,
            )
            .await?;

        let result_message = ChatCompletionRequestMessage::Assistant(result.to_string().into());

        Ok(result_message)
    }

    async fn loop_success_validator(
        &self,
        req: CreateChatCompletionRequest,
        model: &dyn Chat,
    ) -> Result<(VerificationResponse, String), anyhow::Error> {
        let mut iteration = 0;
        loop {
            let response = model.chat_request(req.clone()).await?;
            let verification: Result<VerificationResponse, ConversionError> =
                validate_structured_output(
                    include_str!("executor_response_format.yaml"),
                    &response,
                );

            let Some(message) = response.choices[0].message.content.clone() else {
                return Err(anyhow::anyhow!("No message content found"));
            };

            match verification {
                Ok(v) => return Ok((v, message)),
                Err(ConversionError::SerdeJson(e)) => {
                    return Err(anyhow::anyhow!("Failed to parse tool step: {e}"));
                }
                Err(ConversionError::SerdeYaml(e)) => {
                    return Err(anyhow::anyhow!("Failed to parse tool step: {e}"));
                }
                Err(ConversionError::JsonSchema(e)) => {
                    if iteration > 3 {
                        return Err(anyhow::anyhow!("Failed to validate tool step: {e}"));
                    }

                    tracing::warn!(
                        "Structured output for success validation was invalid. Retrying..."
                    );
                    iteration += 1;
                    continue;
                }
            }
        }
    }

    #[allow(dead_code, clippy::too_many_lines)]
    async fn tool_call_succeeded(
        &self,
        step_history: &[ChatCompletionRequestMessage],
        tool_output: ChatCompletionRequestMessage,
        model_name: &str,
        success_criteria: &str,
        iteration: Option<usize>,
        extra_message: Option<ChatCompletionRequestMessage>,
    ) -> Result<VerificationResponse, anyhow::Error> {
        let iteration = iteration.unwrap_or(0);

        let mut messages = step_history.to_vec();
        messages.push(tool_output.clone());
        messages.push(ChatCompletionRequestMessage::User(
            format!("# Goal

            The previous message was a tool call. Classify if the previous tool call was successful or not, based on the output of the previous tool call and the step's success criteria.

            Plan the steps you need to take to verify the success criteria, and execute the steps to verify the success criteria.

            # Success Criteria

            {success_criteria}

            # Guidelines

            1. A non-zero status code does not always mean the tool call failed - ensure you inspect the relevant tool output, or call additional tools if needed, to determine if the output actually indicates a failure.
            2. If the tool call message indicates a terminal command was executed, ensure you retrieve the relevant terminal output and classify based on the retrieved terminal output.
            3. Ensure you consider that the existence of an Stderr output does not always mean the call failed - ensure you inspect the Stderr output, to determine if the output actually indicates a failure.
            4. Use any of your available tools to verify the success criteria has been met.
            5. If there is no evidence or confirmation from the tool output that the success criteria has been met, make tool calls to retrieve the relevant information to verify the success criteria has been met.
            6. If additional tool calls are required to classify success, make them before classifying.

            # Example Successful Classification Process

            1. Inspect the tool output. The tool output talks about a terminal command that completed with status code 4, with a method to access terminal logs based on a terminal ID.
            2. Because the status code is non-zero, we need to inspect the terminal logs to determine if the command actually failed.
            3. Make a tool call to retrieve the relevant terminal logs.
            4. The terminal logs include no error messages, and the command output is as expected.
            5. The tool call is classified as successful.

            # Example Failed Classification Process

            1. Inspect the tool step. The tool step talks about removing some files.
            2. Because the tool step talks about removing files, we need to inspect the filesystem to determine if the files were actually removed.
            3. Make a tool call to retrieve the relevant filesystem information.
            4. The filesystem information includes the list of files, and the files are still present.
            5. The tool call is classified as failed.

            # Example Inconclusive Classification Process

            1. Inspect the tool step. The tool step talks about a terminal command that is still running.
            2. Make a tool call to retrieve the relevant terminal logs.
            3. The terminal logs include no error messages.
            4. Because the process is still running, the tool call is classified as inconclusive.

            In this situation, we should wait a few minutes and check again.
            On the second check:

            1. Make a tool call to retrieve the terminal logs.
            2. The terminal logs include no error messages, and the terminal is idle.
            3. The tool call is classified as successful.
            ",
        ).into()));

        if let Some(extra_message) = extra_message {
            messages.push(extra_message);
        }

        let llms = &*self.llms.read().await;
        let model = llms
            .get(model_name)
            .ok_or_else(|| anyhow::anyhow!("Model {} not found", model_name))?;

        let yaml_str = include_str!("executor_response_format.yaml");
        let Ok(yaml_value) = serde_yaml::from_str::<ResponseFormat>(yaml_str) else {
            return Err(anyhow::anyhow!(
                "Failed to parse response format: {yaml_str}"
            ));
        };

        let req = CreateChatCompletionRequestArgs::default()
            .response_format(yaml_value)
            .messages(messages.clone())
            .build()
            .map_err(|e| {
                anyhow::anyhow!("Error building chat completion request: {}", e.to_string())
            })?;
        let (verification, message) = self.loop_success_validator(req, model.as_ref()).await?;

        if verification.status == VerificationStatus::Succeeded {
            tracing::info!("Tool call success: {}", verification.reason);
            Ok(verification)
        } else {
            tracing::error!("Tool call failed: {}", verification.reason);
            if verification.status == VerificationStatus::Inconclusive {
                tracing::warn!("Model thinks the result is inconclusive");
            }

            match (verification.status.clone(), verification.wait) {
                (VerificationStatus::Failed | VerificationStatus::Inconclusive, true) => {
                    if iteration < 3 {
                        tracing::warn!("Model thinks waiting might be needed to verify result");
                        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                        let mut messages = step_history.to_vec();

                        messages.push(ChatCompletionRequestMessage::Assistant(message.into()));

                        return Box::pin(self.tool_call_succeeded(
                            step_history,
                            tool_output,
                            model_name,
                            success_criteria,
                            Some(iteration + 1),
                            Some(ChatCompletionRequestMessage::User(
                                format!("# Status

                                You have already checked {} times if the previous tool call was successful or not, but you were unable to verify the result of the tool call.
                                Last time, you thought waiting might be needed to verify the result of the previous tool call.

                                # Next Steps

                                On the next success criteria check, call all neccessary tools to collect additional information as the state will have changed since the last check.", iteration+1).into()
                            ))
                        ))
                        .await;
                    }

                    tracing::warn!("Model thinks waiting might be needed to verify result, but we have already waited 3 times");
                }
                (VerificationStatus::Inconclusive, false) => {
                    tracing::warn!("Model thinks the result is inconclusive, but does not think waiting is needed");
                    if iteration < 3 {
                        tracing::warn!("Model thinks waiting might be needed to verify result");
                        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

                        messages.push(ChatCompletionRequestMessage::Assistant(message.into()));

                        return Box::pin(self.tool_call_succeeded(
                            &messages,
                            tool_output,
                            model_name,
                            success_criteria,
                            Some(iteration + 1),
                            Some(ChatCompletionRequestMessage::System(
                                format!("# Status

                                You have already checked {} times if the previous tool call was successful or not, but you came to an inconclusive result.
                                On the next success criteria check, ensure you call all relevant tools to collect the additional verification you require.

                                This could include collecting updated terminal logs, as the state may have changed since the last check.", iteration+1).into()
                            ))
                        ))
                        .await;
                    }
                }
                _ => {}
            }

            Ok(verification)
        }
    }

    async fn summarize_executed_steps(
        &self,
        model: &str,
        step_history: &[ChatCompletionRequestMessage],
    ) -> Result<Vec<ChatCompletionRequestMessage>, anyhow::Error> {
        let mut messages: Vec<ChatCompletionRequestMessage> = vec![]; // step_history.to_vec();

        let content = step_history
            .iter()
            .map(|msg| format!("{msg:?}"))
            .collect::<Vec<String>>()
            .join("\n\n");

        messages.push(ChatCompletionRequestMessage::User(
            format!("# Goal

            Summarize the steps below that have been executed previously.
             - Only include main results, list of steps completed,
             - Incldue learnings, hints and important details that could be useful to effectively execute further tasks.

            Summary must be concise, clear and short.

            **Steps**

            {content}
            ").into(),
        ));

        let llms = &*self.llms.read().await;
        let model = llms
            .get(model)
            .ok_or_else(|| anyhow::anyhow!("Model {} not found", model))?;

        let req = CreateChatCompletionRequestArgs::default()
            .messages(messages)
            .build()
            .map_err(|e| {
                anyhow::anyhow!("Error building chat completion request: {}", e.to_string())
            })?;
        let response = model.chat_request(req).await?;

        let Some(summary) = response.choices[0].message.content.clone() else {
            return Err(anyhow::anyhow!("No message content found"));
        };

        let message = ChatCompletionRequestUserMessageContent::Text(format!("Find information about previously executed tasks to help complete your next steps\n\n# Previous Steps\n\n{summary}"));

        Ok(vec![ChatCompletionRequestMessage::User(message.into())])
    }

    async fn final_summary(
        &self,
        model: &str,
        step_history: &[ChatCompletionRequestMessage],
    ) -> Result<String, anyhow::Error> {
        let mut messages: Vec<ChatCompletionRequestMessage> = vec![]; // step_history.to_vec();

        let content = step_history
            .iter()
            .map(|msg| format!("{msg:?}"))
            .collect::<Vec<String>>()
            .join("\n\n");

        messages.push(ChatCompletionRequestMessage::User(
            format!("# Goal

            Create a final summary of all executed steps and tasks.
            Include a final note, with an emoji for check or cross, for whether the task was successful or not.

            Collect information from tool calls if you need additional information to complete the summary.

            # Steps

            {content}
            ").into(),
        ));

        let llms = &*self.llms.read().await;
        let model = llms
            .get(model)
            .ok_or_else(|| anyhow::anyhow!("Model {} not found", model))?;

        let req = CreateChatCompletionRequestArgs::default()
            .messages(messages)
            .build()
            .map_err(|e| {
                anyhow::anyhow!("Error building chat completion request: {}", e.to_string())
            })?;
        let response = model.chat_request(req).await?;

        let Some(summary) = response.choices[0].message.content.clone() else {
            return Err(anyhow::anyhow!("No message content found"));
        };

        Ok(summary)
    }
}

fn trace_execution_progress(step: &Step, output: &str) {
    let task_id = match step.task_id() {
        Some(id) => id.to_string(),
        None => "None".to_string(),
    };
    match step {
        Step::Prompt(prompt) => {
            log_execution_update(&format!(
                "Task ID: {task_id}, calling model {} to complete action: {}",
                prompt.model, prompt.prompt
            ));
        }
        Step::Tool(tool_step) => {
            log_execution_update(&format!(
                "Task ID: {task_id}, calling tool: {}\n{}",
                tool_step.tool, tool_step.body
            ));
        }
    }
    log_execution_update(&format!("Task ID: {task_id}, tool response:\n{output}",));
}

fn reset_execution_log() {
    let log_path = "data/physical/physical_plan_execution.log";
    if let Err(e) = std::fs::remove_file(log_path) {
        tracing::error!("Failed to reset execution log file: {e}");
    }
}

fn log_execution_update(update_message: &str) {
    tracing::debug!(update_message);

    let log_path = "data/physical/physical_plan_execution.log";
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);

    if let Err(e) = options
        .open(log_path)
        .and_then(|mut file| writeln!(file, "{update_message}"))
    {
        tracing::error!("Failed to write execution update to log: {e}");
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct VerificationResponse {
    pub status: VerificationStatus,
    pub reason: String,
    pub wait: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum VerificationStatus {
    Succeeded,
    Failed,
    Inconclusive,
}

impl Display for VerificationResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "# Tool Call Result\n\n{}\n\n# Reasoning\n\n{}",
            self.status, self.reason
        )
    }
}

impl Display for VerificationStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            VerificationStatus::Succeeded => write!(f, "Succeeded"),
            VerificationStatus::Failed => write!(f, "Failed"),
            VerificationStatus::Inconclusive => write!(f, "Inconclusive"),
        }
    }
}
