use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    io::Write,
    sync::Arc,
};

use crate::{
    physical::agentic_retry::extract_content,
    progress::{Progress, ProgressType, StageName},
    validate_structured_output, ConversionError,
};

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
    pub async fn execute(&mut self) -> Result<String, anyhow::Error> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "orchestrator::physical_plan_execution", input = %serde_json::to_string(&self.plan).unwrap_or_default());
        let progress =
            Progress::new(ProgressType::Log).parent_id(StageName::Execution.id().to_string());

        let result: Result<String, anyhow::Error> = async {
            // reset physical plan execution log
            reset_execution_log();

            let mut task_history = vec![];
            let mut step_history = vec![];

            for (t, task) in self.plan.tasks.iter().enumerate() {
                let task_span = tracing::span!(target: "task_history", parent: &span, tracing::Level::INFO, "orchestrator::physical_task_execution", input = %serde_json::to_string(&task).unwrap_or_default(), task = t); // Yes

                let step_history: Vec<ChatCompletionRequestMessage> = async {
                    let t_progress = progress.clone().tag("task", format!("{}", t + 1));
                    tracing::info!(target: "task_history", progress = %t_progress.clone().title(task.objective.clone()).content(format!("Executing task: {}", task.objective)).to_jsonl());

                    tracing::info!("Previous steps summary: {steps:?}", steps = step_history);
                    for (i, step) in task.steps.iter().enumerate() {
                        let s_progress = t_progress.clone().tag("step", format!("{}", i + 1));
                        let step_span = tracing::span!(target: "task_history",  parent: &task_span, tracing::Level::INFO, "orchestrator::physical_step_execution", input = %serde_json::to_string(&task).unwrap_or_default(), task = t);  // Yes
                        async {
                            let output = self
                                .execute_step(&mut step_history, step, s_progress.clone())
                                .await
                                .inspect_err(|err| {
                                    trace_execution_progress(step, &err.to_string());
                                    tracing::error!(target: "task_history", parent: &step_span, "{err}");
                                    let e_progress = s_progress.clone().content(format!("{err}")).to_jsonl();
                                    tracing::error!(target: "task_history", progress = %e_progress);
                                })?;
                            tracing::info!(
                                target: "task_history",
                                parent: &step_span,
                                captured_output = %serde_json::to_string(&output).unwrap_or_default()
                            );
                            let output_content = extract_content(&output)?;
                            let step_output_progress = s_progress.clone().content(output_content).to_jsonl();
                            tracing::info!(target: "task_history", progress = %step_output_progress);

                            let s_progress = s_progress.content(step.execute_step_summary()).to_jsonl();
                            tracing::info!(target: "task_history", progress = %s_progress);
                            step_history.push(output);
                            Ok::<(), anyhow::Error>(())
                        }
                        .instrument(step_span.clone())
                        .await?;
                    };

                    self
                        .summarize_executed_steps(&self.verifier_model, &step_history)
                        .await
                        .inspect_err(|e| {
                            tracing::error!(target: "task_history", parent: &task_span, "{e}");
                            let e_progress = t_progress.clone().content(format!("{e}")).to_jsonl();
                            tracing::error!(target: "task_history", progress = %e_progress);
                        })
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
                let e_progress = progress.clone().content(format!("{e}")).to_jsonl();
                tracing::error!(target: "task_history", progress = %e_progress);
                Err(e)
            }
        }
    }

    async fn execute_step(
        &self,
        step_history: &mut Vec<ChatCompletionRequestMessage>,
        step: &Step,
        progress: Progress,
    ) -> Result<ChatCompletionRequestMessage, anyhow::Error> {
        match step {
            Step::Tool(tool_step) => self
                .execute_tool(step_history, tool_step, progress)
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
        progress: Progress,
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
                progress,
            )
            .await?;

        let result_message = ChatCompletionRequestMessage::Assistant(result.to_string().into());

        Ok(result_message)
    }

    async fn loop_success_validator(
        &self,
        mut req: CreateChatCompletionRequest,
        model: &dyn Chat,
    ) -> Result<(VerificationResponse, String), anyhow::Error> {
        let mut iteration = 0;
        let mut messages = req.messages.clone();
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
                    if iteration > 5 {
                        return Err(anyhow::anyhow!("Failed to validate tool step: {e}"));
                    }

                    tracing::warn!(
                        "Structured output for success validation was invalid. Retrying..."
                    );
                    iteration += 1;

                    // 1) Include the assistant’s previous, invalid payload
                    messages.push(ChatCompletionRequestMessage::Assistant(
                        message.clone().into(),
                    ));

                    // 2) Push a user message with the precise validation feedback
                    messages.push(
                        ChatCompletionRequestMessage::User(
                            format!(
                                "Your last response did not pass the structured output schema validation: {}.\n\
                                Please re-generate the response exactly matching the structured output schema, \
                                including all required fields (status, reason, wait) and no extra properties.",
                                e
                            ).into()
                        )
                    );

                    req.messages = messages.clone();
                    continue;
                }
            }
        }
    }

    #[allow(dead_code, clippy::too_many_lines, clippy::too_many_arguments)]
    async fn tool_call_succeeded(
        &self,
        step_history: &[ChatCompletionRequestMessage],
        tool_output: ChatCompletionRequestMessage,
        model_name: &str,
        success_criteria: &str,
        iteration: Option<usize>,
        extra_message: Option<ChatCompletionRequestMessage>,
        progress: Progress,
    ) -> Result<VerificationResponse, anyhow::Error> {
        let iteration = iteration.unwrap_or(0);

        let mut messages = step_history.to_vec();
        messages.push(tool_output.clone());
        messages.push(ChatCompletionRequestMessage::User(
            format!(
                "# Verify Tool Call Success

                ## Success Criteria
                {success_criteria}

                ## Guidelines
                1. Non-zero status codes aren't always failures - examine the actual output
                2. Background processes may run without returning status codes
                3. Terminal commands need output verification beyond status codes
                4. Stderr output doesn't always indicate failure
                5. Use tools to verify success criteria if needed
                6. Make additional tool calls if verification requires more data
                7. Classify after thorough investigation

                ## Expected Classification
                - `succeeded`: Success criteria met with evidence
                - `failed`: Evidence shows criteria not met
                - `inconclusive`: Need more information to determine

                ## Example Successful Classification Process
                1. Inspect the tool output. The tool output talks about a terminal command that completed with status code 4, with a method to access terminal logs based on a terminal ID.
                2. Because the status code is non-zero, we need to inspect the terminal logs to determine if the command actually failed.
                3. Make a tool call to retrieve the relevant terminal logs.
                4. The terminal logs include no error messages, and the command output is as expected.
                5. The tool call is classified as successful.

                ## Example Successful Classification Process
                1. Inspect the tool output. The tool output shows that a background service `postgres` was started with a command that exited with status code 0.
                2. Make a tool call to check if the daemon process is running with `ps -aux | grep postgres`.
                3. The tool output confirms the daemon is running with PID 12345.
                4. Make a tool call to retrieve the relevant terminal logs.
                5. The logs show some error messages about configuration warnings, but the service is still running.
                6. The tool call is classified as successful because the daemon is running as expected, despite the warnings in the logs.
                7. The warning details are included in the reason section of the verification response.

                ## Example Failed Classification Process
                1. Inspect the tool step. The tool step talks about removing some files.
                2. Because the tool step talks about removing files, we need to inspect the filesystem to determine if the files were actually removed.
                3. Make a tool call to retrieve the relevant filesystem information.
                4. The filesystem information includes the list of files, and the files are still present.
                5. The tool call is classified as failed.

                ## Example Inconclusive Classification Process
                1. Inspect the tool step. The tool step talks about a terminal command that is still running.
                2. Make a tool call to retrieve the relevant terminal logs.
                3. The terminal logs include no error messages.
                4. Because the process is still running, the tool call is classified as inconclusive.

                In this situation, we should wait a few minutes and check again.
                On the second check:
                1. Make a tool call to retrieve the terminal logs.
                2. The terminal logs include no error messages, and the terminal is idle.
                3. The tool call is classified as successful.

                ## Response Format
                - Status: [`succeeded`,`failed`,`inconclusive`]
                - Reasoning: Clear explanation of your classification
                - Should we wait and retry later: [true/false]"
            )
            .into(),
        ));

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

                        return Box::pin(
                            self.tool_call_succeeded(
                                step_history,
                                tool_output,
                                model_name,
                                success_criteria,
                                Some(iteration + 1),
                                Some(ChatCompletionRequestMessage::User(
                                    format!(
                                        "# Verification Check #{} Required

                                        Previous attempts to verify the tool call were inconclusive, and you recommended waiting.

                                        ## Instructions
                                        1. The system state has likely changed during the waiting period
                                        2. Use appropriate tools to gather fresh evidence:
                                        - Check terminal logs
                                        - Verify process status
                                        - Examine file contents/permissions
                                        - Confirm network connectivity
                                        3. Make a definitive assessment based on the success criteria
                                        4. Provide clear reasoning for your conclusion

                                        Your goal is to reach a conclusive determination `succeeded` or `failed` after this waiting period.",
                                        iteration + 1
                                    )
                                    .into(),
                                )),
                                progress,
                            ),
                        )
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
                                format!(
                                    "# Verification Attempt #{} - Need Decisive Assessment

                                    Previous verification attempts were inconclusive. You MUST make a decisive determination now by:
                                    - Collecting additional evidence through appropriate tool calls
                                    - Checking updated terminal logs or process status
                                    - Verifying file system changes or network status
                                    - Examining any relevant metrics or state changes since last check

                                    Focus on gathering concrete evidence that directly addresses success criteria.
                                    Be thorough but decisive - a conclusive verdict `succeeded` or `failed` is required.",
                                    iteration + 1
                                )
                                .into()
                            )),
                            progress,
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
        let mut messages: Vec<ChatCompletionRequestMessage> = vec![];

        let content = step_history
            .iter()
            .map(|msg| format!("{msg:?}"))
            .collect::<Vec<String>>()
            .join("\n\n");

        messages.push(ChatCompletionRequestMessage::User(
            format!(
                "# Summarize Execution Steps

                Create a concise summary (100-200 words) of these execution steps focusing on:
                1. Main results achieved
                2. Completed steps
                3. Important discoveries
                4. Technical details relevant for future tasks

                Omit process descriptions and unnecessary details.

                ## Execution Steps
                {content}"
            )
            .into(),
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

        let message = ChatCompletionRequestUserMessageContent::Text(format!(
            "# Task Execution Summary
            {summary}

            ## Instructions
            - Use this summary as context for subsequent tasks
            - Reference key findings and outcomes when relevant
            - Build upon these completed actions rather than repeating work"
        ));

        Ok(vec![ChatCompletionRequestMessage::User(message.into())])
    }

    async fn final_summary(
        &self,
        model: &str,
        step_history: &[ChatCompletionRequestMessage],
    ) -> Result<String, anyhow::Error> {
        let mut messages: Vec<ChatCompletionRequestMessage> = vec![];

        let content = step_history
            .iter()
            .map(|msg| format!("{msg:?}"))
            .collect::<Vec<String>>()
            .join("\n\n");

        messages.push(ChatCompletionRequestMessage::User(
            format!(
                "# Generate Software Testing QA Report

                Create a professional report (Markdown format) with:

                1. **Executive Summary** (1-2 sentence overview)
                2. **Test Objectives** (What was tested and why, max 3 points)
                3. **Test Process** (Approach summary, max 3 points)
                4. **Results Summary** (~300 words)
                   - Key tests with success indicators (✅/❌)
                   - Issues, warnings, and info with indicators (⚠️/ℹ️)
                5. **Data & Evidence**
                   - Include relevant data, logs, and evidence
                   - Use Markdown code blocks for clarity
                5. **Learnings and Recommendations** (brief, max 3 points)
                6. **Next Steps** (brief, max 3 points)

                Be concise, highlight success/failure points clearly, and provide actionable insights.

                ## Guidelines
                - Wrap URLs, code, constants, and filenames in backticks

                ## Execution Log
                {content}"
            )
            .into(),
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
    let _ = std::fs::remove_file(log_path);
}

// TODO: This should really be a tracing call with a new subscriber for logging to a file
fn log_execution_update(update_message: &str) {
    tracing::debug!(update_message);

    let log_path = "data/physical/physical_plan_execution.log";

    // Create directories if they don't exist
    if let Err(e) = std::fs::create_dir_all("data/physical") {
        tracing::error!("Failed to create log directory: {e}");
        return;
    }

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
            "Tool Call Result: {}\n### Reasoning\n{}",
            self.status, self.reason
        )
    }
}

impl Display for VerificationStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            VerificationStatus::Succeeded => write!(f, "succeeded"),
            VerificationStatus::Failed => write!(f, "failed"),
            VerificationStatus::Inconclusive => write!(f, "inconclusive"),
        }
    }
}
