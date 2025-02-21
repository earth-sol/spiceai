use std::{collections::HashMap, io::Write, sync::Arc};

use super::plan::{PhysicalPlan, PromptStep, Step, ToolStep};
use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
        ChatCompletionRequestUserMessageContent, CreateChatCompletionRequestArgs, ResponseFormat,
        ResponseFormatJsonSchema,
    },
};
use llms::chat::Chat;
use serde::Deserialize;
use tokio::sync::RwLock;
use tools::SpiceModelTool;
pub struct PhysicalJobExecutor {
    // INPUTS
    plan: PhysicalPlan,
    llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
    tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    summarization_model: String,

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
        summarization_model: String,
    ) -> Self {
        Self {
            plan,
            llms,
            tools,
            execution_history: vec![],
            summarization_model,
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
        // reset physical plan execution log
        reset_execution_log();

        let mut step_history = vec![];

        for task in &self.plan.tasks {
            tracing::info!("Executing task: {}", task.objective);

            tracing::info!("Previous steps summary: {steps:?}", steps = step_history);

            for step in &task.steps {
                let output = self
                    .execute_step(&mut step_history, step)
                    .await
                    .inspect_err(|err| {
                        trace_execution_progress(step, &err.to_string());
                    })?;
                tracing::info!("Step output: {output:?}");
                step_history.push(output);
            }

            self.execution_history.push(step_history.clone());

            step_history = self
                .summarize_executed_steps(&self.summarization_model, &step_history)
                .await?;
        }

        // TODO: Generate a report of the execution and return as the output

        Ok("Done!".to_string())
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

        let tool_result = self
            .tool_call_succeeded(
                step_history,
                request_message.clone(),
                &step.model,
                &step.success_criteria,
                None,
            )
            .await?;

        // match tool_result {
        //     ToolCallResult::Success => (),
        //     ToolCallResult::Failure(failure_reason) => {
        //         tracing::error!("Tool call failed: {failure_reason}");
        //     }
        // }

        Ok(request_message)
    }

    #[allow(dead_code, clippy::too_many_lines)]
    async fn tool_call_succeeded(
        &self,
        step_history: &[ChatCompletionRequestMessage],
        tool_output: ChatCompletionRequestMessage,
        model_name: &str,
        success_criteria: &str,
        iteration: Option<usize>,
    ) -> Result<ToolCallResult, anyhow::Error> {
        let iteration = iteration.unwrap_or(0);

        let mut messages = step_history.to_vec();
        messages.push(tool_output.clone());
        messages.push(ChatCompletionRequestMessage::User(
            format!("# Goal

            The previous message was a tool call. Classify if the previous tool call was successful or not, based on the output of the tool call and the step's success criteria.

            # Success Criteria

            {success_criteria}

            # Guidelines

            1. A non-zero status code does not always mean the tool call failed - ensure you inspect the relevant tool output, or call additional tools if needed, to determine if the output actually indicates a failure.
            2. If the tool call message indicates a terminal command was executed, ensure you retrieve the relevant terminal output and classify based on the retrieved terminal output.
            3. Ensure you consider that the existence of an Stderr output does not always mean the call failed - ensure you inspect the Stderr output, to determine if the output actually indicates a failure.
            4. Use any of your available tools to verify the success criteria has been met.
            5. If there is no evidence or confirmation from the tool output that the success criteria has been met, make tool calls to retrieve the relevant information to verify the success criteria has been met.
            
            # Example Successful Classification Process

            1. Inspect the tool output. The tool output talks about a terminal command that completed with status code 4, with a method to access terminal logs based on a terminal ID.
            2. Because the status code is non-zero, we need to inspect the terminal logs to determine if the command actually failed.
            3. Make a tool call is made to retrieve the relevant terminal logs.
            4. The terminal logs include no error messages, and the command output is as expected.
            5. The tool call is classified as successful.
            
            # Example Failed Classification Process

            1. Inspect the tool step. The tool step talks about removing some files.
            2. Because the tool step talks about removing files, we need to inspect the filesystem to determine if the files were actually removed.
            3. Make a tool call is made to retrieve the relevant filesystem information.
            4. The filesystem information includes the list of files, and the files are still present.
            5. The tool call is classified as failed.

            # Example Inconclusive Classification Process

            1. Inspect the tool step. The tool step talks about a terminal command that is still running.
            2. Make a tool call is made to retrieve the relevant terminal logs.
            3. The terminal logs include no error messages.
            4. Because the process is still running, the tool call is classified as inconclusive.

            In this situation, we should wait a few minutes and check again. After checking again, we inspect the available terminals to see if the process is still running and the terminal logs for errors to classify again.
            ",
        ).into()));

        let llms = &*self.llms.read().await;
        let model = llms
            .get(model_name)
            .ok_or_else(|| anyhow::anyhow!("Model {} not found", model_name))?;

        let yaml_str = include_str!("executor_response_format.yaml");
        let yaml_value: ResponseFormat =
            serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

        let req = CreateChatCompletionRequestArgs::default()
            .response_format(yaml_value)
            .messages(messages.clone())
            .build()
            .map_err(|e| {
                anyhow::anyhow!("Error building chat completion request: {}", e.to_string())
            })?;
        let response = model.chat_request(req).await?;

        let Some(message) = response.choices[0].message.content.clone() else {
            return Err(anyhow::anyhow!("No message content found"));
        };

        let verification = serde_json::from_str::<VerificationResponse>(&message).map_err(|e| {
            OpenAIError::InvalidArgument(format!("Failed to parse verification response: {e}"))
        })?;

        if verification.status == VerificationStatus::Succeeded {
            tracing::info!("Tool call success: {}", verification.reason);
            Ok(ToolCallResult::Success)
        } else {
            tracing::error!("Tool call failed: {}", verification.reason);

            if verification.status == VerificationStatus::Inconclusive {
                tracing::warn!("Model thinks the result is inconclusive");
            }

            match (verification.status, verification.wait) {
                (VerificationStatus::Failed | VerificationStatus::Inconclusive, true) => {
                    if iteration < 3 {
                        tracing::warn!("Model thinks waiting might be needed to verify result");
                        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

                        let mut messages = step_history.to_vec();

                        messages.push(ChatCompletionRequestMessage::System(
                            format!("# Status
                            
                            You have already checked {} times if the previous tool call was successful or not, but you were unable to verify the result of the tool call.
                            Last time, you thought waiting might be needed to verify the result of the previous tool call.

                            On the next success criteria check, ensure you call relevant tools to verify the result of the previous tool call, like collecting updated terminal logs, as the state may have changed since the last check.", iteration+1).into()
                        ));

                        return Box::pin(self.tool_call_succeeded(
                            &messages,
                            tool_output,
                            model_name,
                            success_criteria,
                            Some(iteration + 1),
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
                        messages.push(ChatCompletionRequestMessage::System(
                            format!("# Status
                            
                            You have already checked {} times if the previous tool call was successful or not, but you came to an inconclusive result.
                            On the next success criteria check, ensure you call all relevant tools to collect the additional verification you require.
                            
                            This could include collecting updated terminal logs, as the state may have changed since the last check.", iteration+1).into()
                        ));

                        return Box::pin(self.tool_call_succeeded(
                            &messages,
                            tool_output,
                            model_name,
                            success_criteria,
                            Some(iteration + 1),
                        ))
                        .await;
                    }
                }
                _ => {}
            }

            Ok(ToolCallResult::Failure(verification.reason))
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
            format!("
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

        let message = ChatCompletionRequestUserMessageContent::Text(format!("Find information about previosly executed task to help complete next steps\n\n{summary}"));

        Ok(vec![ChatCompletionRequestMessage::User(message.into())])
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

#[derive(Debug, Deserialize)]
pub struct VerificationResponse {
    pub status: VerificationStatus,
    pub reason: String,
    pub wait: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VerificationStatus {
    Succeeded,
    Failed,
    Inconclusive,
}
