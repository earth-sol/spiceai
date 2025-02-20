use std::{collections::HashMap, io::Write, sync::Arc};

use super::plan::{PhysicalPlan, PromptStep, Step, ToolStep};
use async_openai::types::{
    ChatCompletionRequestMessage, ChatCompletionRequestToolMessageArgs,
    ChatCompletionRequestToolMessageContent, CreateChatCompletionRequestArgs,
};
use llms::chat::Chat;
use tokio::sync::RwLock;
use tools::SpiceModelTool;

pub struct PhysicalJobExecutor {
    // INPUTS
    plan: PhysicalPlan,
    llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
    tools: HashMap<String, Arc<dyn SpiceModelTool>>,

    // JOB STATE
    execution_history: Vec<Vec<ChatCompletionRequestMessage>>,
}

impl PhysicalJobExecutor {
    #[must_use]
    pub fn new(
        plan: PhysicalPlan,
        llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
        tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    ) -> Self {
        Self {
            plan,
            llms,
            tools,
            execution_history: vec![],
        }
    }
}

impl PhysicalJobExecutor {
    pub async fn execute(&mut self) -> Result<(), anyhow::Error> {
        for task in &self.plan.tasks {
            tracing::info!("Executing task: {:?}", task.objective);
            let mut step_history = vec![];
            for step in &task.steps {
                let output = self
                    .execute_step(&step_history, step)
                    .await
                    .inspect_err(|err| {
                        trace_execution_progress(step, &err.to_string());
                    })?;
                tracing::info!("Step output: {output:?}");
                step_history.push(output);
            }

            self.execution_history.push(step_history);
        }

        Ok(())
    }

    async fn execute_step(
        &self,
        step_history: &[ChatCompletionRequestMessage],
        step: &Step,
    ) -> Result<ChatCompletionRequestMessage, anyhow::Error> {
        match step {
            Step::Tool(tool_step) => self.execute_tool(tool_step).await,
            Step::Prompt(prompt_step) => self.execute_prompt(step_history, prompt_step).await,
        }
    }

    async fn execute_prompt(
        &self,
        step_history: &[ChatCompletionRequestMessage],
        step: &PromptStep,
    ) -> Result<ChatCompletionRequestMessage, anyhow::Error> {
        let prompt = step.prompt.clone();
        let llms = &*self.llms.read().await;
        let model = llms
            .get(&step.target_model)
            .ok_or_else(|| anyhow::anyhow!("Model {} not found", step.target_model))?;

        let mut messages = step_history.to_vec();
        messages.push(ChatCompletionRequestMessage::User(prompt.into()));
        let req = CreateChatCompletionRequestArgs::default()
            .messages(messages)
            .model(step.target_model.clone())
            .build()
            .map_err(|e| {
                anyhow::anyhow!("Error building chat completion request: {}", e.to_string())
            })?;
        let response = model.chat_request(req).await?;

        let Some(message) = response.choices[0].message.content.clone() else {
            return Err(anyhow::anyhow!("No message content found"));
        };

        trace_execution_progress(&Step::Prompt(step.clone()), &message);

        let tool_message = ChatCompletionRequestToolMessageArgs::default()
            .content(ChatCompletionRequestToolMessageContent::Text(message))
            .build()
            .map_err(|e| anyhow::anyhow!("Error building tool message: {}", e.to_string()))?;
        Ok(ChatCompletionRequestMessage::Tool(tool_message))
    }

    async fn execute_tool(
        &self,
        step: &ToolStep,
    ) -> Result<ChatCompletionRequestMessage, anyhow::Error> {
        let tool = self
            .tools
            .get(&step.tool)
            .ok_or_else(|| anyhow::anyhow!("Tool {} not found", step.tool))?;

        let response = tool
            .call(step.body.as_str())
            .await
            .map_err(|e| anyhow::anyhow!("Error calling tool {}: {}", step.tool, e.to_string()))?;

        trace_execution_progress(&Step::Tool(step.clone()), &response.to_string());

        let tool_message_content =
            ChatCompletionRequestToolMessageContent::Text(response.to_string());

        let tool_message = ChatCompletionRequestToolMessageArgs::default()
            .content(tool_message_content)
            .build()
            .map_err(|e| anyhow::anyhow!("Error building tool message: {}", e.to_string()))?;
        Ok(ChatCompletionRequestMessage::Tool(tool_message))
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
                prompt.target_model, prompt.prompt
            ));
        }
        Step::Tool(tool_step) => {
            log_execution_update(&format!(
                "Task ID: {task_id}, calling tool: {}\n{}",
                tool_step.tool, tool_step.body
            ));
        }
    }
    log_execution_update(&format!(
        "Task ID: {task_id}, tool response:\n{:?}",
        output
    ));
}

fn log_execution_update(update_message: &str) {
    tracing::debug!(update_message);

    let log_path = "data/physical/physical_plan_execution.log";
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);

    if let Err(e) = options.open(log_path).and_then(|mut file| {
        writeln!(
            file,
            "{} {}",
            chrono::Local::now().format("%Y%m%d_%H%M%S"),
            update_message
        )
    }) {
        tracing::error!("Failed to write execution update to log: {e}");
    }
}