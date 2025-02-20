#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use std::{collections::HashMap, sync::Arc};

use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoice, ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageContent,
        ChatCompletionRequestUserMessageContent, ChatCompletionResponseMessage,
        CreateChatCompletionRequest, CreateChatCompletionResponse, Role,
    },
};
use async_trait::async_trait;
use llms::chat::{nsql::SqlGeneration, Chat};
use logical::plan::LogicalPlan;
use physical::{executor::PhysicalJobExecutor, plan::PhysicalPlan};
use tokio::sync::RwLock;
use tools::SpiceModelTool;

pub mod logical;
pub mod physical;

pub struct AgentChat {
    objective: String,
    orchestrator: String,
    llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
    tools: HashMap<String, Arc<dyn SpiceModelTool>>,
}

impl AgentChat {
    pub fn new(
        objective: String,
        orchestrator: String,
        llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
        tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    ) -> Self {
        Self {
            objective,
            orchestrator,
            llms,
            tools,
        }
    }

    fn parse_request(
        req: &CreateChatCompletionRequest,
    ) -> Result<(Option<LogicalPlan>, Option<PhysicalPlan>), OpenAIError> {
        let mut content = String::new();
        tracing::debug!("Request: {req:?}");
        let Some(message) = req.messages.last() else {
            return Ok((None, None));
        };
        match message {
            ChatCompletionRequestMessage::User(user_message) => {
                if let ChatCompletionRequestUserMessageContent::Text(text) = &user_message.content {
                    content.push_str(text);
                }
            }
            // For some reason, we are getting a system message here from `spice chat`
            ChatCompletionRequestMessage::System(system_message) => {
                if let ChatCompletionRequestSystemMessageContent::Text(text) =
                    &system_message.content
                {
                    content.push_str(text);
                }
            }
            _ => return Ok((None, None)),
        }

        if content.starts_with("logical_plan:") {
            let logical_plan_file = content
                .split(':')
                .nth(1)
                .expect("Logical plan file not found");
            let logical_plan_str = std::fs::read_to_string(logical_plan_file).map_err(|e| {
                OpenAIError::InvalidArgument(format!("Error reading logical plan: {e}"))
            })?;
            tracing::info!("Logical plan from file: {logical_plan_str}");
            let logical_plan = LogicalPlan::new(&logical_plan_str).map_err(|e| {
                OpenAIError::InvalidArgument(format!("Error parsing logical plan: {e}"))
            })?;
            return Ok((Some(logical_plan), None));
        }
        if content.starts_with("physical_plan:") {
            let physical_plan_file = content
                .split(':')
                .nth(1)
                .expect("Physical plan file not found");
            let physical_plan_str = std::fs::read_to_string(physical_plan_file).map_err(|e| {
                OpenAIError::InvalidArgument(format!("Error reading physical plan: {e}"))
            })?;
            tracing::info!("Physical plan from file: {physical_plan_str}");
            let physical_plan = PhysicalPlan::new(&physical_plan_str).map_err(|e| {
                OpenAIError::InvalidArgument(format!("Error parsing physical plan: {e}"))
            })?;
            return Ok((None, Some(physical_plan)));
        }

        Ok((None, None))
    }

    async fn generate_logical_plan(
        &self,
        logical_planner_model: &dyn Chat,
        mut initial_request: CreateChatCompletionRequest,
    ) -> Result<LogicalPlan, OpenAIError> {
        add_system_message(&mut initial_request, self.objective.clone());
        let response = logical_planner_model.chat_request(initial_request).await?;
        let plan = LogicalPlan::from_chat_completion(&response)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let logical_plan_json =
            serde_json::to_string_pretty(&plan).expect("Failed to serialize logical plan");
        tracing::info!("Logical plan: {logical_plan_json}");
        if let Err(e) = std::fs::write(
            format!(
                "data/logical/logical_plan_{}.json",
                chrono::Local::now().format("%Y%m%d_%H%M%S")
            ),
            logical_plan_json,
        ) {
            tracing::error!("Failed to write logical plan: {e}");
        }

        Ok(plan)
    }

    async fn generate_physical_plan(
        &self,
        plan: &LogicalPlan,
        physical_tool_planner_model: &dyn Chat,
        physical_prompt_planner_model: &dyn Chat,
        model_names: Vec<String>,
    ) -> Result<PhysicalPlan, OpenAIError> {
        let physical_plan = PhysicalPlan::plan(
            plan,
            physical_tool_planner_model,
            physical_prompt_planner_model,
            model_names,
        )
        .await?;

        let physical_plan_json = serde_json::to_string_pretty(&physical_plan)
            .expect("Failed to serialize physical plan");
        tracing::info!("Physical plan: {physical_plan_json}");
        if let Err(e) = std::fs::write(
            format!(
                "data/physical/physical_plan_{}.json",
                chrono::Local::now().format("%Y%m%d_%H%M%S")
            ),
            physical_plan_json,
        ) {
            tracing::error!("Failed to write physical plan: {e}");
        }

        Ok(physical_plan)
    }
}

#[async_trait]
impl Chat for AgentChat {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        todo!()
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let llm = self.llms.read().await;
        let Some(logical_planner_model) = llm.get("agentic_logical_planner") else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model {} not found.",
                self.orchestrator
            )));
        };
        let Some(physical_tool_planner_model) = llm.get("agentic_physical_tool_planner") else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model {} not found.",
                self.orchestrator
            )));
        };
        let Some(physical_prompt_planner_model) = llm.get("agentic_physical_prompt_planner") else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model {} not found.",
                self.orchestrator
            )));
        };

        let model_names = llm.keys().map(String::clone).collect::<Vec<String>>();

        let (mut logical_plan, mut physical_plan) = Self::parse_request(&req)
            .map_err(|e| OpenAIError::InvalidArgument(format!("Error parsing request: {e}")))?;

        if logical_plan.is_none() && physical_plan.is_none() {
            logical_plan = Some(
                self.generate_logical_plan(logical_planner_model.as_ref(), req)
                    .await?,
            );
        }

        if let Some(plan) = logical_plan {
            physical_plan = Some(
                self.generate_physical_plan(
                    &plan,
                    physical_tool_planner_model.as_ref(),
                    physical_prompt_planner_model.as_ref(),
                    model_names,
                )
                .await?,
            );
        }

        let physical_plan = physical_plan.expect("Physical plan is required");

        let mut executor =
            PhysicalJobExecutor::new(physical_plan, Arc::clone(&self.llms), self.tools.clone());
        executor.execute().await.map_err(|e| {
            OpenAIError::InvalidArgument(format!("Error executing physical plan: {e}"))
        })?;

        Ok(get_done_message())
    }
}

#[allow(deprecated)]
fn get_done_message() -> CreateChatCompletionResponse {
    let message = ChatCompletionResponseMessage {
        content: Some("Done!".into()),
        tool_calls: None,
        role: Role::Assistant,
        audio: None,
        function_call: None,
        refusal: None,
    };
    CreateChatCompletionResponse {
        id: String::new(),
        object: String::new(),
        created: 0,
        model: String::new(),
        choices: vec![ChatChoice {
            message,
            index: 0,
            finish_reason: None,
            logprobs: None,
        }],
        service_tier: None,
        system_fingerprint: None,
        usage: None,
    }
}

fn add_system_message(req: &mut CreateChatCompletionRequest, message: String) {
    req.messages
        .insert(0, ChatCompletionRequestMessage::System(message.into()));
}

// fn build_user_request(prompt: String) -> Result<CreateChatCompletionRequest, OpenAIError> {
//     CreateChatCompletionRequestArgs::default()
//         .messages(vec![ChatCompletionRequestMessage::User(prompt.into())])
//         .build()
// }

// fn extract_request_content(req: &CreateChatCompletionRequest) -> Result<String, anyhow::Error> {
//     let mut content = String::new();

//     for message in &req.messages {
//         match message {
//             ChatCompletionRequestMessage::User(user_message) => match &user_message.content {
//                 ChatCompletionRequestUserMessageContent::Text(user_message) => {
//                     content.push_str(user_message);
//                 }
//                 ChatCompletionRequestUserMessageContent::Array(_) => {
//                     return Err(anyhow::anyhow!("Invalid message content type"));
//                 }
//             },
//             ChatCompletionRequestMessage::System(system_message) => match &system_message.content {
//                 ChatCompletionRequestSystemMessageContent::Text(system_message) => {
//                     content.push_str(system_message);
//                 }
//                 ChatCompletionRequestSystemMessageContent::Array(_) => {
//                     return Err(anyhow::anyhow!("Invalid message content type"));
//                 }
//             },
//             _ => return Err(anyhow::anyhow!("Invalid message type")),
//         }
//     }

//     Ok(content)
// }
