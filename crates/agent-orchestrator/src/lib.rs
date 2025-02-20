#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use std::{collections::HashMap, sync::Arc};

use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, CreateChatCompletionRequest, CreateChatCompletionResponse,
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
}

#[async_trait]
impl Chat for AgentChat {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        todo!()
    }

    async fn chat_request(
        &self,
        mut req: CreateChatCompletionRequest,
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

        add_system_message(&mut req, self.objective.clone());

        let response = logical_planner_model.chat_request(req.clone()).await?;

        // Attempt to convert the chat response to a logical plan. If the JSONSchema format is not satisfied, reattempt once.
        let plan = match LogicalPlan::from_chat_completion(&response) {
            Ok(plan) => plan,
            Err(logical::plan::ConversionError::JsonSchema(e)) => {
                tracing::warn!(
                    "Logical plan created did not satisfy JSONSchema format. Reattempting.\n   Initial Error: {e}"
                );
                let response = logical_planner_model.chat_request(req).await?;
                LogicalPlan::from_chat_completion(&response)
                    .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            }
            Err(logical::plan::ConversionError::SerdeJson(e)) => {
                return Err(OpenAIError::InvalidArgument(format!(
                    "Failed to convert chat response to logical plan: {e}"
                )))
            }
            Err(logical::plan::ConversionError::SerdeYaml(e)) => {
                return Err(OpenAIError::InvalidArgument(format!(
                    "Failed to convert chat response to logical plan: {e}"
                )))
            }
        };

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

        let physical_plan = PhysicalPlan::plan(
            &plan,
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

        let mut executor =
            PhysicalJobExecutor::new(physical_plan, Arc::clone(&self.llms), self.tools.clone());
        executor.execute().await.map_err(|e| {
            OpenAIError::InvalidArgument(format!("Error executing physical plan: {e}"))
        })?;

        Ok(response)
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
