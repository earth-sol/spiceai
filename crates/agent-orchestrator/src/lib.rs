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
use physical::plan::{PhysicalPlan, Step};
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

        let response = logical_planner_model.chat_request(req).await?;
        let plan = LogicalPlan::from_chat_completion(&response)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        println!(
            "Logical plan: {}",
            serde_json::to_string_pretty(&plan).expect("Failed to serialize logical plan")
        );

        let physical_plan = PhysicalPlan::plan(
            &plan,
            physical_tool_planner_model,
            physical_prompt_planner_model,
            model_names,
        )
        .await?;

        println!(
            "Physical plan: {}",
            serde_json::to_string_pretty(&physical_plan)
                .expect("Failed to serialize physical plan")
        );

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

// pub struct PhysicalJobOrchestrator {
//     // INPUTS
//     plan: PhysicalPlan,

//     // JOB STATE
//     execution_history: Vec<Vec<ChatCompletionRequestMessage>>,
// }

// impl PhysicalJobOrchestrator {
//     #[must_use]
//     pub fn new(plan: PhysicalPlan) -> Self {
//         Self {
//             plan,
//             execution_history: vec![],
//         }
//     }
// }

// impl PhysicalJobOrchestrator {
//     pub async fn execute(&mut self) {}

//     fn execute_step(
//         &mut self,
//         step_history: &[ChatCompletionRequestMessage],
//         step: &Step,
//     ) -> Result<ChatCompletionRequestMessage, anyhow::Error> {
//         todo!()
//     }
// }
