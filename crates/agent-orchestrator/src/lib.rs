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
use physical::plan::PhysicalPlan;
use tokio::sync::RwLock;

pub mod logical;
pub mod physical;

pub struct AgentChat {
    objective: String,
    orchestrator: String,
    llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
}

impl AgentChat {
    pub fn new(
        objective: String,
        orchestrator: String,
        llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
    ) -> Self {
        Self {
            objective,
            orchestrator,
            llms,
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

        add_system_message(&mut req, self.objective.clone());

        let response = logical_planner_model.chat_request(req).await?;
        let plan = LogicalPlan::from_chat_completion(&response)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        println!(
            "Logical plan: {}",
            serde_json::to_string_pretty(&plan).expect("Failed to serialize logical plan")
        );

        // Now build the initial physical plan
        // let logical_plan_chat_request = plan.to_chat_request()?;
        // let response = physical_planner_model
        //     .chat_request(logical_plan_chat_request)
        //     .await?;

        let physical_plan = PhysicalPlan::plan(
            &plan,
            physical_tool_planner_model,
            physical_prompt_planner_model,
        )
        .await?;

        // let physical_plan = PhysicalPlan::from_chat_completion(&response)
        //     .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

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

// pub struct AgentJobOrchestrator {
//     // INPUTS
//     job: Job,
//     request: String,

//     // JOB STATE
//     plan: LogicalPlan,
//     execution_history: Vec<StepResult>,
// }

// impl AgentJobOrchestrator {
//     pub fn new(job: Job, request: String) -> Self {
//         Self { job, request }
//     }
// }

// impl AgentJobOrchestrator {
//     pub async fn start(&mut self) {
//         self.plan().await;
//         self.execute().await;
//     }

//     async fn plan(&mut self) {}

//     async fn execute(&mut self) {}
// }
