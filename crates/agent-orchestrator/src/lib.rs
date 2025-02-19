use std::{collections::HashMap, sync::Arc};

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoice, ChatChoiceStream, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestDeveloperMessage, ChatCompletionRequestDeveloperMessageContent,
        ChatCompletionRequestFunctionMessage, ChatCompletionRequestMessage,
        ChatCompletionRequestSystemMessage, ChatCompletionRequestToolMessage,
        ChatCompletionRequestUserMessage, ChatCompletionRequestUserMessageContent,
        ChatCompletionResponseMessage, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, CreateChatCompletionRequest,
        CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
        CreateChatCompletionStreamResponse, Role,
    },
};
use async_trait::async_trait;
use llms::chat::{nsql::SqlGeneration, Chat};
use logical_plan::LogicalPlan;
use tokio::sync::RwLock;

pub mod agentic_logical_planner;
mod logical_plan;

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
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let llm = self.llms.read().await;
        let Some(model) = llm.get("agentic_logical_planner") else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model {} not found.",
                self.orchestrator
            )));
        };

        let prompt = format!(
            "{}\n{}",
            self.objective,
            extract_request_content(&req)
                .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
        );
        let req = build_user_request(prompt)?;

        let response = model.chat_request(req).await?;
        let plan = LogicalPlan::from_chat_completion(&response)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        println!("Logical plan: {:?}", plan);

        Ok(response)
    }
}

fn build_user_request(prompt: String) -> Result<CreateChatCompletionRequest, OpenAIError> {
    CreateChatCompletionRequestArgs::default()
        .messages(vec![ChatCompletionRequestMessage::User(prompt.into())])
        .build()
}

fn extract_request_content(req: &CreateChatCompletionRequest) -> Result<String, anyhow::Error> {
    match &req.messages[0] {
        ChatCompletionRequestMessage::User(user_message) => match &user_message.content {
            ChatCompletionRequestUserMessageContent::Text(content) => Ok(content.clone()),
            ChatCompletionRequestUserMessageContent::Array(_) => {
                Err(anyhow::anyhow!("Invalid message content type"))
            }
        },
        _ => Err(anyhow::anyhow!("Invalid message type")),
    }
}

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
