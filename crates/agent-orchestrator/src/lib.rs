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
        CreateChatCompletionResponse, CreateChatCompletionStreamResponse, Role,
    },
};
use async_trait::async_trait;
use llms::chat::{nsql::SqlGeneration, Chat};
use logical_plan::LogicalPlan;
use tokio::sync::RwLock;

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

        let response = model.chat_request(req).await?;

        Ok(response)
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
