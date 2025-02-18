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
        // let content = match &req.messages[0] {
        //     ChatCompletionRequestMessage::User(user_message) => match &user_message.content {
        //         ChatCompletionRequestUserMessageContent::Text(text) => text,
        //         _ => todo!(),
        //     },
        //     _ => todo!(),
        // };

        // let initial_prompt = format!("Objective: {}\n\nRequest: {}", self.objective, content);

        // let llm = self.llms.read().await;
        // let model = llm.get(self.orchestrator.as_str()).unwrap();

        //let req = CreateChatCompletionRequest {messages:vec![ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage{content:ChatCompletionRequestSystemMessageContent::Text(initial_prompt),},)], model: todo!(), store: todo!(), reasoning_effort: todo!(), metadata: todo!(), frequency_penalty: todo!(), logit_bias: todo!(), logprobs: todo!(), top_logprobs: todo!(), max_tokens: todo!(), max_completion_tokens: todo!(), n: todo!(), modalities: todo!(), prediction: todo!(), audio: todo!(), presence_penalty: todo!(), response_format: todo!(), seed: todo!(), service_tier: todo!(), stop: todo!(), stream: todo!(), stream_options: todo!(), temperature: todo!(), top_p: todo!(), tools: todo!(), tool_choice: todo!(), parallel_tool_calls: todo!(), user: todo!(), function_call: todo!(), functions: todo!() }};

        let resp = CreateChatCompletionResponse {
            id: String::new(),
            choices: vec![ChatChoice {
                index: 0,
                message: ChatCompletionResponseMessage {
                    role: Role::Assistant,
                    content: Some("Hello, world!".to_string()),
                    refusal: None,
                    tool_calls: None,
                    function_call: None,
                    audio: None,
                },
                finish_reason: None,
                logprobs: None,
            }],
            created: 0,
            model: String::new(),
            service_tier: None,
            system_fingerprint: None,
            object: String::new(),
            usage: None,
        };

        Ok(resp)

        // let response = model.chat_request(req).await?;
        // Ok(response)
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
