/*
Copyright 2024 The Spice.ai OSS Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
     https://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::chat::nsql::structured_output::StructuredOutputSqlGeneration;
use crate::chat::nsql::{json::JsonSchemaSqlGeneration, SqlGeneration};
use crate::chat::Chat;
use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::{
    ChatChoice, ChatCompletionRequestMessage, ChatCompletionResponseChoice,
    ChatCompletionResponseMessage, ChatCompletionResponseStream, ChatCompletionUsage,
    CompletionUsage, CreateChatCompletionRequest, CreateChatCompletionResponse, FinishReason, Role,
};
use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_bedrockruntime::config::http::HttpResponse;
use aws_sdk_bedrockruntime::error::SdkError;
use aws_sdk_bedrockruntime::operation::converse::builders::ConverseFluentBuilder;
use aws_sdk_bedrockruntime::operation::converse::{ConverseError, ConverseOutput};

use aws_sdk_bedrockruntime::types::{
    ContentBlock, ConversationRole, ConverseOutput as ConverseOutputType, StopReason, TokenUsage,
};
use aws_sdk_bedrockruntime::Client;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use uuid::Uuid;

pub struct Bedrock {
    model_id: String,
    client: Client,
}

impl Bedrock {
    pub async fn from_config(model_id: &str, region: String) -> Self {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region))
            .load()
            .await;

        let client = Client::new(&sdk_config);

        Self {
            model_id: model_id.to_string(),
            client,
        }
    }
    fn construct_request(
        &self,
        mut bldr: &ConverseFluentBuilder,
        req: &CreateChatCompletionRequest,
    ) -> Result<(), OpenAIError> {
        Ok(())
    }

    fn convert_converse_error(err: SdkError<ConverseError, HttpResponse>) -> OpenAIError {
        match err.into_service_error() {
            ConverseError::ResourceNotFoundException(e) => {
                OpenAIError::InvalidArgument(e.to_string())
            }
            ConverseError::ValidationException(e) => OpenAIError::InvalidArgument(e.to_string()),
            ConverseError::ModelTimeoutException(e) => OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: Some("amazon_bedrock".to_string()),
                param: None,
                code: Some("ModelTimeoutException".to_string()),
            }),
            ConverseError::AccessDeniedException(e) => OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: Some("amazon_bedrock".to_string()),
                param: None,
                code: Some("AccessDeniedException".to_string()),
            }),
            ConverseError::ThrottlingException(e) => OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: Some("amazon_bedrock".to_string()),
                param: None,
                code: Some("ThrottlingException".to_string()),
            }),
            ConverseError::ServiceUnavailableException(e) => OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: Some("amazon_bedrock".to_string()),
                param: None,
                code: Some("ServiceUnavailableException".to_string()),
            }),
            ConverseError::InternalServerException(e) => OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: Some("amazon_bedrock".to_string()),
                param: None,
                code: Some("InternalServerException".to_string()),
            }),
            ConverseError::ModelNotReadyException(e) => OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: Some("amazon_bedrock".to_string()),
                param: None,
                code: Some("ModelNotReadyException".to_string()),
            }),
            ConverseError::ModelErrorException(e) => OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: Some("amazon_bedrock".to_string()),
                param: None,
                code: Some("ModelErrorException".to_string()),
            }),
            e => OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: Some("amazon_bedrock".to_string()),
                param: None,
                code: Some("GenericError".to_string()),
            }),
        }
    }

    fn to_chat_choice(
        role: &ConversationRole,
        block: &ContentBlock,
        stop_reason: &StopReason,
        i: u32,
    ) -> ChatChoice {
        match block {
            ContentBlock::Text(text) => ChatChoice {
                message: ChatCompletionResponseMessage {
                    content: Some(text.clone()),
                    role: Self::map_role(role).unwrap_or_default(),
                    refusal: None,
                    tool_calls: None,
                    function_call: None,
                },
                index: i,
                finish_reason: map_stop_reason(stop_reason),
                logprobs: None,
            },
            ContentBlock::Image(image) => ChatChoice {
                role: role.to_string(),
                index: i,
                message: ChatCompletionResponseChoice::Image(image.clone()),
            },
            ContentBlock::Video(video) => ChatChoice {
                role: role.to_string(),
                index: i,
                message: ChatCompletionResponseChoice::Video(video.clone()),
            },
            ContentBlock::Audio(audio) => ChatChoice {
                role: role.to_string(),
                index: i,
                message: ChatCompletionResponseChoice::Audio(audio.clone()),
            },
            ContentBlock::Custom(custom) => ChatChoice {
                role: role.to_string(),
                index: i,
                message: ChatCompletionResponseChoice::Custom(custom.clone()),
            },
        }
    }

    fn map_role(role: &ConversationRole) -> Option<Role> {
        match role {
            ConversationRole::User => Some(Role::User),
            ConversationRole::Assistant => Some(Role::Assistant),
            _ => None,
        }
    }

    fn convert_converse_output(
        model_name: String,
        resp: ConverseOutput,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let usage = resp.usage().map(map_usage);
        let stop_reason = resp.stop_reason();
        let chat_choices = match resp.output {
            Some(ConverseOutputType::Message(ref msg)) => {
                let role = msg.role();
                if let Some(content) = msg.content().first() {
                    let choice = Self::to_chat_choice(role, content, stop_reason, 0);
                    vec![choice]
                } else {
                    vec![]
                }
            }
            _ => vec![],
        };

        let created = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            .as_secs() as u32;

        Ok(CreateChatCompletionResponse {
            id: Uuid::new_v4().to_string(),
            created,
            model: model_name,
            service_tier: None,
            system_fingerprint: None,
            object: "chat.completion".to_string(),
            choices: chat_choices,
            usage,
        })
    }
}
fn map_stop_reason(reason: &StopReason) -> Option<FinishReason> {
    match reason {
        StopReason::EndTurn | StopReason::StopSequence => Some(FinishReason::Stop),
        StopReason::MaxTokens => Some(FinishReason::Length),
        StopReason::ToolUse => Some(FinishReason::ToolCalls),
        StopReason::GuardrailIntervened => Some(FinishReason::ContentFilter),
        _ => Some(FinishReason::Stop), // default fallback
    }
}

// Converts AWS `TokenUsage` to OpenAI `CompletionUsage`
fn map_usage(usage: &TokenUsage) -> CompletionUsage {
    CompletionUsage {
        prompt_tokens: usage.input_tokens as u32,
        completion_tokens: usage.output_tokens as u32,
        total_tokens: usage.total_tokens as u32,
        prompt_tokens_details: None,
        completion_tokens_details: None,
    }
}

#[async_trait]
impl Chat for Bedrock {
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let model_name = req.model.clone();
        let req_bldr = self.client.converse();
        self.construct_request(&req_bldr, &req)?;
        let resp = req_bldr
            .send()
            .await
            .map_err(Self::convert_converse_error)?;

        Self::convert_converse_output(model_name, resp)
    }
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
}
