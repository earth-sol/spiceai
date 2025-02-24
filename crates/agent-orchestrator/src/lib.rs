#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoice, ChatCompletionNamedToolChoice, ChatCompletionRequestMessage,
        ChatCompletionResponseMessage, ChatCompletionToolChoiceOption, ChatCompletionToolType,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
        FunctionName, Role,
    },
};
use async_trait::async_trait;
use llms::chat::{nsql::SqlGeneration, Chat};
use logical::plan::LogicalPlan;
use physical::{executor::PhysicalJobExecutor, plan::PhysicalPlan};
use research::{model::parse_response, Research};
use serde::Serialize;
use snafu::ResultExt;
use tokio::sync::RwLock;
use tools::SpiceModelTool;

pub mod logical;
pub mod physical;
pub mod pipeline;
pub mod research;

pub struct AgentChat {
    _objective: String,
    orchestrator: String,
    executor: String,
    llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
    tools: HashMap<String, Arc<dyn SpiceModelTool>>,
}

impl AgentChat {
    pub fn new(
        objective: String,
        orchestrator: String,
        executor: String,
        llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
        tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    ) -> Self {
        Self {
            _objective: objective,
            orchestrator,
            executor,
            llms,
            tools,
        }
    }

    #[allow(clippy::unused_async)]
    async fn generate_research(
        &self,
        research_model: &dyn Chat,
        prompt: String,
    ) -> Result<Research, OpenAIError> {
        let mut initial_request = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestMessage::User(
                prompt.clone().into(),
            )])
            .build()?;

        initial_request.tool_choice = Some(ChatCompletionToolChoiceOption::Named(
            ChatCompletionNamedToolChoice {
                r#type: ChatCompletionToolType::Function,
                function: FunctionName {
                    name: "document_similarity".to_string(),
                },
            },
        ));

        let response = research_model.chat_request(initial_request).await?;
        let artifacts = parse_response(&response)?;

        Ok(Research { prompt, artifacts })
    }

    async fn generate_logical_plan(
        &self,
        logical_planner_model: &dyn Chat,
        research: Research,
    ) -> Result<LogicalPlan, OpenAIError> {
        let artifacts_prompt = research
            .artifacts
            .iter()
            .map(|artifact| format!("{artifact}"))
            .collect::<Vec<String>>()
            .join("\n\n");
        let initial_request = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestMessage::User(
                format!("{}\n\n{}", artifacts_prompt, research.prompt).into(),
            )])
            .build()?;

        let response = logical_planner_model
            .chat_request(initial_request.clone())
            .await?;
        let plan = match LogicalPlan::from_chat_completion(&response) {
            Ok(plan) => plan,
            Err(logical::plan::ConversionError::JsonSchema(e)) => {
                tracing::warn!(
                    "Logical plan created did not satisfy JSONSchema format. Reattempting.\n   Initial Error: {e}"
                );
                let response = logical_planner_model.chat_request(initial_request).await?;
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

        Ok(plan)
    }

    async fn generate_physical_plan(
        &self,
        plan: &LogicalPlan,
        physical_tool_planner_model: &dyn Chat,
        physical_prompt_planner_model: &dyn Chat,
    ) -> Result<PhysicalPlan, OpenAIError> {
        let physical_plan = PhysicalPlan::plan(
            plan,
            physical_tool_planner_model,
            physical_prompt_planner_model,
            self.executor.clone(),
        )
        .await?;

        Ok(physical_plan)
    }
}

fn write_artifact<T: ?Sized + Serialize>(
    base_name: &str,
    id: &str,
    artifact: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let artifact_json =
        serde_json::to_string_pretty(artifact).expect("Failed to serialize logical plan");

    tracing::debug!("{base_name}: {artifact_json}");

    if let Some(base_dir) = PathBuf::from(format!("data/{base_name}")).parent() {
        if !base_dir.exists() {
            tracing::debug!("Creating directory(s) {base_dir:?}");
            std::fs::create_dir_all(base_dir).boxed()?;
        }
    }

    std::fs::write(format!("data/{base_name}_{id}.json"), &artifact_json).boxed()?;
    std::fs::write(format!("data/{base_name}_latest.json"), &artifact_json).boxed()?;
    Ok(())
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
        let id = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
        let llm = self.llms.read().await;
        let Some(agentic_researcher_model) = llm.get("agentic_researcher") else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model {} not found.",
                self.orchestrator
            )));
        };
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

        let (mut pipeline, advance_mode) = pipeline::AgentPipeline::try_new(&req)
            .map_err(|e| OpenAIError::InvalidArgument(format!("Error parsing request: {e}")))?;

        loop {
            match pipeline {
                pipeline::AgentPipeline::Research { prompt } => {
                    let research = self
                        .generate_research(agentic_researcher_model.as_ref(), prompt)
                        .await?;

                    write_artifact("research/research_artifacts", id.as_str(), &research)
                        .expect("Failed to save research artifacts");

                    if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                        return get_output_message_from_struct(id.as_str(), research);
                    }
                    pipeline = pipeline::AgentPipeline::LogicalPlan(research);
                }
                pipeline::AgentPipeline::LogicalPlan(research) => {
                    let prompt = research.prompt.clone();
                    let logical_plan = match self
                        .generate_logical_plan(logical_planner_model.as_ref(), research)
                        .await
                    {
                        Ok(p) => p,
                        Err(e) => {
                            match e {
                                OpenAIError::ApiError(_) => {
                                    tracing::warn!(
                                        "Logical plan generation failed with error {e}. Regenerating research plan."
                                    );
                                    // Regenerate research plan on logical plan generation failure
                                    pipeline = pipeline::AgentPipeline::Research { prompt: prompt };
                                    continue;
                                }
                                _ => return Err(e),
                            }
                        }
                    };

                    write_artifact("logical/logical_plan", id.as_str(), &logical_plan)
                        .expect("Failed to save logical plan");

                    if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                        return get_output_message_from_struct(id.as_str(), logical_plan);
                    }
                    pipeline = pipeline::AgentPipeline::PhysicalPlan(logical_plan);
                }
                pipeline::AgentPipeline::PhysicalPlan(logical_plan) => {
                    let physical_plan = self
                        .generate_physical_plan(
                            &logical_plan,
                            physical_tool_planner_model.as_ref(),
                            physical_prompt_planner_model.as_ref(),
                        )
                        .await?;
                    write_artifact("physical/physical_plan", id.as_str(), &physical_plan)
                        .expect("Failed to save physical plan");

                    if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                        return get_output_message_from_struct(id.as_str(), physical_plan);
                    }
                    pipeline = pipeline::AgentPipeline::Execution(physical_plan);
                }
                pipeline::AgentPipeline::Execution(physical_plan) => {
                    let mut executor = PhysicalJobExecutor::new(
                        physical_plan,
                        Arc::clone(&self.llms),
                        self.tools.clone(),
                        "verifier-gpt-4o".to_string(),
                    );
                    let output = executor.execute().await.map_err(|e| {
                        OpenAIError::InvalidArgument(format!("Error executing physical plan: {e}"))
                    })?;
                    pipeline = pipeline::AgentPipeline::Output(output);
                }
                pipeline::AgentPipeline::Output(output) => {
                    return Ok(get_output_message(id.as_str(), output));
                }
            }
        }
    }
}

fn get_output_message_from_struct<T: Serialize>(
    id: &str,
    output: T,
) -> Result<CreateChatCompletionResponse, OpenAIError> {
    let output_json = serde_json::to_string(&output)
        .map_err(|e| OpenAIError::InvalidArgument(format!("Failed to serialize output: {e}")))?;
    Ok(get_output_message(id, output_json))
}

#[allow(deprecated)]
fn get_output_message(id: &str, output: String) -> CreateChatCompletionResponse {
    let message = ChatCompletionResponseMessage {
        content: Some(output),
        tool_calls: None,
        role: Role::Assistant,
        audio: None,
        function_call: None,
        refusal: None,
    };
    CreateChatCompletionResponse {
        id: id.to_string(),
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

#[allow(dead_code)]
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
