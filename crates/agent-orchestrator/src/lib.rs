#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use std::{collections::HashMap, sync::Arc};

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
use research::{model::parse_response, Artifact, Research};
use serde::Serialize;
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

        let artifacts_json =
            serde_json::to_string_pretty(&artifacts).expect("Failed to serialize logical plan");
        tracing::info!("Artifacts plan: {artifacts_json}");

        // trace twice: once to dedicated log and also to the baseline
        let trace_file_names = vec![
            format!(
                "data/research/artifacts_{}.json",
                chrono::Local::now().format("%Y%m%d_%H%M%S")
            ),
            "data/research/artifacts.json".to_string(),
        ];

        for file_name in trace_file_names {
            if let Err(e) = std::fs::write(file_name, &artifacts_json) {
                tracing::error!("Failed to write research artifacts: {e}");
            }
        }

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
        let mut initial_request = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestMessage::User(
                format!("{}\n\n{}", artifacts_prompt, research.prompt).into(),
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

        let logical_plan_json =
            serde_json::to_string_pretty(&plan).expect("Failed to serialize logical plan");
        tracing::info!("Logical plan: {logical_plan_json}");

        // trace twice: once to dedicated log and also to the baseline
        let trace_file_names = vec![
            format!(
                "data/logical/logical_plan_{}.json",
                chrono::Local::now().format("%Y%m%d_%H%M%S")
            ),
            "data/logical/logical_plan.json".to_string(),
        ];

        for file_name in trace_file_names {
            if let Err(e) = std::fs::write(file_name, &logical_plan_json) {
                tracing::error!("Failed to write logical plan: {e}");
            }
        }

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

        let physical_plan_json = serde_json::to_string_pretty(&physical_plan)
            .expect("Failed to serialize physical plan");
        tracing::info!("Physical plan: {physical_plan_json}");

        // trace twice: once to dedicated log and also to the baseline
        let trace_file_names = vec![
            format!(
                "data/physical/physical_plan_{}.json",
                chrono::Local::now().format("%Y%m%d_%H%M%S")
            ),
            "data/physical/physical_plan.json".to_string(),
        ];

        for file_name in trace_file_names {
            if let Err(e) = std::fs::write(file_name, &physical_plan_json) {
                tracing::error!("Failed to write physical plan: {e}");
            }
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
                    if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                        return get_output_message_from_struct(research);
                    }
                    pipeline = pipeline::AgentPipeline::LogicalPlan(research);
                }
                pipeline::AgentPipeline::LogicalPlan(research) => {
                    let logical_plan = self
                        .generate_logical_plan(logical_planner_model.as_ref(), research)
                        .await?;
                    if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                        return get_output_message_from_struct(logical_plan);
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
                    if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                        return get_output_message_from_struct(physical_plan);
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
                    return Ok(get_output_message(output));
                }
            }
        }
    }
}

fn get_output_message_from_struct<T: Serialize>(
    output: T,
) -> Result<CreateChatCompletionResponse, OpenAIError> {
    let output_json = serde_json::to_string(&output)
        .map_err(|e| OpenAIError::InvalidArgument(format!("Failed to serialize output: {e}")))?;
    Ok(get_output_message(output_json))
}

#[allow(deprecated)]
fn get_output_message(output: String) -> CreateChatCompletionResponse {
    let message = ChatCompletionResponseMessage {
        content: Some(output),
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
