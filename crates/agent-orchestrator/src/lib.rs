#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoice, ChatCompletionMessageToolCall, ChatCompletionNamedToolChoice,
        ChatCompletionRequestMessage, ChatCompletionResponseMessage, ChatCompletionResponseStream,
        ChatCompletionToolChoiceOption, ChatCompletionToolType, CreateChatCompletionRequest,
        CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
        CreateChatCompletionStreamResponse, FunctionCall, FunctionName,
    },
};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use llms::chat::{nsql::SqlGeneration, Chat};
use logical::{logical_plan_complete_summary, plan::LogicalPlan};
use physical::{executor::PhysicalJobExecutor, plan::PhysicalPlan};
use pipeline::create_working_stream_payload;
use progress::Progress;
use research::{
    model::{parse_response, research_complete_msg},
    Research,
};
use serde::{de::DeserializeOwned, Serialize};
use snafu::ResultExt;
use std::future::Future;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tools::SpiceModelTool;
use tracing::{Instrument, Span};
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};
pub mod logical;
pub mod physical;
pub mod pipeline;
mod progress;
pub mod research;
mod score;

#[derive(Clone)]
#[allow(dead_code)]
pub struct AgentModels {
    _orchestrator: String,
    executor: String,
    logical_planner: String,
    physical_tool_planner: String,
    physical_prompt_planner: String,
    researcher: String,
    verifier: String,

    /// Optional models to check that, for each stage, the output satisfies an evaluation model's score.
    research_artifact_eval: Option<EvalModelConfig>,
    logical_plan_eval: Option<EvalModelConfig>,
    physical_plan_eval: Option<EvalModelConfig>,
}

/// Encapsulate eval models and score configurations for checking each stage.
#[derive(Clone)]
pub struct EvalModelConfig {
    pub model_name: String,
    pub threshold: Option<f32>,
}

impl AgentModels {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        orchestrator: String,
        executor: String,
        logical_planner: String,
        physical_tool_planner: String,
        physical_prompt_planner: String,
        researcher: String,
        verifier: String,
        research_eval_model: Option<String>,
        logical_plan_eval_model: Option<String>,
        physical_plan_eval_model: Option<String>,
    ) -> Self {
        Self {
            _orchestrator: orchestrator,
            executor,
            logical_planner,
            physical_tool_planner,
            physical_prompt_planner,
            researcher,
            verifier,
            research_artifact_eval: research_eval_model.map(|model_name| EvalModelConfig {
                model_name,
                threshold: None,
            }),
            logical_plan_eval: logical_plan_eval_model.map(|model_name| EvalModelConfig {
                model_name,
                threshold: None,
            }),
            physical_plan_eval: physical_plan_eval_model.map(|model_name| EvalModelConfig {
                model_name,
                threshold: None,
            }),
        }
    }
}

/// Helper macro to send an error to the [`mpsc::Sender`] if the expression is an error.
macro_rules! try_send_err {
    ($expr:expr, $tx:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => {
                let _ = $tx.send(Err(err)).await;
                return;
            }
        }
    };
}

#[derive(Debug)]
pub enum ConversionError {
    SerdeJson(serde_json::Error),
    SerdeYaml(serde_yaml::Error),
    JsonSchema(jsonschema::ValidationError<'static>),
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConversionError::SerdeJson(e) => write!(f, "{e}"),
            ConversionError::JsonSchema(e) => write!(f, "{e}"),
            ConversionError::SerdeYaml(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Clone)]
pub struct AgentChat {
    _objective: String,
    models: AgentModels,
    llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
    tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    max_retry: usize,
}

const DEFAULT_MAX_RETRY: usize = 1;
// This limitation is about single message in chat completion and not related to the model context window.
// TODO: Split one message into multiple messages if it exceeds the limit. https://github.com/spicehq/timmy/issues/47
const MAX_CONTENT_LENGTH: usize = 1_048_576;

impl AgentChat {
    pub fn new(
        objective: String,
        models: AgentModels,
        llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
        tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    ) -> Self {
        Self {
            _objective: objective,
            models,
            llms,
            tools,
            max_retry: DEFAULT_MAX_RETRY,
        }
    }

    #[must_use]
    pub fn with_max_retry(mut self, max_retry: usize) -> Self {
        self.max_retry = max_retry;
        self
    }

    async fn retry_stage<F, Fut, T>(
        &self,
        retry_message: String,
        progress: &Progress,
        operation: F,
    ) -> Result<T, OpenAIError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, OpenAIError>>,
    {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(Some(self.max_retry))
            .max_duration(Some(Duration::from_secs(60)))
            .build();

        retry(retry_strategy, || async {
            match operation().await {
                Ok(result) => Ok(result),
                Err(e) => {
                    if should_retry_on_error(&e) {
                        tracing::warn!("Error: {e}. Retrying operation.");
                        progress
                            .send_message(format!("{retry_message}\n").as_str())
                            .await;
                        return Err(RetryError::transient(e));
                    }
                    Err(RetryError::Permanent(e))
                }
            }
        })
        .await
    }

    async fn generate_research(
        &self,
        research_model: &dyn Chat,
        prompt: &String,
        retry_message: String,
        progress: &Progress,
    ) -> Result<Research, OpenAIError> {
        self.retry_stage(retry_message, progress, || {
            self.generate_research_single(research_model, prompt)
        })
        .await
    }

    async fn generate_logical_plan(
        &self,
        logical_planner_model: &dyn Chat,
        research: &Research,
        retry_message: String,
        progress: &Progress,
    ) -> Result<LogicalPlan, OpenAIError> {
        self.retry_stage(retry_message, progress, || {
            self.generate_logical_plan_single(logical_planner_model, research)
        })
        .await
    }

    async fn generate_physical_plan(
        &self,
        plan: &LogicalPlan,
        physical_tool_planner_model: &dyn Chat,
        physical_prompt_planner_model: &dyn Chat,
        retry_message: String,
        progress: &Progress,
    ) -> Result<PhysicalPlan, OpenAIError> {
        self.retry_stage(retry_message, progress, || {
            self.generate_physical_plan_single(
                plan,
                physical_tool_planner_model,
                physical_prompt_planner_model,
                progress,
            )
        })
        .await
    }

    #[allow(clippy::unused_async)]
    async fn generate_research_single(
        &self,
        research_model: &dyn Chat,
        prompt: &String,
    ) -> Result<Research, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "orchestrator::research", input = %prompt);
        let result: Result<Research, OpenAIError> = async {
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
            let artifacts_prompt = artifacts
                .iter()
                .map(|artifact| format!("{artifact}"))
                .collect::<Vec<String>>()
                .join("\n\n");

            let logical_plan_prompt = format!("{artifacts_prompt}\n\n{prompt}");
            if logical_plan_prompt.len() > MAX_CONTENT_LENGTH {
                return Err(OpenAIError::ApiError(async_openai::error::ApiError {
                    message: format!(
                        "Research artifacts exceeds size limit: {} (max: {})",
                        logical_plan_prompt.len(),
                        MAX_CONTENT_LENGTH
                    )
                    .to_string(),
                    r#type: None,
                    param: None,
                    code: Some("string_above_max_length".to_string()),
                }));
            }

            Ok(Research {
                prompt: prompt.clone(),
                artifacts,
            })
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                tracing::info!(target: "task_history", captured_output = %serde_json::to_string(&value).unwrap_or_default());
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    async fn generate_logical_plan_single(
        &self,
        logical_planner_model: &dyn Chat,
        research: &Research,
    ) -> Result<LogicalPlan, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "orchestrator::logical_plan", input = %serde_json::to_string(&research).unwrap_or_default());

        let result: Result<LogicalPlan, OpenAIError> = async {
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
                Err(ConversionError::JsonSchema(e)) => {
                    tracing::warn!(
                        "Logical plan created did not satisfy JSONSchema format. Reattempting.\n   Initial Error: {e}"
                    );
                    let response = logical_planner_model.chat_request(initial_request).await?;
                    LogicalPlan::from_chat_completion(&response)
                        .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
                }
                Err(ConversionError::SerdeJson(e)) => {
                    return Err(OpenAIError::InvalidArgument(format!(
                        "Failed to convert chat response to logical plan: {e}"
                    )))
                }
                Err(ConversionError::SerdeYaml(e)) => {
                    return Err(OpenAIError::InvalidArgument(format!(
                        "Failed to convert chat response to logical plan: {e}"
                    )))
                }
            };

            Ok(plan)

        }.instrument(span.clone()).await;

        match result {
            Ok(value) => {
                tracing::info!(target: "task_history", captured_output = %serde_json::to_string(&value).unwrap_or_default());
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    async fn generate_physical_plan_single(
        &self,
        plan: &LogicalPlan,
        physical_tool_planner_model: &dyn Chat,
        physical_prompt_planner_model: &dyn Chat,
        progress: &Progress,
    ) -> Result<PhysicalPlan, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "orchestrator::physical_plan", input = %serde_json::to_string(&plan).unwrap_or_default());

        let result: Result<PhysicalPlan, OpenAIError> = async {
            let physical_plan = PhysicalPlan::plan(
                plan,
                physical_tool_planner_model,
                physical_prompt_planner_model,
                self.models.executor.clone(),
                progress,
                &self.tools,
            )
            .instrument(span.clone())
            .await?;
            Ok(physical_plan)
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                tracing::info!(target: "task_history", captured_output = %serde_json::to_string(&value).unwrap_or_default());
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn run_pipeline_stream(
        self: Arc<Self>,
        mut pipeline: pipeline::AgenticStage,
        advance_mode: pipeline::AdvanceMode,
    ) -> ChatCompletionResponseStream {
        let (tx, rx) =
            mpsc::channel::<Result<CreateChatCompletionStreamResponse, OpenAIError>>(100);
        let id = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();

        let models = Arc::clone(&self.llms);
        tokio::spawn(
            async move {
                let models = models.read().await;
                let Some(researcher_model) = models.get(&self.models.researcher) else {
                    let _ = tx
                        .send(Err(OpenAIError::InvalidArgument(format!(
                            "Model {} not found.",
                            self.models.researcher
                        ))))
                        .await;
                    return;
                };
                let Some(logical_planner_model) = models.get(&self.models.logical_planner) else {
                    let _ = tx
                        .send(Err(OpenAIError::InvalidArgument(format!(
                            "Model {} not found.",
                            self.models.logical_planner
                        ))))
                        .await;
                    return;
                };
                let Some(physical_tool_planner_model) =
                    models.get(&self.models.physical_tool_planner)
                else {
                    let _ = tx
                        .send(Err(OpenAIError::InvalidArgument(format!(
                            "Model {} not found.",
                            self.models.physical_tool_planner
                        ))))
                        .await;
                    return;
                };
                let Some(physical_prompt_planner_model) =
                    models.get(&self.models.physical_prompt_planner)
                else {
                    let _ = tx
                        .send(Err(OpenAIError::InvalidArgument(format!(
                            "Model {} not found.",
                            self.models.physical_prompt_planner
                        ))))
                        .await;
                    return;
                };

                let service = Arc::clone(&self);
                let id = id.clone();
                let mut progress = Progress::new(pipeline.new_stage_index(), tx.clone());
                loop {
                    progress.start_working_stage().await;
                    let retry_message = pipeline.retry_message();
                    match pipeline {
                        pipeline::AgenticStage::Research { prompt } => {
                            let research = try_send_err!(
                                service
                                    .generate_research(
                                        researcher_model.as_ref(),
                                        &prompt,
                                        retry_message,
                                        &progress,
                                    )
                                    .await,
                                tx
                            );
                            if let Some(EvalModelConfig { ref model_name, .. }) = self.models.research_artifact_eval {
                                let Some(model) = models.get(model_name) else {
                                    let _ = tx
                                        .send(Err(OpenAIError::InvalidArgument(format!(
                                            "Model {model_name} not found."
                                        ))))
                                        .await;
                                    return;
                                };
                                let score = try_send_err!(
                                    score::score_research(prompt.as_str(), &research, model.as_ref()).await,
                                    tx
                                );
                                let _ = progress
                                    .send_message(
                                        format!(
                                            "<meta name=\"score\" value=\"{score}\"/>\n<meta name=\"scorer\" value=\"{model_name}\"/>"
                                        )
                                        .as_str(),
                                    )
                                    .await;
                            }
                            try_send_err!(
                                write_artifact("research/research_artifacts", &id, &research)
                                    .map_err(|e| {
                                        OpenAIError::InvalidArgument(format!(
                                            "Error writing research artifacts: {e}"
                                        ))
                                    }),
                                tx
                            );
                            pipeline = pipeline::AgenticStage::LogicalPlan(research);
                        }
                        pipeline::AgenticStage::LogicalPlan(research) => {
                            let logical_plan = try_send_err!(
                                service
                                    .generate_logical_plan(
                                        logical_planner_model.as_ref(),
                                        &research,
                                        retry_message,
                                        &progress,
                                    )
                                    .await,
                                tx
                            );
                            try_send_err!(
                                write_artifact("logical/logical_plan", &id, &logical_plan).map_err(
                                    |e| {
                                        OpenAIError::InvalidArgument(format!(
                                            "Error writing logical plan: {e}"
                                        ))
                                    }
                                ),
                                tx
                            );
                            pipeline = pipeline::AgenticStage::PhysicalPlan(logical_plan);
                        }
                        pipeline::AgenticStage::PhysicalPlan(logical_plan) => {
                            let physical_plan = try_send_err!(
                                service
                                    .generate_physical_plan(
                                        &logical_plan,
                                        physical_tool_planner_model.as_ref(),
                                        physical_prompt_planner_model.as_ref(),
                                        retry_message,
                                        &progress,
                                    )
                                    .await,
                                tx
                            );

                            try_send_err!(
                                write_artifact("physical/physical_plan", &id, &physical_plan)
                                    .map_err(|e| {
                                        OpenAIError::InvalidArgument(format!(
                                            "Error writing physical plan: {e}"
                                        ))
                                    }),
                                tx
                            );

                            pipeline = pipeline::AgenticStage::Execution(physical_plan);
                        }
                        pipeline::AgenticStage::Execution(physical_plan) => {
                            let mut executor = PhysicalJobExecutor::new(
                                physical_plan,
                                Arc::clone(&service.llms),
                                service.tools.clone(),
                                self.models.verifier.clone(),
                            );
                            let output = try_send_err!(
                                executor.execute(&progress).await.map_err(|e| {
                                    OpenAIError::InvalidArgument(format!(
                                        "Error executing physical plan: {e}"
                                    ))
                                }),
                                tx
                            );

                            pipeline = pipeline::AgenticStage::Reporting(output);
                        }
                        pipeline::AgenticStage::Reporting(ref s) => {
                            progress
                                .with_working_ending(
                                    format!("{}\n", pipeline.previous_stage_summary()).as_str(),
                                )
                                .await;
                            progress.send_message(s.as_str()).await;
                            break;
                        }
                    }
                    progress
                        .with_working_ending(
                            format!("{}\n", pipeline.previous_stage_summary()).as_str(),
                        )
                        .await;

                    // Advance to the next stage (pipeline itself updated above).
                    progress.new_stage((&pipeline).into());

                    if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                        break;
                    }
                }
            }
            .instrument(Span::current()),
        );

        Box::pin(ReceiverStream::new(rx))
    }
}

fn write_artifact<T: ?Sized + Serialize>(
    base_name: &str,
    id: &str,
    artifact: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let artifact_json = serde_json::to_string_pretty(artifact)?;

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

fn should_retry_on_error(e: &OpenAIError) -> bool {
    if let OpenAIError::ApiError(api_error) = e {
        if let Some(code) = &api_error.code {
            return code == "string_above_max_length";
        }
    }
    false
}

#[async_trait]
impl Chat for AgentChat {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        todo!()
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let (pipeline, advance_mode) = pipeline::AgenticStage::try_new(&req)
            .map_err(|e| OpenAIError::InvalidArgument(format!("Error parsing request: {e}")))?;

        Ok(Arc::new(self.clone()).run_pipeline_stream(pipeline, advance_mode))
    }

    // The non-streaming endpoint consumes the stream and returns the final result.
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let stream = self.chat_stream(req).await?;

        // For chat request, the last item is to be used in the response.
        let final_stream_item = stream
            .try_fold(None, |_, item| async { Ok(Some(item)) })
            .await?
            .ok_or_else(|| OpenAIError::InvalidArgument("No output was produced".to_string()))?;

        Ok(stream_to_request_payload(final_stream_item))
    }
}

/// `stream` should be a full message.
#[allow(deprecated)]
fn stream_to_request_payload(
    stream: CreateChatCompletionStreamResponse,
) -> CreateChatCompletionResponse {
    CreateChatCompletionResponse {
        id: stream.id,
        object: stream.object,
        created: stream.created,
        model: stream.model,
        choices: stream
            .choices
            .into_iter()
            .map(|c| ChatChoice {
                index: c.index,
                message: ChatCompletionResponseMessage {
                    content: c.delta.content,
                    refusal: c.delta.refusal,
                    tool_calls: c.delta.tool_calls.map(|calls| {
                        calls
                            .into_iter()
                            .map(|call| ChatCompletionMessageToolCall {
                                id: call.id.unwrap_or_default(),
                                r#type: ChatCompletionToolType::Function,
                                function: FunctionCall {
                                    name: call
                                        .function
                                        .clone()
                                        .and_then(|f| f.name)
                                        .unwrap_or_default(),
                                    arguments: call
                                        .function
                                        .and_then(|f| f.arguments)
                                        .unwrap_or_default(),
                                },
                            })
                            .collect()
                    }),
                    role: c.delta.role.unwrap_or_default(),
                    function_call: None,
                    audio: None,
                },
                finish_reason: c.finish_reason,
                logprobs: c.logprobs,
            })
            .collect(),
        service_tier: stream.service_tier,
        system_fingerprint: stream.system_fingerprint,
        usage: stream.usage,
    }
}

#[allow(dead_code)]
fn add_system_message(req: &mut CreateChatCompletionRequest, message: String) {
    req.messages
        .insert(0, ChatCompletionRequestMessage::System(message.into()));
}

pub fn validate_structured_output<T>(
    yaml_str: &str,
    completion: &CreateChatCompletionResponse,
) -> Result<T, ConversionError>
where
    T: DeserializeOwned,
{
    let body = completion
        .choices
        .first()
        .and_then(|choice| choice.message.content.clone())
        .unwrap_or_default();

    let as_value = serde_json::from_str(body.as_str()).map_err(ConversionError::SerdeJson)?;

    // First we validate against JSONSchema so the error message is more precise and informative.
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).map_err(ConversionError::SerdeYaml)?;

    let v = jsonschema::validator_for(&yaml_value["json_schema"]["schema"])
        .map_err(|e| ConversionError::JsonSchema(e.to_owned()))?;
    v.validate(&as_value)
        .map_err(|e| ConversionError::JsonSchema(e.to_owned()))?;

    serde_json::from_value(as_value).map_err(ConversionError::SerdeJson)
}
