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
use event_stream::get_event_stream;
use futures::stream;
use futures_util::TryStreamExt;
use llms::chat::{nsql::SqlGeneration, Chat};
use logical::{logical_plan_complete_summary, plan::LogicalPlan};
use physical::{executor::PhysicalJobExecutor, plan::PhysicalPlan};
use pipeline::create_working_stream_payload;
use progress::{Index, Progress, ProgressType, StageName};
use research::{
    model::{parse_response, research_complete_msg},
    Research,
};
use score::score;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock as RwLockSync},
    time::Duration,
};
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
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
    progress_index: Arc<RwLockSync<Index>>,
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
            progress_index: Arc::new(RwLockSync::new(Index::new(StageName::Research))),
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
                        tracing::info!(target: "task_history", progress = %retry_message);
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
    ) -> Result<Research, OpenAIError> {
        self.retry_stage(retry_message, || {
            self.generate_research_single(research_model, prompt)
        })
        .await
    }

    async fn generate_logical_plan(
        &self,
        logical_planner_model: &dyn Chat,
        research: &Research,
        retry_message: String,
    ) -> Result<LogicalPlan, OpenAIError> {
        self.retry_stage(retry_message, || {
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
    ) -> Result<PhysicalPlan, OpenAIError> {
        self.retry_stage(retry_message, || {
            self.generate_physical_plan_single(
                plan,
                physical_tool_planner_model,
                physical_prompt_planner_model,
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
    ) -> Result<PhysicalPlan, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "orchestrator::physical_plan", input = %serde_json::to_string(&plan).unwrap_or_default());

        let result: Result<PhysicalPlan, OpenAIError> = async {
            let physical_plan = PhysicalPlan::plan(
                plan,
                physical_tool_planner_model,
                physical_prompt_planner_model,
                self.models.executor.clone(),
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

        let models = Arc::clone(&self.llms);

        let (stream_ended, stream_ended_receiver) = tokio::sync::oneshot::channel::<()>();

        if let Ok(mut event_stream) = get_event_stream() {
            let mut main_stream_ended = Box::pin(stream::once(async move {
                let _ = stream_ended_receiver.await;
            }));
            let current_index = Arc::clone(&self.progress_index);
            let tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        Some(mut event) = event_stream.next() => {
                            if !event.starts_with("!---jsonl") {
                                // If the event isn't already formatted for JSONL, format it using the current index details.
                                event = Index::log(&current_index, event);
                            }
                            let req = create_working_stream_payload(format!("{event}\n"));
                            if tx.send(req).await.is_err() {
                                // Stream is closed - stop waiting for new events
                                return;
                            }
                        }
                        _ = main_stream_ended.next() => {
                            // Stream ended signal received
                            return;
                        }
                    }
                }
            });
        };

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
                loop {
                    progress::advance_stage(&self.progress_index, (&pipeline).into());
                    let retry_message = pipeline.retry_message();
                    match pipeline {
                        pipeline::AgenticStage::Research { prompt } => {
                            let research = try_send_err!(
                                service
                                    .generate_research(
                                        researcher_model.as_ref(),
                                        &prompt,
                                        retry_message,
                                    )
                                    .await,
                                tx
                            );
                            if let Err(e) = evaluate_with_model(
                                self.models.research_artifact_eval.as_ref(),
                                &models,
                                prompt.clone(),
                                format!("{research:?}"),
                                Progress::new(ProgressType::Evaluation)
                                    .parent_id(StageName::Research.id().to_string()),
                            )
                            .await
                            {
                                tracing::error!(target: "task_history", progress = %e);
                                return;
                            }
                            pipeline = pipeline::AgenticStage::LogicalPlan(research);
                        }
                        pipeline::AgenticStage::LogicalPlan(research) => {
                            let logical_plan = try_send_err!(
                                service
                                    .generate_logical_plan(
                                        logical_planner_model.as_ref(),
                                        &research,
                                        retry_message,
                                    )
                                    .await,
                                tx
                            );
                            if let Ok(logical_plan_json_str) = serde_json::to_string(&logical_plan) {
                                let logical_plan_artifact = Progress::new(ProgressType::Log)
                                    .parent_id(StageName::LogicalPlan.id().to_string())
                                    .content(format!("Logical Plan:\n```json\n{logical_plan_json_str}\n```"))
                                    .tag("artifact", "logical_plan")
                                    .to_jsonl();
                                tracing::info!(target: "task_history", progress = %logical_plan_artifact);
                            };
                            if let Err(e) = evaluate_with_model(
                                self.models.logical_plan_eval.as_ref(),
                                &models,
                                format!("{research:?}"),
                                format!("{logical_plan:?}"),
                                Progress::new(ProgressType::Evaluation)
                                    .parent_id(StageName::LogicalPlan.id().to_string()),
                            )
                            .await
                            {
                                tracing::error!(target: "task_history", progress = %e);
                                return;
                            }
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
                                    )
                                    .await,
                                tx
                            );
                            if let Ok(physical_plan_json_str) = serde_json::to_string(&physical_plan) {
                                let physical_plan_artifact = Progress::new(ProgressType::Log)
                                    .parent_id(StageName::PhysicalPlan.id().to_string())
                                    .content(format!("Execution Plan:\n```json\n{physical_plan_json_str}\n```"))
                                    .tag("artifact", "physical_plan")
                                    .to_jsonl();
                                tracing::info!(target: "task_history", progress = %physical_plan_artifact);
                            };
                            if let Err(e) = evaluate_with_model(
                                self.models.physical_plan_eval.as_ref(),
                                &models,
                                format!("{logical_plan:?}"),
                                format!("{physical_plan:?}"),
                                Progress::new(ProgressType::Evaluation)
                                    .parent_id(StageName::PhysicalPlan.id().to_string()),
                            )
                            .await
                            {
                                tracing::error!(target: "task_history", progress = %e);
                                return;
                            }

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
                                executor.execute().await.map_err(|e| {
                                    OpenAIError::InvalidArgument(format!(
                                        "Error executing physical plan: {e}"
                                    ))
                                }),
                                tx
                            );

                            pipeline = pipeline::AgenticStage::Reporting(output);
                        }
                        pipeline::AgenticStage::Reporting(ref s) => {
                            let summary = pipeline.previous_stage_summary();
                            let json_progress = Progress::new(ProgressType::Log)
                                .parent_id(StageName::Reporting.id().to_string())
                                .content(summary)
                                .to_jsonl();
                            tracing::info!(target: "task_history", progress = %json_progress);
                            let req = create_working_stream_payload(format!("{s}\n"));
                            let _ = tx.send(req).await;
                            break;
                        }
                    }
                    let summary = pipeline.previous_stage_summary();
                    let json_progress = Progress::new(ProgressType::Log)
                        .parent_id(
                            progress::current_stage(&self.progress_index)
                                .id()
                                .to_string(),
                        )
                        .content(summary)
                        .to_jsonl();
                    tracing::info!(target: "task_history", progress = %json_progress);

                    if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                        break;
                    }
                }
                let _ = stream_ended.send(());
            }
            .instrument(Span::current()),
        );

        Box::pin(ReceiverStream::new(rx))
    }
}

async fn evaluate_with_model(
    eval_config: Option<&EvalModelConfig>,
    models: &HashMap<String, Box<dyn Chat>>,
    input: String,
    actual: String,
    progress: Progress,
) -> Result<(), anyhow::Error> {
    if let Some(EvalModelConfig { ref model_name, .. }) = eval_config {
        let Some(model) = models.get(model_name) else {
            return Err(anyhow::anyhow!("Model {} not found.", model_name));
        };

        let score = score(model.as_ref(), input, actual).await?;

        let json_progress = progress
            .tag("scorer", model_name.clone())
            .tag("score", score.to_string())
            .to_jsonl();
        tracing::info!(target: "task_history", progress = %json_progress);
    }

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
