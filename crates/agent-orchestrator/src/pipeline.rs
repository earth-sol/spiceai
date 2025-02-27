use std::{fmt::Display, time::SystemTime};

use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoiceStream, ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageContent,
        ChatCompletionRequestUserMessageContent, ChatCompletionStreamResponseDelta,
        CreateChatCompletionRequest, CreateChatCompletionStreamResponse, Role,
    },
};

use crate::{
    logical_plan_complete_summary, progress::Index, research::Research, research_complete_msg,
    LogicalPlan, PhysicalPlan,
};

/// Defines the pipeline stages that an agent request goes through. The values for each stage are the inputs for that stage.
#[derive(Clone, Debug)]
pub enum AgenticStage {
    /// The research stage is used to gather artifacts that will be used to create the logical plan.
    Research { prompt: String },
    /// The logical plan stage is used to create a logical plan from prompt and the artifacts gathered in the research stage.
    LogicalPlan(Research),
    /// The physical plan stage is used to create a physical plan from the logical plan.
    PhysicalPlan(LogicalPlan),
    /// The execution stage is used to execute the physical plan.
    Execution(PhysicalPlan),
    /// The reporting stage is used to report the results of the execution stage.
    Reporting(String),
}

pub enum StageName {
    Research,
    LogicalPlan,
    PhysicalPlan,
    Execution,
    Reporting,
}

impl From<&AgenticStage> for StageName {
    fn from(stage: &AgenticStage) -> Self {
        match stage {
            AgenticStage::Research { .. } => Self::Research,
            AgenticStage::LogicalPlan(_) => Self::LogicalPlan,
            AgenticStage::PhysicalPlan(_) => Self::PhysicalPlan,
            AgenticStage::Execution(_) => Self::Execution,
            AgenticStage::Reporting(_) => Self::Reporting,
        }
    }
}

impl Display for StageName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Research => write!(f, "research"),
            Self::LogicalPlan | Self::PhysicalPlan => write!(f, "planning"),
            Self::Execution => write!(f, "execution"),
            Self::Reporting => write!(f, "reporting"),
        }
    }
}

impl AgenticStage {
    pub(crate) fn previous_stage_summary(&self) -> String {
        match self {
            Self::Research { prompt } => format!("Researching: {prompt}"),
            Self::LogicalPlan(r) => research_complete_msg(r),
            Self::PhysicalPlan(l) => logical_plan_complete_summary(l),
            Self::Execution(_) => PhysicalPlan::summary(),
            Self::Reporting(_) => "Execution complete".to_string(),
        }
    }

    pub(crate) fn index(&self, task: usize, step: usize) -> Index {
        Index {
            stage: self.into(),
            task,
            step,
        }
    }
}

pub enum AdvanceMode {
    /// The pipeline will stop after the current stage is completed.
    Stop,
    /// The pipeline will continue to the next stage after the current stage is completed.
    Continue,
}

impl AgenticStage {
    pub fn try_new(
        req: &CreateChatCompletionRequest,
    ) -> Result<(Self, AdvanceMode), anyhow::Error> {
        let mut content = String::new();
        tracing::debug!("Request: {req:?}");
        let Some(message) = req.messages.last() else {
            return Err(anyhow::anyhow!("No message found in request"));
        };
        match message {
            ChatCompletionRequestMessage::User(user_message) => {
                if let ChatCompletionRequestUserMessageContent::Text(text) = &user_message.content {
                    content.push_str(text);
                }
            }
            // For some reason, we are getting a system message here from `spice chat`
            ChatCompletionRequestMessage::System(system_message) => {
                if let ChatCompletionRequestSystemMessageContent::Text(text) =
                    &system_message.content
                {
                    content.push_str(text);
                }
            }
            _ => return Err(anyhow::anyhow!("Invalid message type")),
        }

        tracing::debug!("Request content: {content}");
        let Some(last_line) = content.lines().last() else {
            return Ok((Self::Research { prompt: content }, AdvanceMode::Continue));
        };
        tracing::debug!("Last line: {last_line}");

        let advance_mode = if last_line.contains("--stop") {
            AdvanceMode::Stop
        } else {
            AdvanceMode::Continue
        };

        if last_line.starts_with(".research") {
            let Some(research_file) = last_line.split(' ').nth(1) else {
                return Err(anyhow::anyhow!("Research artifacts file not found"));
            };
            tracing::debug!("Research artifacts file: {research_file}");
            let research_str = std::fs::read_to_string(research_file)
                .map_err(|e| anyhow::anyhow!("Error reading research artifacts: {e}"))?;
            tracing::info!("Research artifacts from file: {research_str}");
            let research = serde_json::from_str(&research_str)
                .map_err(|e| anyhow::anyhow!("Error parsing research artifacts: {e}"))?;
            return Ok((Self::LogicalPlan(research), advance_mode));
        }
        if last_line.starts_with(".logical_plan") {
            let Some(logical_plan_file) = last_line.split(' ').nth(1) else {
                return Err(anyhow::anyhow!("Logical plan file not found"));
            };
            tracing::debug!("Logical plan file: {logical_plan_file}");
            let logical_plan_str = std::fs::read_to_string(logical_plan_file)
                .map_err(|e| anyhow::anyhow!("Error reading logical plan: {e}"))?;
            tracing::info!("Logical plan from file: {logical_plan_str}");
            let logical_plan = LogicalPlan::new(&logical_plan_str)
                .map_err(|e| anyhow::anyhow!("Error parsing logical plan: {e}"))?;
            return Ok((Self::PhysicalPlan(logical_plan), advance_mode));
        }
        if last_line.starts_with(".physical_plan") {
            let Some(physical_plan_file) = last_line.split(' ').nth(1) else {
                return Err(anyhow::anyhow!("Physical plan file not found"));
            };
            tracing::debug!("Physical plan file: {physical_plan_file}");
            let physical_plan_str = std::fs::read_to_string(physical_plan_file)
                .map_err(|e| anyhow::anyhow!("Error reading physical plan: {e}"))?;
            tracing::info!("Physical plan from file: {physical_plan_str}");
            let physical_plan = PhysicalPlan::new(&physical_plan_str)
                .map_err(|e| anyhow::anyhow!("Error parsing physical plan: {e}"))?;
            return Ok((Self::Execution(physical_plan), advance_mode));
        }

        Ok((Self::Research { prompt: content }, AdvanceMode::Continue))
    }
}

#[allow(clippy::cast_possible_truncation, deprecated)]
pub(crate) fn create_working_stream_payload(
    content: String,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    let created = u32::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            .as_secs(),
    )
    .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

    Ok(CreateChatCompletionStreamResponse {
        created,
        service_tier: None,
        system_fingerprint: None,
        object: "chat.completion.chunk".to_string(),
        usage: None,
        model: String::new(),
        id: String::new(),
        choices: vec![ChatChoiceStream {
            index: 0,
            finish_reason: None,
            logprobs: None,
            delta: ChatCompletionStreamResponseDelta {
                content: Some(content),
                function_call: None,
                tool_calls: None,
                role: Some(Role::Assistant),
                refusal: None,
            },
        }],
    })
}
