use async_openai::types::ChatCompletionRequestMessage;
use async_openai::types::ChatCompletionRequestSystemMessageContent;
use async_openai::types::ChatCompletionRequestUserMessageContent;
use async_openai::types::CreateChatCompletionRequest;

use crate::research::Research;
use crate::LogicalPlan;
use crate::PhysicalPlan;

/// Defines the pipeline stages that an agent request goes through. The values for each stage are the inputs for that stage.
pub enum AgentPipeline {
    /// The research stage is used to gather artifacts that will be used to create the logical plan.
    Research { prompt: String },
    /// The logical plan stage is used to create a logical plan from prompt and the artifacts gathered in the research stage.
    LogicalPlan(Research),
    /// The physical plan stage is used to create a physical plan from the logical plan.
    PhysicalPlan(LogicalPlan),
    /// The execution stage is used to execute the physical plan.
    Execution(PhysicalPlan),
    /// The output stage is used to output the result of the execution.
    Output(String),
}

impl AgentPipeline {
    pub fn try_new(req: &CreateChatCompletionRequest) -> Result<Self, anyhow::Error> {
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
            return Ok(Self::Research { prompt: content });
        };
        tracing::debug!("Last line: {last_line}");

        if last_line.starts_with(".research") {
            let research_file = last_line
                .split(' ')
                .nth(1)
                .expect("Research artifacts file not found");
            tracing::debug!("Research artifacts file: {research_file}");
            let research_str = std::fs::read_to_string(research_file)
                .map_err(|e| anyhow::anyhow!("Error reading research artifacts: {e}"))?;
            tracing::info!("Research artifacts from file: {research_str}");
            let research = serde_json::from_str(&research_str)
                .map_err(|e| anyhow::anyhow!("Error parsing research artifacts: {e}"))?;
            return Ok(Self::LogicalPlan(research));
        }
        if last_line.starts_with(".logical_plan") {
            let logical_plan_file = last_line
                .split(' ')
                .nth(1)
                .expect("Logical plan file not found");
            tracing::debug!("Logical plan file: {logical_plan_file}");
            let logical_plan_str = std::fs::read_to_string(logical_plan_file)
                .map_err(|e| anyhow::anyhow!("Error reading logical plan: {e}"))?;
            tracing::info!("Logical plan from file: {logical_plan_str}");
            let logical_plan = LogicalPlan::new(&logical_plan_str)
                .map_err(|e| anyhow::anyhow!("Error parsing logical plan: {e}"))?;
            return Ok(Self::PhysicalPlan(logical_plan));
        }
        if last_line.starts_with(".physical_plan") {
            let physical_plan_file = last_line
                .split(' ')
                .nth(1)
                .expect("Physical plan file not found");
            tracing::debug!("Physical plan file: {physical_plan_file}");
            let physical_plan_str = std::fs::read_to_string(physical_plan_file)
                .map_err(|e| anyhow::anyhow!("Error reading physical plan: {e}"))?;
            tracing::info!("Physical plan from file: {physical_plan_str}");
            let physical_plan = PhysicalPlan::new(&physical_plan_str)
                .map_err(|e| anyhow::anyhow!("Error parsing physical plan: {e}"))?;
            return Ok(Self::Execution(physical_plan));
        }

        Ok(Self::Research { prompt: content })
    }
}
