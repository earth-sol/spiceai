use super::plan::ToolStep;
use async_openai::types::{
    ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestDeveloperMessageContent,
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageContent,
    ChatCompletionRequestToolMessageContent, ChatCompletionRequestUserMessageContent,
    CreateChatCompletionRequestArgs,
};
use llms::chat::Chat;

pub async fn tool_call_agentic_retry(
    model: &dyn Chat,
    history: &[ChatCompletionRequestMessage],
    tool_step: ToolStep,
    tool_output: ChatCompletionRequestMessage,
    tool_failure_reason: String,
) -> Result<ToolStep, anyhow::Error> {
    let mut messages = history.to_vec();
    let feedback_content = extract_content(&tool_output)?;
    messages.push(ChatCompletionRequestMessage::User(
        format!(
            "You are an expert at understanding why commands failed and how to fix them.
        The previous messages have the history of the conversation so far.
        You need to figure out why this command failed and how to fix it.
        Your output should be a JSON object with the new command input.
        If you cannot fix the command, return the word ERROR on the first line, and on the second line, return the reason why you cannot fix it.

        Command: {}
        Command input: {}
        Command output: {}
        Command failure reason: {}
        ",
            tool_step.tool, tool_step.body, feedback_content, tool_failure_reason
        )
        .into(),
    ));

    let req = CreateChatCompletionRequestArgs::default()
        .messages(messages)
        .build()?;
    let response = model.chat_request(req).await?;

    let Some(message) = response.choices[0].message.content.clone() else {
        return Err(anyhow::anyhow!("No message content found"));
    };

    if message.starts_with("ERROR") {
        let reason = message.split('\n').nth(1).unwrap_or("Unknown error");
        return Err(anyhow::anyhow!("Unable to fix tool call: {reason}"));
    }

    tracing::debug!("Agentic retry response: {}", message);

    let _: serde_json::Value =
        serde_json::from_str(&message).map_err(|e| anyhow::anyhow!("Invalid JSON: {e}"))?;

    Ok(ToolStep {
        tool: tool_step.tool,
        body: message,
        task_uuid: tool_step.task_uuid,
        model: tool_step.model,
    })
}

fn extract_content(message: &ChatCompletionRequestMessage) -> Result<String, anyhow::Error> {
    match message {
        ChatCompletionRequestMessage::User(msg) => match &msg.content {
            ChatCompletionRequestUserMessageContent::Text(text) => Ok(text.clone()),
            ChatCompletionRequestUserMessageContent::Array(_) => {
                Err(anyhow::anyhow!("Unsupported message content type"))
            }
        },
        ChatCompletionRequestMessage::Tool(msg) => match &msg.content {
            ChatCompletionRequestToolMessageContent::Text(text) => Ok(text.clone()),
            ChatCompletionRequestToolMessageContent::Array(_) => {
                Err(anyhow::anyhow!("Unsupported message content type"))
            }
        },
        ChatCompletionRequestMessage::Assistant(msg) => match &msg.content {
            Some(ChatCompletionRequestAssistantMessageContent::Text(text)) => Ok(text.clone()),
            _ => Err(anyhow::anyhow!("Unsupported message content type")),
        },
        ChatCompletionRequestMessage::System(msg) => match &msg.content {
            ChatCompletionRequestSystemMessageContent::Text(text) => Ok(text.clone()),
            ChatCompletionRequestSystemMessageContent::Array(_) => {
                Err(anyhow::anyhow!("Unsupported message content type"))
            }
        },
        ChatCompletionRequestMessage::Developer(msg) => match &msg.content {
            ChatCompletionRequestDeveloperMessageContent::Text(text) => Ok(text.clone()),
            ChatCompletionRequestDeveloperMessageContent::Array(_) => {
                Err(anyhow::anyhow!("Unsupported message content type"))
            }
        },
        ChatCompletionRequestMessage::Function(msg) => match &msg.content {
            Some(text) => Ok(text.clone()),
            None => Err(anyhow::anyhow!("Unsupported message content type")),
        },
    }
}

// Code graveyard:

// async fn execute_step(
//     &self,
//     step_history: &[ChatCompletionRequestMessage],
//     step: &Step,
// ) -> Result<ChatCompletionRequestMessage, anyhow::Error> {
//     match step {
//         Step::Tool(tool_step) => {
//             let mut tool_step = tool_step.clone();
//             let tool_context = step_history.to_vec();
//             let mut attempts = 0;
//             loop {
//                 let tool_result = self.execute_tool(&tool_context, &tool_step).await;
//                 match tool_result {
//                     Ok(tool_output) => return Ok(tool_output),
//                     Err(e) => match e {
//                         ExecuteToolError::ToolCallFailed {
//                             reason,
//                             tool_output,
//                         } => {
//                             attempts += 1;
//                             if attempts > 5 {
//                                 return Err(anyhow::anyhow!(
//                                     "Tool call failed: {reason}\nTool output: {tool_output:?}\nTool step: {tool_step:?}"
//                                 ));
//                             }
//                             tracing::error!(
//                                 "Tool call failed: {reason}\nTool output: {tool_output:?}\nTool step: {tool_step:?}"
//                             );
//                             let llms = &*self.llms.read().await;
//                             let model = llms.get(&tool_step.model).ok_or_else(|| {
//                                 anyhow::anyhow!("Model {} not found", tool_step.model)
//                             })?;
//                             tool_step = tool_call_agentic_retry(
//                                 model.as_ref(),
//                                 &tool_context,
//                                 tool_step,
//                                 tool_output,
//                                 reason,
//                             )
//                             .await?;
//                         }
//                         ExecuteToolError::Other(e) => {
//                             return Err(e);
//                         }
//                     },
//                 }
//             }
//         }
//         Step::Prompt(prompt_step) => self.execute_prompt(step_history, prompt_step).await,
//     }
// }
