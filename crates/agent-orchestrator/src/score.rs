use crate::research::Research;
use async_openai::{
    error::OpenAIError,
    types::{CreateChatCompletionRequestArgs, ResponseFormat, ResponseFormatJsonSchema},
};
use llms::chat::Chat;
use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(JsonSchema, Serialize, Deserialize)]
pub struct OutputScore {
    score: f32,
}

async fn score(model: &dyn Chat, input: String, actual: String) -> Result<f32, OpenAIError> {
    let mut schema = serde_json::to_value(schema_for!(OutputScore))
        .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;
    schema["additionalProperties"] = Value::Bool(false);

    // For some models json_schema doesn't like format: 'format' is not permitted.
    if let Some(properties) = schema.get_mut("properties").and_then(|v| v.as_object_mut()) {
        for (_key, value) in properties.iter_mut() {
            if let Some(obj) = value.as_object_mut() {
                obj.remove("format");
            }
        }
    }

    let req = CreateChatCompletionRequestArgs::default()
        .messages(vec![])
        .response_format(ResponseFormat::JsonSchema {
            json_schema: ResponseFormatJsonSchema {
                description: None,
                name: "outputscore".to_string(),
                schema: Some(schema),
                strict: Some(true),
            },
        })
        .metadata(json!({
            "input": input,
            "actual": actual,
        }))
        .build()?;
    let resp = model.chat_request(req).await?;

    if let Some(choice) = resp.choices.first() {
        if let Some(ref content) = choice.message.content {
            let output_score: OutputScore =
                serde_json::from_str(content.as_str()).map_err(OpenAIError::JSONDeserialize)?;
            return Ok(output_score.score);
        }
    }

    Ok(0.0)
}

pub(crate) async fn score_research(
    input: &str,
    research: &Research,
    model: &dyn Chat,
) -> Result<f32, OpenAIError> {
    score(model, input.to_string(), format!("{research:?}")).await
}
