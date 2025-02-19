use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
        CreateChatCompletionResponse,
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct LogicalPlan {
    pub tasks: Vec<Task>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    #[serde(default)]
    pub uuid: Option<Uuid>,
    pub objective: String,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Step {
    #[serde(default)]
    pub uuid: Option<Uuid>,
    pub description: String,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    pub action: Action,
    pub input: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    ChangeDirectory,
    CreateDirectory,
    ReadObject,
    WriteObject,
    ExecuteTerminal,
    Other,
    Response,
    RequestForInfo,
    RetrieveMetadata,
    Validation,
    Improvement,
}

impl LogicalPlan {
    pub fn new(body: &str) -> Result<Self, serde_json::Error> {
        let mut plan: LogicalPlan = serde_json::from_str(body)?;

        plan.tasks.iter_mut().for_each(|task| {
            task.steps.iter_mut().for_each(|step| {
                if step.uuid.is_none() {
                    step.uuid = Some(Uuid::new_v4());
                }
            });

            if task.uuid.is_none() {
                task.uuid = Some(Uuid::new_v4());
            }
        });

        Ok(plan)
    }

    pub fn from_chat_completion(
        completion: &CreateChatCompletionResponse,
    ) -> Result<Self, anyhow::Error> {
        let body = completion
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .ok_or_else(|| anyhow::anyhow!("No content in the response"))?;

        Ok(Self::new(body)?)
    }

    pub fn to_chat_request(&self) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let body = serde_json::to_string(self)?;
        let req = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestMessage::User(body.into())])
            .build()?;
        Ok(req)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_new_logical_plan() {
        let body = r#"
        {
            "tasks": [
                {
                    "objective": "Task 1",
                    "tags": ["setup"],
                    "steps": [
                        {
                            "description": "Change to temporary directory",
                            "tags": ["filesystem"],
                            "action": "change_directory",
                            "input": "/tmp"
                        },
                        {
                            "description": "Create test directory",
                            "action": "create_directory",
                            "input": "/tmp/test"
                        }
                    ]
                }
            ]
        }
        "#;

        let plan = LogicalPlan::new(body).expect("Should be able to parse the body");

        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].steps.len(), 2);
        assert!(plan.tasks[0].uuid.is_some());
        assert!(plan.tasks[0].steps[0].uuid.is_some());
        assert!(plan.tasks[0].steps[1].uuid.is_some());
    }

    #[test]
    fn test_logical_plan_retains_uuid() {
        let body = r#"
        {
            "tasks": [
                {
                    "uuid": "d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b",
                    "objective": "Stage 1",
                    "steps": [
                        {
                            "uuid": "d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b",
                            "description": "Step 1",
                            "action": "change_directory",
                            "input": "/tmp"
                        }
                    ]
                }
            ]
        }
        "#;

        let plan = LogicalPlan::new(body).expect("Should be able to parse the body");

        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(
            plan.tasks[0].uuid,
            Some(
                Uuid::parse_str("d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b").expect("Should be a UUID")
            )
        );
    }
}
