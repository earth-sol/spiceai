use async_openai::types::CreateChatCompletionResponse;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct LogicalPlan {
    pub groups: Vec<Group>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Group {
    #[serde(default)]
    pub uuid: Option<Uuid>,
    pub position: i64,
    pub objective: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Step {
    #[serde(default)]
    pub uuid: Option<Uuid>,
    pub position: i64,
    pub description: String,
    pub r#type: StepType,
    pub action: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepType {
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

        plan.groups.iter_mut().for_each(|group| {
            group.steps.iter_mut().for_each(|step| {
                if step.uuid.is_none() {
                    step.uuid = Some(Uuid::new_v4());
                }
            });

            if group.uuid.is_none() {
                group.uuid = Some(Uuid::new_v4());
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
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_new_logical_plan() {
        let body = r#"
        {
            "groups": [
                {
                    "position": 1,
                    "objective": "Group 1",
                    "steps": [
                        {
                            "position": 1,
                            "description": "Step 1",
                            "type": "change_directory",
                            "action": "/tmp"
                        },
                        {
                            "position": 2,
                            "description": "Step 2",
                            "type": "create_directory",
                            "action": "/tmp/test"
                        }
                    ]
                },
                {
                    "position": 2,
                    "objective": "Group 2",
                    "steps": [
                        {
                            "position": 1,
                            "description": "Step 1",
                            "type": "read_object",
                            "action": "/tmp/test.txt"
                        },
                        {
                            "position": 2,
                            "description": "Step 2",
                            "type": "write_object",
                            "action": "/tmp/test.txt"
                        }
                    ]
                }
            ]
        }
        "#;

        let plan = LogicalPlan::new(body).expect("Should be able to parse the body");

        assert_eq!(plan.groups.len(), 2);
        assert_eq!(plan.groups[0].steps.len(), 2);
        assert_eq!(plan.groups[1].steps.len(), 2);

        assert!(plan.groups[0].uuid.is_some());
        assert!(plan.groups[0].steps[0].uuid.is_some());
        assert!(plan.groups[0].steps[1].uuid.is_some());

        assert!(plan.groups[1].uuid.is_some());
        assert!(plan.groups[1].steps[0].uuid.is_some());
        assert!(plan.groups[1].steps[1].uuid.is_some());
    }

    #[test]
    fn test_logical_plan_retains_uuid() {
        let body = r#"
        {
            "groups": [
                {
                    "uuid": "d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b",
                    "position": 1,
                    "objective": "Group 1",
                    "steps": [
                        {
                            "uuid": "d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b",
                            "position": 1,
                            "description": "Step 1",
                            "type": "change_directory",
                            "action": "/tmp"
                        },
                        {
                            "uuid": "d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b",
                            "position": 2,
                            "description": "Step 2",
                            "type": "create_directory",
                            "action": "/tmp/test"
                        }
                    ]
                },
                {
                    "uuid": "d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b",
                    "position": 2,
                    "objective": "Group 2",
                    "steps": [
                        {
                            "uuid": "d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b",
                            "position": 1,
                            "description": "Step 1",
                            "type": "read_object",
                            "action": "/tmp/test.txt"
                        },
                        {
                            "uuid": "d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b",
                            "position": 2,
                            "description": "Step 2",
                            "type": "write_object",
                            "action": "/tmp/test.txt"
                        }
                    ]
                }
            ]
        }
        "#;

        let plan = LogicalPlan::new(body).expect("Should be able to parse the body");

        assert_eq!(plan.groups.len(), 2);
        assert_eq!(plan.groups[0].steps.len(), 2);
        assert_eq!(plan.groups[1].steps.len(), 2);

        assert_eq!(
            plan.groups[0].uuid,
            Some(
                Uuid::parse_str("d1b3b3b4-0b3b-4b3b-8b3b-0b3b3b3b3b3b").expect("Should be a UUID")
            )
        );
    }
}
