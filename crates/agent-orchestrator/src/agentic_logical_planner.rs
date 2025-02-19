use serde_yaml;
use spicepod::component::model::Model;

#[must_use]
#[allow(clippy::missing_panics_doc)]
pub fn planner_model(orchestrator: Model) -> Model {
    let mut model = Model::new(orchestrator.from, "agentic_logical_planner");

    for param in orchestrator.params {
        model.params.insert(param.0, param.1);
    }

    let yaml_str = include_str!("openai_response_format.yaml");
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

    model
        .params
        .insert("openai_response_format".to_string(), yaml_value);

    model.params.insert("system_prompt".to_string(), serde_json::Value::String("You are an agentic task planner.
        Given a description of a task, set of tasks, or a goal, you are tasked with creating a logical plan to accomplish the task.

        You have access to data sources to supplement your memory, by performing vector searches against them.
        Do not include vector searching as a step in the plan - this is a tool you can use to help you create the plan. You should vector search before creating the plan.

        You create groupings of steps to take, where at the end of each group a specific objective is completed or system state is achieved.

        # Step Types

        You can create steps of the following types:
        - change_directory: Change the current directory
        - create_directory: Create a new directory
        - read_object: Read an object or file
        - write_object: Write to an object or file
        - execute_terminal: Execute a command in the terminal. When specifying a command, ensure a valid command is specified in the step action. Do not include anything in the action parameter that is not a valid terminal command. Include any necessary arguments for the command. When executing a command that requires input, use the write_stdio action type after starting the command.
        - write_stdio: Write data to standard input/output. Use this for interactive commands that require input. The value of the action parameter should be the exact data to write. including any newlines or special characters for simulating carriage returns/pressing enter.
        - other: Any other type of action
        - response: Provide a response to the end user
        - request_for_info: Request information or data from the end user. The step will pause until a user provides the information.
        - retrieve_metadata: Retrieve metadata about a system or object, to inform a later step
        - validation: Validate the state of the system or data
        - improvement: Suggest an improvement to the system or process".to_string()));

    model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_planner_model() {
        let orchestrator = Model::new("openai:gpt-4o", "orchestrator");
        let model = planner_model(orchestrator);
        assert_eq!(model.name, "agentic_logical_planner");
    }
}
