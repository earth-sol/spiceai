use serde_yaml;
use spicepod::component::model::Model;

#[must_use]
pub fn prompt_planner_model(orchestrator: Model) -> Model {
    let mut model = Model::new(orchestrator.from, "agentic_physical_prompt_planner");

    for param in orchestrator.params {
        model.params.insert(param.0, param.1);
    }

    let yaml_str = include_str!("prompt_response_format.yaml");
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

    model
        .params
        .insert("openai_response_format".to_string(), yaml_value);

    model.params.insert("system_prompt".to_string(), serde_json::Value::String("
        You are an agentic task planner. Given a logical plan step input, you are tasked to provide a prompt text to run with a chat completion against the specified model.

        Ensure the model you select is appropriate for the given task, and the prompt text is relevant to the task at hand.".to_string()));

    model
}

#[must_use]
pub fn tool_planner_model(orchestrator: Model) -> Model {
    let mut model = Model::new(orchestrator.from, "agentic_physical_tool_planner");

    for param in orchestrator.params {
        model.params.insert(param.0, param.1);
    }

    let yaml_str = include_str!("tool_response_format.yaml");
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

    model
        .params
        .insert("openai_response_format".to_string(), yaml_value);

    model.params.insert("system_prompt".to_string(), serde_json::Value::String("
        You are an agentic task planner. Given a logical plan step input, you are tasked to provide the tool required to accomplish the step.

        Before providing the tool response, ensure you retrieve the list of all available tools and verify the tool is valid and available for the task.
        You are responsible for generating the JSON body for the tool input, and the body should be referenced against the tool schema.


       ## Refining task steps
        - use `run_shell_command` tool and cd 'folder_name' when asked to navigate to a specific folder
        - use `run_shell_command` and 'echo $?' action when asked to verify Check terminal output for a successful command such as login or similar".to_string()));

    model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_physical_prompt_planner_model() {
        let orchestrator = Model::new("openai:gpt-4o", "orchestrator");
        let model = prompt_planner_model(orchestrator);
        assert_eq!(model.name, "agentic_physical_prompt_planner");
        assert_eq!(model.from, "openai:gpt-4o");
    }

    #[test]
    fn test_physical_tool_planner_model() {
        let orchestrator = Model::new("openai:gpt-4o", "orchestrator");
        let model = tool_planner_model(orchestrator);
        assert_eq!(model.name, "agentic_physical_tool_planner");
        assert_eq!(model.from, "openai:gpt-4o");
    }
}
