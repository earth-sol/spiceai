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

    model.params.insert("system_prompt".to_string(), serde_json::Value::String("You are an agentic planner responsible to provide clear guidelines for the physical steps required to complete the task.
        Given a logical plan you are tasked to create updated physical plan to accomplish steps in the logical plan.

       ## Refine the plan by adding physical steps
        - add step to install Spice runtime using `curl https://install.spiceai.org | /bin/bash` shell command if plan provided includes Spice manipulation
        - use `run_shell_command` tool and cd 'folder_name' when asked to navigate to a specific folder
        - use `run_shell_command` and 'echo $?' action when asked to verify Check terminal output for a successful command such as login or similar

        ## Retrive available tools and update all steps by selecting one of the tool available. Must select a tool for each action.".to_string()));

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
        You are an agentic planner responsible to provide clear guidelines for the physical steps required to complete the task.
        Given a logical plan you are tasked to create updated physical plan to accomplish steps in the logical plan.

       ## Refine the plan by adding physical steps
        - add step to install Spice runtime using `curl https://install.spiceai.org | /bin/bash` shell command if plan provided includes Spice manipulation
        - use `run_shell_command` tool and cd 'folder_name' when asked to navigate to a specific folder
        - use `run_shell_command` and 'echo $?' action when asked to verify Check terminal output for a successful command such as login or similar

        ## Retrive available tools and update all steps by selecting one of the tool available. Must select a tool for each action.".to_string()));

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
