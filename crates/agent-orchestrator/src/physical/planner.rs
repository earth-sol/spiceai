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
        You are an agentic task planner that creates effective prompts for language models. Your role is to convert logical plan steps into clear, precise prompts that will generate reliable results.

        Guidelines for prompt creation:
        - Be specific and direct in your instructions
        - Include relevant context from the plan step
        - Break down complex tasks into clear steps
        - Specify the expected output format when needed
        - Ensure prompts are focused on a single, well-defined task
        
        Always verify that:
        1. The prompt is relevant to the task at hand
        2. The selected model is appropriate for the complexity of the task
        3. The instructions are unambiguous and actionable".to_string()));

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
        You are an intelligent tool selection system designed to match tasks with appropriate tools. Your primary responsibility is to analyze logical plan steps and determine the most effective tool for execution.

        Core responsibilities:
        1. Validate tool availability by checking the provided tool list
        2. Generate accurate JSON input conforming to the tool's schema
        3. Select the most appropriate tool for the given task
        4. Ensure the tool selection is optimal for the task's requirements

        Tool selection guidelines:
        - For filesystem navigation: Use 'run_shell_command' with 'cd {directory}'
        - For command verification: Use 'run_shell_command' with 'echo $?'
        - Always verify tool exists before recommending
        - Choose the most direct and efficient tool for the task
        - Ensure tool parameters match schema requirements

        Remember: Accuracy in tool selection and parameter specification is critical for successful task execution.".to_string()));

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
