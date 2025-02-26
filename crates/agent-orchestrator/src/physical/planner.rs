use serde_yaml;
use spicepod::component::model::Model;

#[must_use]
pub fn prompt_planner_model(physical_planner: Model) -> Model {
    tracing::info!(
        "Initializing physical prompt planner model {}",
        physical_planner.name
    );

    let mut model = Model::new(physical_planner.from, "agentic_physical_prompt_planner");

    for param in physical_planner.params {
        model.params.insert(param.0, param.1);
    }

    let yaml_str = include_str!("prompt_response_format.yaml");
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

    model
        .params
        .insert("openai_response_format".to_string(), yaml_value);

    model.params.insert("system_prompt".to_string(), serde_json::Value::String("
        # Objective

        You are an agentic task planner that creates effective prompts for language models. Your role is to convert logical plan steps into clear, precise prompts that will generate reliable results.

        # Guidelines

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
pub fn tool_planner_model(physical_planner: Model) -> Model {
    tracing::info!(
        "Initializing physical tool planner model [{}]",
        physical_planner.name
    );

    let mut model = Model::new(physical_planner.from, "agentic_physical_tool_planner");

    for param in physical_planner.params {
        model.params.insert(param.0, param.1);
    }

    let yaml_str = include_str!("tool_response_format.yaml");
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

    model
        .params
        .insert("openai_response_format".to_string(), yaml_value);

    let tools_str = include_str!("tools.json");
    model.params.insert("system_prompt".to_string(), serde_json::Value::String(format!("
        # Objective

        You are an intelligent tool selection system designed to match tasks with appropriate tools.
        Your primary responsibility is to analyze logical plan steps and determine the most effective tool for execution.

        You should only select a tool from the list of available tools, or respond with 'unknown' if no suitable tool is found.

        # Responsibilities
        1. Validate tool availability by checking the provided tool list
        2. Generate accurate JSON input conforming to the tool's schema
        3. Select the most appropriate tool for the given task
        4. Ensure the tool selection is optimal for the task's requirements

        # Guidelines

        Guidelines for tool selection:
        - Prefer using terminal tool for shell commands execution
        - Always verify tool exists before recommending
        - Choose the most direct and efficient tool for the task
        - Ensure tool parameters match schema requirements
        
        If the logical plan step specifies an action where no sufficient tool is available, respond with the tool 'unknown'.
        Do not hallucinate about the completion of the task or tool. You are not responsible for running any tools, only planning the tools to run.
        You should only respond with the tool to execute, and the contents of the tool input.

        Remember: Accuracy in tool selection and parameter specification is critical for successful task execution.
        
        # Example plan conversion

        ## File Download
        In this example, a logical plan requesting the download of a file is converted into a shell command tool call using `curl`.

        <logical_plan>
        {{
          \"description\": \"Download the parquet file from the URL\",
          \"tags\": [\"download\", \"shell\"],
          \"action\": \"read_object\",
          \"input\": \"https://example.com/file.txt\"
        }}
        </logical_plan>

        <physical_plan>
        {{
            \"tool\": \"<tool name to run terminal command>\",
            \"body\": \"{{\\\"command\":\\\"curl -O https://example.com/file.txt\\\"}}\",
            \"target_model\": \"<target model to run the tool>\"
        }}
        </physical_plan>

        ## Directory Change
        In this example, a logical plan requesting a directory change is converted into a shell command tool call using `cd`.

        <logical_plan>
        {{
          \"description\": \"Change to the temporary directory\",
          \"tags\": [\"filesystem\", \"shell\"],
          \"action\": \"change_directory\",
          \"input\": \"/tmp\"
        }}
        </logical_plan>

        <physical_plan>
        {{
            \"tool\": \"<tool name to run terminal command>\",
            \"body\": \"{{\\\"command\\\":\\\"cd /tmp\\\"}}\",
            \"target_model\": \"<target model to run the tool>\"
        }}

        # Available Tools

        The following tools are available: {tools_str}")));

    model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_physical_prompt_planner_model() {
        let physical_planner = Model::new("openai:gpt-4o", "physical_planner");
        let model = prompt_planner_model(physical_planner);
        assert_eq!(model.name, "agentic_physical_prompt_planner");
        assert_eq!(model.from, "openai:gpt-4o");
    }

    #[test]
    fn test_physical_tool_planner_model() {
        let physical_planner = Model::new("openai:gpt-4o", "physical_planner");
        let model = tool_planner_model(physical_planner);
        assert_eq!(model.name, "agentic_physical_tool_planner");
        assert_eq!(model.from, "openai:gpt-4o");
    }
}
