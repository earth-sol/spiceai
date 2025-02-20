use serde_yaml;
use spicepod::component::model::Model;

#[must_use]
pub fn model(logical_planner: Model) -> Model {
    tracing::info!(
        "Initializing logical planner model [{}]",
        logical_planner.name
    );

    let mut model = Model::new(logical_planner.from, "agentic_logical_planner");

    for param in logical_planner.params {
        model.params.insert(param.0, param.1);
    }

    let yaml_str = include_str!("openai_response_format.yaml");
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

    model
        .params
        .insert("openai_response_format".to_string(), yaml_value);

    model.params.insert("system_prompt".to_string(), serde_json::Value::String(
        "You are an agentic task planner specializing in breaking down complex tasks into logical, executable steps.

        KNOWLEDGE GATHERING:
        - Use SQL queries and data source searches before planning to gather context
        - Searching should be done as pre-planning research, not included as plan steps

        PLAN STRUCTURE:
        - Organize steps into logical groups, each with a clear objective
        - Ensure steps are precise, actionable and necessary
        - Maintain sequential flow and dependencies between steps

        AVAILABLE STEP TYPES:
        - `change_directory`: Directory navigation
        - `create_directory`: Create directory structures
        - `read_object`: File/object content reading
        - `write_object`: File/object content writing
        - `execute_terminal`: Shell command execution (must be complete and valid)
        - `retrieve_metadata`: System/object metadata gathering
        - `validation`: State/data verification
        - `improvement`: System optimization proposals
        - `response`: User communication
        - `request_for_info`: User input requests
        - `other`: Miscellaneous actions

        GUIDELINES:
        - Prioritize efficiency and reliability
        - Verify prerequisites before dependent steps
        - Include error handling where appropriate
        - Provide clear success criteria for validation steps".to_string()));

    model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_planner_model() {
        let logical_planner = Model::new("openai:gpt-4o", "logical_planner");
        let model = model(logical_planner);
        assert_eq!(model.name, "agentic_logical_planner");
    }
}
