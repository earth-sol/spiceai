use serde_yaml;
use spicepod::component::model::Model;

#[must_use]
pub fn model(orchestrator: Model) -> Model {
    let mut model = Model::new(orchestrator.from, "agentic_physical_planner");

    for param in orchestrator.params {
        model.params.insert(param.0, param.1);
    }

    let yaml_str = include_str!("tool_response_format.yaml");
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).expect("Failed to parse YAML");

    model
        .params
        .insert("openai_response_format".to_string(), yaml_value);

    model.params.insert("system_prompt".to_string(), serde_json::Value::String("You are an agentic planner responsible to provide clear guidelines for the physical steps required to complete the task.
        Given a logical plan you are tasked to create updated physical plan to accomplish steps in the logical plan.

        ## Refine the plan by adding physical steps if
        - add step to install Spice runtime using `curl https://install.spiceai.org | /bin/bash` shell command if plan provided includes Spice manipulation
        - use `run_shell_command` tool and cd 'folder_name' when asked to navigate to a specific folder
        - use `run_shell_command` and 'echo $?' action when asked to verify Check terminal output for a successful command such as login or similar

        ## Update steps by selecting one of the tools from the list below or use 'unavailable' if the tool was not identified:
        - 'Puppeteer': Browser automation and web scraping.
        - 'Fetch': Web content fetching and conversion for efficient LLM usage.
        - 'git': Read, search, and manipulate Git repositories.
        - 'create_directory': Create a new directory or ensure a directory exists, useful for setting up directory structures for projects.
        - 'directory_tree': Get a recursive tree view of files and directories in a JSON structure.
        - 'edit_file': Make line-based edits to a text file, providing a git-style diff of the changes.
        - 'fetch': Download file content from a specified URL.
        - 'get_file_info': Retrieve detailed metadata about a file or directory.
        - 'list_allowed_directories': List directories that the server is allowed to access.
        - 'list_directory': Get a detailed listing of all files and directories in a specified path.
        - 'move_file': Move or rename files and directories within allowed directories.
        - 'read_file': Read the complete contents of a specified file.
        - 'read_multiple_files': Read the contents of multiple files simultaneously.
        - 'run_shell_command': Run a shell command and return its output.
        - 'search_files': Recursively search for files and directories matching a pattern.
        - 'iterm-mcp::write_to_terminal' - writes to the active terminal terminal,  and execute shell command
        - 'iterm-mcp::read_terminal_output' - to read nd monitor output form the terminal and.
        - 'iterm-mcp::send_control_character' - to send a control character to the activeterminal and for write_stdio type actions
        - 'write_file': create a new file or completely overwrite an existing file with new content.".to_string()));

    model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_physical_planner_model() {
        let orchestrator = Model::new("openai:gpt-4o", "orchestrator");
        let model = model(orchestrator);
        assert_eq!(model.name, "agentic_physical_planner");
    }
}
