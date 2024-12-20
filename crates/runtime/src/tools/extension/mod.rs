/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#[cfg(feature = "extensions_terminal")]
pub mod terminal;
#[cfg(feature = "extensions_terminal")]
use terminal::TerminalTool;

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use secrecy::SecretString;
use spicepod::component::tool::Tool;

use super::{catalog::SpiceToolCatalog, factory::ToolFactory, SpiceModelTool};

/// Holds all tools, defined in spiced, that are not automatically loaded with `spice_tools: auto`.
/// These tools can still be accessed by the user by specifying in the `params.tools` field of the model.
pub struct ExtensionToolCatalog {}
impl ExtensionToolCatalog {
    #[allow(unused_variables)]
    fn get_tool(
        id: &str,
        name: Option<&str>,
        description: Option<&str>,
    ) -> Option<Arc<dyn SpiceModelTool>> {
        let name = name.unwrap_or(id);
        match id {
            #[cfg(feature = "extensions_terminal")]
            "terminal" => Some(Arc::new(TerminalTool::new(name, description))),
            _ => None,
        }
    }
}
#[async_trait]
impl SpiceToolCatalog for ExtensionToolCatalog {
    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        vec![]
    }

    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        ExtensionToolCatalog::get_tool(name, Some(name), None)
    }

    fn name(&self) -> &str {
        "extensions"
    }
}

impl ToolFactory for ExtensionToolCatalog {
    fn construct(
        &self,
        component: &Tool,
        _params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(("extensions", id)) = component.from.split_once(':') else {
            return Err(format!(
                "Invalid component `from` field. Expected: `extensions:<tool_id>`. Error: {}",
                component.from
            )
            .into());
        };

        Self::get_tool(
            id,
            Some(component.name.as_str()),
            component.description.as_deref(),
        )
        .ok_or_else(|| format!("Tool with id `{id}` not found in extensions tool catalog").into())
    }
}
