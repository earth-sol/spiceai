/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use async_trait::async_trait;
use runtime::{Runtime, component::dataset::Dataset};

use crate::Result;

#[async_trait]
pub(crate) trait ScheduleableComponent: Send + Sync {
    /// Executes the defined component.
    ///
    /// # Errors
    ///
    /// - Only when the executor encounters an error while executing the component, not when the component itself fails.
    async fn execute(&self, runtime: &Arc<Runtime>) -> Result<()>;
}

#[async_trait]
impl ScheduleableComponent for Arc<Dataset> {
    async fn execute(&self, runtime: &Arc<Runtime>) -> Result<()> {
        match runtime.datafusion().refresh_table(&self.name, None).await {
            Ok(()) => {
                // Successfully refreshed the dataset
            }
            Err(e) => {
                // Handle the error
                todo!("Handle when refresh fails: {e}");
            }
        }

        Ok(())
    }
}
