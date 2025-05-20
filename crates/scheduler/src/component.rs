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

use datafusion::sql::TableReference;
use runtime::Runtime;

use crate::Result;

#[derive(Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub(crate) enum ScheduleableComponent {
    Dataset(Arc<str>),
    Worker(Arc<str>),
    #[cfg(test)]
    TestComponent(Arc<str>),
}

impl ScheduleableComponent {
    /// Executes the defined component.
    #[allow(clippy::missing_errors_doc, dead_code)]
    pub(crate) async fn execute(&self, runtime: &Arc<Runtime>) -> Result<()> {
        match self {
            ScheduleableComponent::Dataset(dataset) => {
                // Implement the logic to refresh the dataset
                let app_lock = runtime.app();
                let app_lock = app_lock.read().await;
                let Some(app) = app_lock.as_ref() else {
                    todo!("Handle when app is not found");
                };

                let dataset = app.datasets.iter().find(|d| *d.name.as_str() == **dataset);
                let Some(dataset) = dataset else {
                    todo!("Handle when dataset is not found");
                };

                match runtime
                    .datafusion()
                    .refresh_table(&TableReference::parse_str(dataset.name.as_str()), None)
                    .await
                {
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
            ScheduleableComponent::Worker(_worker) => {
                // Implement the logic to execute the worker
                Ok(())
            }
            #[cfg(test)]
            ScheduleableComponent::TestComponent(test_component) => {
                self.execute_test_component(test_component).await;
                Ok(())
            }
        }
    }
}
