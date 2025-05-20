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

use std::{collections::HashMap, hash::Hash, sync::Arc, time::Instant};

use datafusion::sql::TableReference;
use runtime::Runtime;
use snafu::prelude::*;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Snafu)]
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Eq, PartialEq, Hash)]
pub enum ScheduleableComponent {
    Dataset(Arc<str>),
    Worker(Arc<str>),
    #[cfg(test)]
    TestComponent(Arc<str>),
}

impl ScheduleableComponent {
    /// Executes the defined component.
    #[allow(clippy::missing_errors_doc)]
    pub async fn execute(&self, runtime: &Arc<Runtime>) -> Result<()> {
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

pub trait ScheduleEvaluator: Hash + Eq + PartialEq + Send + Sync {
    fn evaluate(&self) -> Instant;
}

#[derive(Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub struct CronSchedule(Arc<str>);
impl ScheduleEvaluator for CronSchedule {
    fn evaluate(&self) -> Instant {
        // Implement the logic to evaluate the cron schedule
        Instant::now()
    }
}

#[derive(Eq, PartialEq, Hash)]
pub struct Schedule<T: ScheduleEvaluator> {
    evaluator: T,
    components: Vec<ScheduleableComponent>,
}

impl<T: ScheduleEvaluator> Schedule<T> {
    /// Executes the components defined by this schedule.
    ///
    /// # Errors
    ///
    /// - Only when the executor encounters an error while executing the component, not when the component itself fails.
    pub async fn execute(&self, runtime: &Arc<Runtime>) -> Result<()> {
        let mut failed_components = Vec::new();
        for component in &self.components {
            if let Err(e) = component.execute(runtime).await {
                failed_components.push(e);
            }
        }

        if !failed_components.is_empty() {
            // Log or handle the errors
        }

        Ok(())
    }
}

pub struct Scheduler<T: ScheduleEvaluator> {
    name: Arc<str>,
    schedules: Vec<Arc<Schedule<T>>>,
    cancellation_token: Arc<CancellationToken>,
}

impl<T: ScheduleEvaluator + 'static> Scheduler<T> {
    #[must_use]
    pub fn new(name: Arc<str>, schedules: Vec<Arc<Schedule<T>>>) -> Self {
        Self {
            name,
            schedules,
            cancellation_token: Arc::new(CancellationToken::new()),
        }
    }

    #[must_use]
    pub fn cancellation_token(&self) -> Arc<CancellationToken> {
        Arc::clone(&self.cancellation_token)
    }

    #[must_use]
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    #[must_use]
    pub fn schedules(&self) -> Vec<Arc<Schedule<T>>> {
        self.schedules.iter().map(Arc::clone).collect()
    }

    #[must_use]
    pub fn run(&self, runtime: &Arc<Runtime>) -> JoinHandle<Result<()>> {
        let runtime = Arc::clone(runtime);
        let cancellation_token = Arc::clone(&self.cancellation_token);
        let schedules = self.schedules();

        tokio::spawn(async move {
            // Implement the logic to run the scheduler
            let mut pending_tasks: HashMap<Arc<Schedule<T>>, Instant> = HashMap::new();

            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if cancellation_token.is_cancelled() {
                    break;
                }

                let now = Instant::now();
                for schedule in &schedules {
                    let next = schedule.evaluator.evaluate();
                    if let Some(pending_run) = pending_tasks.get(schedule) {
                        if *pending_run != next {
                            // The next run time has changed, check if the current time is past the pending run time
                            if now >= *pending_run || now >= next {
                                // Execute the schedule
                                if let Err(_e) = schedule.execute(&runtime).await {
                                    todo!()
                                }
                                if now >= next {
                                    // If the current time is past the next run time, remove the task to force a reschedule
                                    pending_tasks.remove(schedule);
                                } else {
                                    pending_tasks.insert(Arc::clone(schedule), next);
                                }
                            }
                        }
                    } else {
                        // Schedule the first run
                        pending_tasks.insert(Arc::clone(schedule), next);
                    }
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;
    use tokio::sync::RwLock;

    use super::*;

    #[derive(Eq, PartialEq, Hash)]
    struct TestEvaluator;
    impl ScheduleEvaluator for TestEvaluator {
        fn evaluate(&self) -> Instant {
            Instant::now() + std::time::Duration::from_secs(1)
        }
    }

    static TEST_EXECUTION_COUNT: LazyLock<RwLock<HashMap<Arc<str>, usize>>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(Arc::from("test_scheduler"), 0);
        RwLock::new(map)
    });

    impl ScheduleableComponent {
        #[allow(clippy::missing_panics_doc)]
        pub async fn execute_test_component(&self, name: &Arc<str>) {
            let mut map_lock = TEST_EXECUTION_COUNT.write().await;

            let count = map_lock.get_mut(name).expect("To get test execution count");
            *count += 1;
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let runtime = Arc::new(Runtime::builder().build().await);

        let schedule = Schedule {
            evaluator: TestEvaluator {},
            components: vec![ScheduleableComponent::TestComponent(
                "test_scheduler".into(),
            )],
        };

        let scheduler = Scheduler::new("test_scheduler".into(), vec![Arc::new(schedule)]);
        let scheduler_handle = scheduler.run(&runtime);

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        scheduler.cancellation_token().cancel();
        scheduler_handle
            .await
            .expect("Should join handle")
            .expect("To finish the handle without error");

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_scheduler")
            .expect("To get test execution count");

        // 2 or 3 times, because of the sleep times and delay inaccuracies
        assert!(
            *count == 2 || *count == 3,
            "Test component should have executed 2 or 3 times, but got {count}"
        );
    }
}
