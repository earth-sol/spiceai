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

use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use evaluators::ScheduleEvaluator;
use schedule::Schedule;
use snafu::prelude::*;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Snafu)]
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod component;
pub mod evaluators;
pub mod schedule;

#[derive(Eq, PartialEq)]
pub enum TaskDelivery {
    Immediate,
    Queued,
}

pub struct TaskRequest {
    at: Instant,
    delivery: TaskDelivery,
    created_at: Instant,
}

impl TaskRequest {
    #[must_use]
    pub fn now() -> Self {
        let now = Instant::now();
        Self {
            at: now,
            delivery: TaskDelivery::Queued,
            created_at: now,
        }
    }

    #[must_use]
    pub fn from_secs(secs: u64) -> Self {
        let now = Instant::now();
        Self {
            at: now + Duration::from_secs(secs),
            delivery: TaskDelivery::Queued,
            created_at: now,
        }
    }

    #[must_use]
    pub fn immediate(mut self) -> Self {
        self.delivery = TaskDelivery::Immediate;
        self
    }
}

struct RunningTask {
    task_request: Arc<TaskRequest>,
    schedule: Arc<Schedule>,
    handle: JoinHandle<Result<()>>,
}

#[allow(dead_code)]
pub(crate) struct Scheduler {
    name: Arc<str>,
    schedules: Vec<Arc<Schedule>>,
    cancellation_token: Arc<CancellationToken>,
    evaluation_period: Duration,
}

impl Scheduler {
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn new(name: Arc<str>, schedules: Vec<Arc<Schedule>>) -> Self {
        Self {
            name,
            schedules,
            cancellation_token: Arc::new(CancellationToken::new()),
            evaluation_period: Duration::from_secs(60), // default 1 minute expression evaluation period
        }
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn with_evaluation_period(mut self, evaluation_period: Duration) -> Self {
        self.evaluation_period = evaluation_period;
        self
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn cancellation_token(&self) -> Arc<CancellationToken> {
        Arc::clone(&self.cancellation_token)
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn schedules(&self) -> Vec<Arc<Schedule>> {
        self.schedules.iter().map(Arc::clone).collect()
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn run(&self) -> JoinHandle<Result<()>> {
        let evaluation_period = self.evaluation_period;
        let cancellation_token = Arc::clone(&self.cancellation_token);
        let schedules = self.schedules();

        tokio::spawn(async move {
            // store a rolling map of the pending tasks
            let mut pending_tasks: HashMap<Schedule, BTreeMap<Instant, TaskRequest>> =
                HashMap::new();
            let mut running_tasks: HashMap<Schedule, RunningTask> = HashMap::new();

            loop {
                tokio::time::sleep(evaluation_period).await;
                if cancellation_token.is_cancelled() {
                    break;
                }

                let now = Instant::now();
                for schedule in &schedules {
                    if let Some(currently_running) = running_tasks.get(schedule) {
                        if currently_running.handle.is_finished() {
                            running_tasks.remove(schedule);
                        } else {
                            // the task is still running. evaluate any manual interrupts
                            for evaluator in schedule.evaluators() {
                                todo!()
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use std::sync::LazyLock;
    use tokio::sync::RwLock;

    use crate::component::ScheduleableComponent;

    use super::*;

    struct TestEvaluator;

    impl Iterator for TestEvaluator {
        type Item = TaskRequest;

        fn next(&mut self) -> Option<Self::Item> {
            Some(TaskRequest::from_secs(1))
        }
    }

    impl ScheduleEvaluator for TestEvaluator {}

    static TEST_EXECUTION_COUNT: LazyLock<RwLock<HashMap<Arc<str>, usize>>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(Arc::from("test_scheduler"), 0);
        map.insert(Arc::from("test_multi_schedule"), 0);
        RwLock::new(map)
    });

    struct TestComponent {
        name: Arc<str>,
    }

    #[async_trait]
    impl ScheduleableComponent for TestComponent {
        async fn execute(&self) -> Result<()> {
            let mut map_lock = TEST_EXECUTION_COUNT.write().await;

            let count = map_lock
                .get_mut(self.name.as_ref())
                .expect("To get test execution count");
            *count += 1;

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let schedule = Schedule::new(
            vec![Arc::new(TestEvaluator {})],
            vec![Arc::new(TestComponent {
                name: "test_scheduler".into(),
            })],
        );

        let scheduler = Scheduler::new("test_scheduler".into(), vec![Arc::new(schedule)])
            .with_evaluation_period(std::time::Duration::from_secs(1));
        let scheduler_handle = scheduler.run();

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

    #[tokio::test]
    async fn test_multi_schedule() {
        let schedule = Schedule::new(
            vec![Arc::new(TestEvaluator {})],
            vec![
                Arc::new(TestComponent {
                    name: "test_multi_schedule".into(),
                }),
                Arc::new(TestComponent {
                    name: "test_multi_schedule".into(),
                }),
            ],
        );

        let scheduler = Scheduler::new("test_multi_schedule".into(), vec![Arc::new(schedule)])
            .with_evaluation_period(std::time::Duration::from_secs(1));
        let scheduler_handle = scheduler.run();

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        scheduler.cancellation_token().cancel();
        scheduler_handle
            .await
            .expect("Should join handle")
            .expect("To finish the handle without error");

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_multi_schedule")
            .expect("To get test execution count");

        // 4-6 times, because of the sleep times and delay inaccuracies
        assert!(
            *count >= 4 && *count <= 6,
            "Test component should have executed 4-6 times, but got {count}"
        );
    }
}
