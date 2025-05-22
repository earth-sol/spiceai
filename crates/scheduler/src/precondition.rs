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

use async_trait::async_trait;

#[async_trait]
pub trait Precondition: Send + Sync {
    /// Check if the precondition is met
    async fn check(&self) -> bool;

    /// Get the name of the precondition
    fn name(&self) -> &str;
}

#[cfg(test)]
mod test {

    use super::*;
    use async_trait::async_trait;
    use std::{
        collections::HashMap,
        sync::{Arc, LazyLock},
    };
    use tokio::sync::RwLock;
    use tracing_subscriber::EnvFilter;

    use crate::{
        Result, Scheduler, evaluators::ScheduleEvaluator, schedule::Schedule, tasks::ScheduledTask,
        tasks::TaskRequest,
    };

    fn init_tracing(default_level: Option<&str>) -> tracing::subscriber::DefaultGuard {
        let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
            (_, Some(log)) => EnvFilter::new(log),
            (Some(level), None) => EnvFilter::new(level),
            _ => EnvFilter::new("DEBUG"),
        };

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(filter)
            .with_ansi(true)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    struct TestEvaluator;

    impl Iterator for TestEvaluator {
        type Item = Arc<TaskRequest>;

        fn next(&mut self) -> Option<Self::Item> {
            Some(Arc::new(TaskRequest::from_secs(1)))
        }
    }

    impl ScheduleEvaluator for TestEvaluator {}

    static TEST_EXECUTION_COUNT: LazyLock<RwLock<HashMap<Arc<str>, usize>>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(Arc::from("test_precondition_fail"), 0);
        map.insert(Arc::from("test_precondition_pass"), 0);

        RwLock::new(map)
    });

    struct TestComponent {
        name: Arc<str>,
    }

    #[async_trait]
    impl ScheduledTask for TestComponent {
        async fn execute(&self) -> Result<()> {
            let mut map_lock = TEST_EXECUTION_COUNT.write().await;

            let count = map_lock
                .get_mut(self.name.as_ref())
                .expect("To get test execution count");
            *count += 1;

            Ok(())
        }
    }

    struct TestPrecondition {
        name: Arc<str>,
        should_pass: bool,
    }

    #[async_trait]
    impl Precondition for TestPrecondition {
        async fn check(&self) -> bool {
            self.should_pass
        }

        fn name(&self) -> &str {
            self.name.as_ref()
        }
    }

    #[tokio::test]
    async fn test_precondition_fail() {
        init_tracing(None);

        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(TestEvaluator {})))
            .add_precondition(Arc::new(TestPrecondition {
                name: "test_precondition_fail".into(),
                should_pass: false,
            }))
            .add_component(Arc::new(TestComponent {
                name: "test_precondition_fail".into(),
            }));

        let scheduler = Scheduler::new("test_precondition_fail".into(), vec![Arc::new(schedule)])
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
            .get("test_precondition_fail")
            .expect("To get test execution count");

        assert!(
            *count == 0,
            "Test component should have executed 0 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_precondition_pass() {
        init_tracing(None);

        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(TestEvaluator {})))
            .add_precondition(Arc::new(TestPrecondition {
                name: "test_precondition_pass".into(),
                should_pass: true,
            }))
            .add_component(Arc::new(TestComponent {
                name: "test_precondition_pass".into(),
            }));

        let scheduler = Scheduler::new("test_precondition_pass".into(), vec![Arc::new(schedule)])
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
            .get("test_precondition_pass")
            .expect("To get test execution count");

        assert!(
            *count == 2 || *count == 3,
            "Test component should have executed 2 or 3 times, but got {count}"
        );
    }
}
