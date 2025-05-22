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
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use schedule::Schedule;
use snafu::prelude::*;
use tasks::{RunningTask, TaskDelivery, TaskRequest};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Snafu)]
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod evaluators;
pub mod precondition;
pub mod schedule;
pub mod tasks;

pub(crate) struct Scheduler {
    name: Arc<str>,
    schedules: Vec<Arc<Schedule>>,
    cancellation_token: Arc<CancellationToken>,
    /// How frequently should schedules be evaluated?
    /// This excludes evaluators which can deliver immediate tasks, which are evaluated every 500ms for new tasks.
    evaluation_period: Duration,
    /// What is an acceptable window for a task to be considered "now"?
    /// Defaults to 0.05ms
    acceptable_window: Duration,
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
            acceptable_window: Duration::from_nanos(50_000), // default 0.05ms acceptable window
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
    pub(crate) fn with_acceptable_window(mut self, acceptable_window: Duration) -> Self {
        self.acceptable_window = acceptable_window;
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
        let acceptable_window = self.acceptable_window;
        let evaluation_period = self.evaluation_period;
        let cancellation_token = Arc::clone(&self.cancellation_token);
        let schedules = self.schedules();

        tokio::spawn(async move {
            let mut pending_tasks: HashMap<Arc<str>, Vec<Arc<TaskRequest>>> = HashMap::new();
            let mut running_tasks: HashMap<Arc<str>, RunningTask> = HashMap::new();

            loop {
                if cancellation_token.is_cancelled() {
                    break;
                }

                tracing::debug!("Scheduler is waiting for next evaluation period");
                evaluation_period_wait(
                    &cancellation_token,
                    &schedules,
                    &mut running_tasks,
                    &mut pending_tasks,
                    evaluation_period,
                    acceptable_window,
                )
                .await;

                for schedule in &schedules {
                    let pending = pending_tasks.entry(schedule.id()).or_default();
                    // ensure pending tasks are sorted by their execution time, soonest first
                    pending.sort_by(|a, b| a.at.cmp(&b.at));

                    if let Some(running_task) = running_tasks.get(&schedule.id()) {
                        if running_task.handle.is_finished() {
                            // Add retry strategies for failed tasks?
                            tracing::debug!(
                                "Scheduled task completed for schedule: {}",
                                schedule.id()
                            );
                            running_tasks.remove(&schedule.id());
                        } else {
                            continue; // skip evaluation this schedule if the task is still running
                        }
                    }

                    let now = Instant::now();

                    // determine if any pending tasks are due
                    for task in pending.clone() {
                        if handle_new_task(
                            now,
                            pending,
                            &mut running_tasks,
                            Arc::clone(&task),
                            schedule,
                            acceptable_window,
                        )
                        .await
                        {
                            // The task was executed
                            pending.retain(|t| t != &task);
                        }
                    }

                    // evaluate new tasks
                    for evaluator in schedule.evaluators() {
                        let mut evaluator = evaluator.write().await;
                        if let Some(task) = evaluator.next() {
                            // if the same task is already scheduled, don't add it again
                            if pending.iter().any(|t| t == &task) {
                                continue;
                            }

                            handle_new_task(
                                now,
                                pending,
                                &mut running_tasks,
                                Arc::clone(&task),
                                schedule,
                                acceptable_window,
                            )
                            .await;
                        }
                    }
                }
            }

            // cancel any running tasks
            for (_, running_task) in running_tasks {
                running_task.handle.abort();
                match running_task.consume_for_handle().await {
                    Ok(Ok(()) | Err(_)) => {}
                    Err(e) => {
                        if !e.is_cancelled() {
                            // TODO: handle join panics?
                            tracing::error!("Scheduler task panicked: {e}");
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

/// While waiting for the next evaluation period, check for new immediate tasks.
/// This allows interrupting an evaluation period when a new immediate task arrives.
///
/// E.g. we wouldn't want to wait for an evaluation period of 5 minutes if a new immediate task arrives in 5 seconds.
async fn evaluation_period_wait(
    cancellation_token: &CancellationToken,
    schedules: &Vec<Arc<Schedule>>,
    running_tasks: &mut HashMap<Arc<str>, RunningTask>,
    pending_tasks: &mut HashMap<Arc<str>, Vec<Arc<TaskRequest>>>,
    evaluation_period: Duration,
    acceptable_window: Duration,
) {
    // after evaluating for immediate tasks, wait for the evaluation period
    let start = Instant::now();
    loop {
        if cancellation_token.is_cancelled() {
            break;
        }

        let now = Instant::now();
        if now > start + evaluation_period {
            break;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // check if a new immediate task has arrived while we were sleeping
        for schedule in schedules {
            let pending = pending_tasks.entry(schedule.id()).or_default();

            pending.sort_by(|a, b| a.at.cmp(&b.at));

            let new_task = if let Some(task) = pending
                .iter()
                .find(|task| task.is_immediate() && task.at.duration_since(now) < acceptable_window)
            {
                Some(Arc::clone(task))
            } else {
                let mut return_interrupt = None;
                for evaluator in schedule.evaluators() {
                    let mut evaluator = evaluator.write().await;
                    if !evaluator.can_deliver_immediate_task() {
                        continue;
                    }

                    let task = evaluator.next();
                    match task {
                        None => {}
                        Some(task) => {
                            // if the task is immediate for now, we can cancel the running task
                            if task.is_immediate()
                                && task.at.duration_since(now) < acceptable_window
                            {
                                return_interrupt = Some(task);
                                continue;
                            }

                            // otherwise, we can just add it to the pending tasks
                            pending.push(task);
                        }
                    }
                }

                return_interrupt
            };

            if let Some(task) = new_task {
                if handle_new_task(
                    now,
                    pending,
                    running_tasks,
                    Arc::clone(&task),
                    schedule,
                    acceptable_window,
                )
                .await
                {
                    // the task was executed
                    pending.retain(|t| t != &task);
                }
            }

            pending.sort_by(|a, b| a.at.cmp(&b.at));
        }
    }
}

/// Handles the execution of new tasks and the cancellation of running tasks if required.
///
/// 1. If the task is scheduled for immediate execution and is due, it will cancel any running tasks and execute the new one.
/// 2. If the task is scheduled for immediate execution but is not due, it will be added to the pending tasks.
/// 3. If the task is scheduled for queued execution and is due, it will be executed immediately unless there is already a task running - then it is added to the pending tasks.
/// 4. If the task is scheduled for queued execution and is not due, it will be added to the pending tasks.
///
/// Returns a boolean whether the task was queued for immediate execution.
async fn handle_new_task(
    now: Instant,
    pending_tasks: &mut Vec<Arc<TaskRequest>>,
    running_tasks: &mut HashMap<Arc<str>, RunningTask>,
    task_request: Arc<TaskRequest>,
    schedule: &Arc<Schedule>,
    acceptable_window: Duration,
) -> bool {
    match (
        task_request.delivery.clone(),
        task_request.at.duration_since(now) < acceptable_window,
        running_tasks.contains_key(&schedule.id()),
    ) {
        (TaskDelivery::Queued, true, false) => {
            // the task is scheduled due now, and nothing else is running
            // execute it immediately
            tracing::debug!("Executing queued task now");
            let Some(new_task) = schedule.execute().await else {
                return false;
            };

            running_tasks.insert(schedule.id(), new_task);
            true
        }

        (TaskDelivery::Queued, _, _)
        | (TaskDelivery::Immediate | TaskDelivery::ImmediateAndClear, false, _) => {
            // the task is scheduled for a future time, or there is already a task running
            tracing::debug!("Scheduling task for later");
            if !pending_tasks.contains(&task_request) {
                // if the task is not already pending, add it to the pending tasks
                pending_tasks.push(Arc::clone(&task_request));
            }
            false
        }

        (TaskDelivery::Immediate | TaskDelivery::ImmediateAndClear, true, _) => {
            // the task is scheduled for immediate and is due
            // cancel the running task and execute the new one
            tracing::debug!("Executing immediate task now");
            let Some(running_task) = running_tasks.remove(&schedule.id()) else {
                let Some(new_task) = schedule.execute().await else {
                    return false;
                };

                running_tasks.insert(schedule.id(), new_task);
                return true;
            };

            if running_task.handle.is_finished() {
                // TODO: retry strategy if the task failed?
                tracing::debug!("Scheduled task completed for schedule: {}", schedule.id());
            } else {
                tracing::debug!("Cancelling running task");
                running_task.handle.abort();
                match running_task.consume_for_handle().await {
                    Ok(Ok(()) | Err(_)) => {}
                    Err(e) => {
                        if !e.is_cancelled() {
                            // TODO: handle join panics?
                            tracing::error!("Scheduler task panicked: {e}");
                        }
                    }
                }
            }

            let Some(new_task) = schedule.execute().await else {
                return false;
            };

            running_tasks.insert(schedule.id(), new_task);

            if task_request.delivery == TaskDelivery::ImmediateAndClear {
                // clear the pending tasks
                tracing::debug!("Clearing pending tasks");
                pending_tasks.clear();
            }

            true
        }
    }
}

#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use std::sync::LazyLock;
    use tokio::sync::RwLock;
    use tracing_subscriber::EnvFilter;

    use crate::{
        evaluators::{ManualInterrupt, ScheduleEvaluator},
        tasks::ScheduledTask,
    };

    use super::*;

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
        map.insert(Arc::from("test_scheduler"), 0);
        map.insert(Arc::from("test_multi_schedule"), 0);
        map.insert(Arc::from("test_multi_component_schedule"), 0);
        map.insert(Arc::from("test_multi_evaluator_multi_component"), 0);
        map.insert(Arc::from("test_manual_interrupts"), 0);
        map.insert(Arc::from("test_manual_queued_with_interrupt"), 0);
        map.insert(Arc::from("test_manual_queue_clears_after_immediate"), 0);

        RwLock::new(map)
    });

    static TIMING_MAP: LazyLock<RwLock<HashMap<Arc<str>, Vec<Instant>>>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(Arc::from("test_scheduler_timing"), Vec::new());

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

    struct LongComponent {
        name: Arc<str>,
        wait: u64,
    }

    #[async_trait]
    impl ScheduledTask for LongComponent {
        async fn execute(&self) -> Result<()> {
            tokio::time::sleep(std::time::Duration::from_secs(self.wait)).await;

            let mut map_lock = TEST_EXECUTION_COUNT.write().await;

            let count = map_lock
                .get_mut(self.name.as_ref())
                .expect("To get test execution count");
            *count += 1;

            Ok(())
        }
    }

    struct TimedComponent {
        name: Arc<str>,
    }

    #[async_trait]
    impl ScheduledTask for TimedComponent {
        async fn execute(&self) -> Result<()> {
            let now = Instant::now();
            let mut map_lock = TIMING_MAP.write().await;

            let timings = map_lock
                .get_mut(self.name.as_ref())
                .expect("To get test execution count");
            timings.push(now);

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        init_tracing(None);
        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(TestEvaluator {})))
            .add_component(Arc::new(TestComponent {
                name: "test_scheduler".into(),
            }));

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

        // expect 2-3 times due to delay inaccuracies with sleep, duration, timing, etc
        assert!(
            *count == 2 || *count == 3,
            "Test component should have executed 2 or 3 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_scheduler_timing() {
        init_tracing(None);
        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(TestEvaluator {})))
            .add_component(Arc::new(TimedComponent {
                name: "test_scheduler_timing".into(),
            }));

        let scheduler = Scheduler::new("test_scheduler_timing".into(), vec![Arc::new(schedule)])
            .with_evaluation_period(std::time::Duration::from_secs(1));
        let scheduler_handle = scheduler.run();

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        scheduler.cancellation_token().cancel();
        scheduler_handle
            .await
            .expect("Should join handle")
            .expect("To finish the handle without error");

        let map_lock = TIMING_MAP.read().await;
        let timings = map_lock
            .get("test_scheduler_timing")
            .expect("To get test timings");

        // the evaluator has 1 second intervals
        // calculate the difference of timings between each key
        let mut diffs = Vec::new();
        for i in 1..timings.len() {
            let diff = timings[i].duration_since(timings[i - 1]);
            diffs.push(diff);
        }

        // there should be 7-8 diffs
        assert!(
            diffs.len() == 7 || diffs.len() == 8,
            "There should be more than 7 or 8 timing differences, but got {diffs:?}"
        );

        // check that each diff is roughly 1 second
        for diff in diffs {
            assert!(
                diff.as_millis() >= 950 && diff.as_millis() <= 1050,
                "Timing difference should be around 1 second, but got {diff:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_multi_component_schedule() {
        init_tracing(None);
        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(TestEvaluator {})))
            .add_component(Arc::new(TestComponent {
                name: "test_multi_component_schedule".into(),
            }))
            .add_component(Arc::new(TestComponent {
                name: "test_multi_component_schedule".into(),
            }));

        let scheduler = Scheduler::new(
            "test_multi_component_schedule".into(),
            vec![Arc::new(schedule)],
        )
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
            .get("test_multi_component_schedule")
            .expect("To get test execution count");

        // expect 4-6 times due to delay inaccuracies with sleep, duration, timing, etc
        assert!(
            *count >= 4 && *count <= 6,
            "Test component should have executed 4-6 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_multi_schedule() {
        init_tracing(None);
        let schedule_one = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(TestEvaluator {})))
            .add_component(Arc::new(TestComponent {
                name: "test_multi_schedule".into(),
            }));
        let schedule_two = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(TestEvaluator {})))
            .add_component(Arc::new(TestComponent {
                name: "test_multi_schedule".into(),
            }));

        let scheduler = Scheduler::new(
            "test_multi_schedule".into(),
            vec![Arc::new(schedule_one), Arc::new(schedule_two)],
        )
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

        // expect 4-6 times due to delay inaccuracies with sleep, duration, timing, etc
        assert!(
            *count >= 4 && *count <= 6,
            "Test component should have executed 4-6 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_multi_evaluator_multi_component() {
        init_tracing(None);
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);

        let manual_interrupt = Arc::new(RwLock::new(ManualInterrupt::new(rx)));

        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(TestEvaluator {})))
            .add_evaluator(manual_interrupt)
            .add_component(Arc::new(TestComponent {
                name: "test_multi_evaluator_multi_component".into(),
            }))
            .add_component(Arc::new(TestComponent {
                name: "test_multi_evaluator_multi_component".into(),
            }));

        let scheduler = Scheduler::new(
            "test_multi_evaluator_multi_component".into(),
            vec![Arc::new(schedule)],
        )
        .with_evaluation_period(std::time::Duration::from_secs(1));
        let scheduler_handle = scheduler.run();

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        tx.send(Some(Arc::new(TaskRequest::now().immediate())))
            .await
            .expect("To send task request");

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        scheduler.cancellation_token().cancel();
        scheduler_handle
            .await
            .expect("Should join handle")
            .expect("To finish the handle without error");

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_multi_evaluator_multi_component")
            .expect("To get test execution count");

        // expect 4-6 times due to delay inaccuracies with sleep, duration, timing, etc
        assert!(
            *count >= 4 && *count <= 6,
            "Test component should have executed 4-6 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_manual_interrupts() {
        init_tracing(None);
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);

        let manual_interrupt = Arc::new(RwLock::new(ManualInterrupt::new(rx)));

        let schedule = Schedule::new()
            .add_evaluator(manual_interrupt)
            .add_component(Arc::new(TestComponent {
                name: "test_manual_interrupts".into(),
            }));

        let scheduler = Scheduler::new("test_manual_interrupts".into(), vec![Arc::new(schedule)])
            .with_evaluation_period(std::time::Duration::from_secs(1));
        let scheduler_handle = scheduler.run();

        tx.send(None).await.expect("To send task request");

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        tx.send(None).await.expect("To send task request");

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        scheduler.cancellation_token().cancel();
        scheduler_handle
            .await
            .expect("Should join handle")
            .expect("To finish the handle without error");

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_manual_interrupts")
            .expect("To get test execution count");

        assert!(
            *count == 2,
            "Test component should have executed 2 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_manual_queued_with_interrupt() {
        init_tracing(None);
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);

        let manual_interrupt = Arc::new(RwLock::new(ManualInterrupt::new(rx)));

        let schedule = Schedule::new()
            .add_evaluator(manual_interrupt)
            .add_component(Arc::new(TestComponent {
                name: "test_manual_queued_with_interrupt".into(),
            }));

        let scheduler = Scheduler::new(
            "test_manual_queued_with_interrupt".into(),
            vec![Arc::new(schedule)],
        )
        .with_evaluation_period(std::time::Duration::from_secs(1));
        let scheduler_handle = scheduler.run();

        tx.send(Some(Arc::new(TaskRequest::from_secs(1))))
            .await
            .expect("To send task request");
        tx.send(Some(Arc::new(TaskRequest::from_secs(2))))
            .await
            .expect("To send task request");
        tx.send(Some(Arc::new(TaskRequest::from_secs(3))))
            .await
            .expect("To send task request");
        tx.send(Some(Arc::new(TaskRequest::from_secs(4))))
            .await
            .expect("To send task request");
        tx.send(Some(Arc::new(TaskRequest::from_secs(5))))
            .await
            .expect("To send task request");

        tokio::time::sleep(std::time::Duration::from_secs(6)).await;

        scheduler.cancellation_token().cancel();
        scheduler_handle
            .await
            .expect("Should join handle")
            .expect("To finish the handle without error");

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_manual_queued_with_interrupt")
            .expect("To get test execution count");

        assert!(
            *count == 5,
            "Test component should have executed 5 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_manual_queue_clears_after_immediate() {
        init_tracing(None);
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_interrupt = Arc::new(RwLock::new(ManualInterrupt::new(rx)));
        let (tx_clearer, rx_clearer) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_clearer = Arc::new(RwLock::new(ManualInterrupt::new(rx_clearer)));

        let schedule = Schedule::new()
            .add_evaluator(manual_interrupt)
            .add_evaluator(manual_clearer)
            .add_component(Arc::new(LongComponent {
                name: "test_manual_queue_clears_after_immediate".into(),
                wait: 5,
            }));

        let scheduler = Scheduler::new(
            "test_manual_queue_clears_after_immediate".into(),
            vec![Arc::new(schedule)],
        )
        .with_evaluation_period(std::time::Duration::from_secs(1));
        let scheduler_handle = scheduler.run();

        tx.send(Some(Arc::new(TaskRequest::from_secs(1))))
            .await
            .expect("To send task request");
        tx.send(Some(Arc::new(TaskRequest::from_secs(2))))
            .await
            .expect("To send task request");

        tokio::time::sleep(std::time::Duration::from_secs(2)).await; // wait for the queue to populate, and the task starts
        // otherwise, these requests will populate after the immediately executed task - because the immediate execution will start first, before anything reaches the queue
        // future de-dupe improvement? if immediate arrives, clear the queue and prevent entering the queue for x millis

        // so, with a populated queue - this task should abort the running task, and clear the queue
        tx_clearer
            .send(Some(Arc::new(TaskRequest::now().immediate_clear())))
            .await
            .expect("To send task request");

        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        scheduler.cancellation_token().cancel();
        scheduler_handle
            .await
            .expect("Should join handle")
            .expect("To finish the handle without error");

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_manual_queue_clears_after_immediate")
            .expect("To get test execution count");

        assert!(
            *count == 1,
            "Test component should have executed 1 times, but got {count}"
        );
    }
}
