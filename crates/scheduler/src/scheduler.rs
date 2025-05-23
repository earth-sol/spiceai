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

use tokio::{
    sync::{Notify, RwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    Result,
    evaluators::EvaluatorType,
    schedule::Schedule,
    tasks::{RunningTask, TaskDelivery, TaskRequest, TaskStatus},
};

type PendingTasks = HashMap<Arc<str>, Vec<Arc<TaskRequest>>>;
type RunningTasks = HashMap<Arc<str>, RunningTask>;

pub struct NotStarted {
    schedules: Vec<Arc<Schedule>>,
    evaluation_period: Duration,
    acceptable_window: Duration,
}

pub struct Running {
    schedules: Vec<Arc<Schedule>>,
    evaluation_period: Duration,
    acceptable_window: Duration,
    pending: Arc<RwLock<PendingTasks>>,
    active: Arc<RwLock<RunningTasks>>,
    cancellation_token: Arc<CancellationToken>,
    evaluation_handle: RwLock<Option<JoinHandle<Result<()>>>>,
    interrupt_handle: RwLock<Option<JoinHandle<Result<()>>>>,
    completion_handle: RwLock<Option<JoinHandle<Result<()>>>>,
    interrupt_notifier: Arc<Notify>,
    completion_notifier: Arc<Notify>,
}

pub struct Scheduler<T> {
    state: Arc<T>,
    name: Arc<str>,
}

impl Scheduler<NotStarted> {
    #[must_use]
    pub fn new(
        name: Arc<str>,
        schedules: Vec<Arc<Schedule>>,
        evaluation_period: Duration,
        acceptable_window: Duration,
    ) -> Self {
        Self {
            state: Arc::new(NotStarted {
                schedules,
                evaluation_period,
                acceptable_window,
            }),
            name,
        }
    }

    pub async fn start(self) -> Scheduler<Running> {
        let cancellation_token = Arc::new(CancellationToken::new());

        let active = Arc::new(RwLock::new(HashMap::new()));
        let pending = Arc::new(RwLock::new(HashMap::new()));

        Scheduler {
            state: Arc::new(Running {
                schedules: self.state.schedules.clone(),
                evaluation_period: self.state.evaluation_period,
                acceptable_window: self.state.acceptable_window,
                active,
                pending,
                cancellation_token,
                evaluation_handle: None.into(),
                interrupt_handle: None.into(),
                completion_handle: None.into(),
                interrupt_notifier: Arc::new(Notify::new()),
                completion_notifier: Arc::new(Notify::new()),
            }),
            name: self.name,
        }
        .start()
        .await
    }
}

impl Scheduler<Running> {
    pub async fn stop(self) {
        tracing::debug!("Scheduler is exiting");

        let cancellation_token = Arc::clone(&self.state.cancellation_token);
        cancellation_token.cancel();

        // cancel in-progress tasks
        for schedule in &self.state.schedules {
            if Self::check_running_task(&self.state.active, schedule).await == TaskStatus::Running {
                Self::abort_task(&self.state.active, schedule).await;
            }
        }

        tracing::debug!("Scheduler finished cancelling tasks");

        if let Some(handle) = self.state.evaluation_handle.write().await.take() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::debug!("Scheduler failed to stop: {e}");
                }
                Err(e) => {
                    tracing::debug!("Scheduler failed to stop: {e}");
                }
            }
        }

        tracing::debug!("Scheduler closed the evaluation handler");

        if let Some(handle) = self.state.interrupt_handle.write().await.take() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::debug!("Scheduler failed to stop: {e}");
                }
                Err(e) => {
                    tracing::debug!("Scheduler failed to stop: {e}");
                }
            }
        }

        tracing::debug!("Scheduler closed the interrupt handler");

        if let Some(handle) = self.state.completion_handle.write().await.take() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::debug!("Scheduler failed to stop: {e}");
                }
                Err(e) => {
                    tracing::debug!("Scheduler failed to stop: {e}");
                }
            }
        }

        tracing::debug!("Scheduler closed the completion handler");

        tracing::debug!("Scheduler has stopped");
    }

    async fn listen_for_interrupts(self) -> Self {
        let state = Arc::clone(&self.state);
        let cancellation_token = Arc::clone(&state.cancellation_token);
        let schedules = state.schedules.clone();

        // Spawn a task to listen for interrupts
        let handle = tokio::spawn(async move {
            loop {
                if cancellation_token.is_cancelled() {
                    break;
                }

                let mut stored_interrupt = false;

                for schedule in &schedules {
                    for evaluator_lock in schedule.evaluators() {
                        let evaluator = evaluator_lock.read().await;
                        if evaluator.evaluator_type() == EvaluatorType::Interrupt {
                            drop(evaluator); // upgrade the lock to a write
                            let mut evaluator = evaluator_lock.write().await;
                            if let Some(request) = evaluator.evaluate() {
                                Self::insert_new_request(&state.pending, schedule, request).await;
                                stored_interrupt = true;
                            }
                        }
                    }
                }

                if stored_interrupt {
                    state.interrupt_notifier.notify_one();
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            Ok(())
        });

        let mut interrupt_handle = self.state.interrupt_handle.write().await;
        *interrupt_handle = Some(handle);
        drop(interrupt_handle); // drop to free to return self
        self
    }

    /// Start a background task to listen for completed tasks.
    /// This allows tasks which complete faster than the evaluation period to be scheduled immediately for their next evaluation schedule.
    async fn listen_for_completed_tasks(self) -> Self {
        let state = Arc::clone(&self.state);
        let cancellation_token = Arc::clone(&state.cancellation_token);
        let schedules = state.schedules.clone();

        // Spawn a task to listen for completed tasks
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = state.completion_notifier.notified() => {},
                    () = cancellation_token.cancelled() => {
                        break;
                    }
                }

                for schedule in &schedules {
                    if let TaskStatus::Finished(evaluator_id) =
                        Self::check_running_task(&state.active, schedule).await
                    {
                        // task from this evaluator has completed
                        // find the next task in the schedule for this evaluator
                        for evaluator_lock in schedule.evaluators() {
                            let evaluator = evaluator_lock.read().await;
                            if evaluator.id() == evaluator_id {
                                drop(evaluator); // upgrade the lock to a write
                                let mut evaluator = evaluator_lock.write().await;
                                if let Some(request) = evaluator.evaluate() {
                                    tracing::debug!(
                                        "Got new task request from evaluator {}",
                                        evaluator.id()
                                    );
                                    Self::insert_new_request(&state.pending, schedule, request)
                                        .await;
                                }
                            }
                        }
                    }
                }
            }

            Ok(())
        });

        let mut evaluation_handle = self.state.evaluation_handle.write().await;
        *evaluation_handle = Some(handle);
        drop(evaluation_handle); // drop to free to return self
        self
    }

    async fn insert_new_request(
        pending_tasks_lock: &Arc<RwLock<PendingTasks>>,
        schedule: &Arc<Schedule>,
        request: Arc<TaskRequest>,
    ) {
        let mut pending_tasks = pending_tasks_lock.write().await;
        let schedule_id = schedule.id();
        let entry = pending_tasks.entry(schedule_id).or_insert_with(Vec::new);
        if !entry.contains(&request) {
            entry.push(request);
        }
    }

    fn time_difference_is_now(
        now: Instant,
        acceptable_window: Duration,
        compare_time: Instant,
    ) -> bool {
        let diff = compare_time.duration_since(now);
        diff <= acceptable_window // the time difference is either zero (now is past the compare time) or within the acceptable window
    }

    async fn abort_task(running_tasks_lock: &Arc<RwLock<RunningTasks>>, schedule: &Arc<Schedule>) {
        let mut running_tasks = running_tasks_lock.write().await;
        let schedule_id = schedule.id();
        let entry = running_tasks.remove(&schedule_id);
        if let Some(task) = entry {
            task.handle.abort();
            match task.consume_for_handle().await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::debug!("Scheduled task {schedule_id} failed: {e}");
                }
                Err(e) => {
                    tracing::debug!("Scheduled task {schedule_id} failed: {e}");
                }
            }
        }
    }

    async fn start_task(
        running_tasks_lock: &Arc<RwLock<RunningTasks>>,
        schedule: &Arc<Schedule>,
        task: &Arc<TaskRequest>,
        notifier: Arc<Notify>,
    ) {
        let mut running_tasks = running_tasks_lock.write().await;
        let schedule_id = schedule.id();
        let new_task = schedule.execute(task, notifier).await;
        if let Some(new_task) = new_task {
            running_tasks.insert(schedule_id, new_task);
        }
    }

    async fn reset_timed_evaluators(schedule: &Arc<Schedule>) {
        for evaluator_lock in schedule.evaluators() {
            let mut evaluator = evaluator_lock.write().await;
            if matches!(evaluator.evaluator_type(), EvaluatorType::Timed) {
                evaluator.reset();
            }
        }
    }

    async fn no_task_pending_for_evaluator(
        pending_tasks_lock: &Arc<RwLock<PendingTasks>>,
        schedule: &Arc<Schedule>,
        evaluator_id: Arc<Uuid>,
    ) -> bool {
        let pending_tasks = pending_tasks_lock.read().await;
        let schedule_id = schedule.id();
        let entry = pending_tasks.get(&schedule_id);
        if let Some(entry) = entry {
            entry.iter().all(|req| req.evaluator_id != evaluator_id)
        } else {
            true
        }
    }

    async fn check_running_task(
        running_tasks_lock: &Arc<RwLock<RunningTasks>>,
        schedule: &Arc<Schedule>,
    ) -> TaskStatus {
        let mut running_tasks = running_tasks_lock.write().await;
        let schedule_id = schedule.id();
        let entry = running_tasks.get(&schedule_id);
        if let Some(task) = entry {
            if task.is_finished() {
                // retry strategy to retry failed tasks?
                // remove the task from the running tasks
                let Some(task) = running_tasks.remove(&schedule_id) else {
                    return TaskStatus::NotStarted;
                };

                tracing::debug!("Scheduled task {schedule_id} finished");

                let evaluator_id = Arc::clone(&task.evaluator_id);
                match task.consume_for_handle().await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::debug!("Scheduled task {schedule_id} failed: {e}");
                    }
                    Err(e) => {
                        tracing::debug!("Scheduled task {schedule_id} failed: {e}");
                    }
                }

                TaskStatus::Finished(evaluator_id)
            } else {
                TaskStatus::Running
            }
        } else {
            TaskStatus::NotStarted
        }
    }

    async fn clear_pending_tasks(
        pending_tasks_lock: &Arc<RwLock<PendingTasks>>,
        schedule: &Arc<Schedule>,
    ) {
        tracing::debug!("Clearing pending tasks for schedule {}", schedule.id());
        let mut pending_tasks = pending_tasks_lock.write().await;
        let schedule_id = schedule.id();
        pending_tasks.remove(&schedule_id);
    }

    async fn find_now_task(
        now: Instant,
        acceptable_window: Duration,
        pending_tasks_lock: &Arc<RwLock<PendingTasks>>,
        schedule: &Arc<Schedule>,
    ) -> Option<Arc<TaskRequest>> {
        tracing::debug!("Finding a task to execute at now");
        let mut pending_tasks = pending_tasks_lock.write().await;
        let schedule_id = schedule.id();
        let entry = pending_tasks.entry(schedule_id).or_insert_with(Vec::new);
        let mut tasks: Vec<Arc<TaskRequest>> = entry
            .iter()
            .filter(|req| Self::time_difference_is_now(now, acceptable_window, req.at))
            .cloned()
            .collect();

        entry.retain(|req| !tasks.contains(req));
        drop(pending_tasks);

        tasks.pop() // only take one task if there are multiple to be executed within the same time period
    }

    async fn find_now_immediate_task(
        now: Instant,
        acceptable_window: Duration,
        pending_tasks_lock: &Arc<RwLock<PendingTasks>>,
        schedule: &Arc<Schedule>,
    ) -> Option<Arc<TaskRequest>> {
        tracing::debug!("Finding any immediate tasks to execute at now");
        let mut pending_tasks = pending_tasks_lock.write().await;
        let schedule_id = schedule.id();
        let entry = pending_tasks.entry(schedule_id).or_insert_with(Vec::new);
        let mut immediate_tasks: Vec<Arc<TaskRequest>> = entry
            .iter()
            .filter(|req| {
                req.is_immediate() && Self::time_difference_is_now(now, acceptable_window, req.at)
            })
            .cloned()
            .collect();

        // remove these tasks from the pending tasks
        entry.retain(|req| !immediate_tasks.contains(req));
        drop(pending_tasks);

        // If there are multiple immediate tasks, preference any with ImmediateAndClear
        if immediate_tasks.is_empty() {
            None
        } else {
            let mut immediate_and_clear_tasks: Vec<Arc<TaskRequest>> = immediate_tasks
                .iter()
                .filter(|req| req.delivery == TaskDelivery::ImmediateAndClear)
                .cloned()
                .collect();

            if immediate_and_clear_tasks.is_empty() {
                immediate_tasks.pop()
            } else {
                immediate_and_clear_tasks.pop()
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn start(self) -> Self {
        let state = Arc::clone(&self.state);
        let cancellation_token = Arc::clone(&self.state.cancellation_token);
        let schedules = self.state.schedules.clone();
        let evaluation_period = self.state.evaluation_period;
        let acceptable_window = self.state.acceptable_window;

        // Spawn a task to evaluate the schedules
        let handle = tokio::spawn(async move {
            let mut first_run = true;
            loop {
                if first_run {
                    first_run = false;
                } else {
                    tokio::select! {
                        () = state.interrupt_notifier.notified() => {
                            // Interrupt received, time to continue so we can process it
                        },
                        () = tokio::time::sleep(evaluation_period) => {
                            // Continue to the next iteration
                        },
                        () = cancellation_token.cancelled() => {
                            break;
                        }
                    }
                }

                for schedule in &schedules {
                    if cancellation_token.is_cancelled() {
                        break;
                    }

                    let now = Instant::now();
                    // pull the immediate task into scope to drop rwlock from the function
                    let immediate_task = Self::find_now_immediate_task(
                        now,
                        acceptable_window,
                        &state.pending,
                        schedule,
                    )
                    .await;

                    if let Some(task) = immediate_task {
                        if task.delivery == TaskDelivery::ImmediateAndClear {
                            Self::clear_pending_tasks(&state.pending, schedule).await;
                            Self::reset_timed_evaluators(schedule).await;
                        }

                        // execute the immediate task, stopping any in-progress task
                        if Self::check_running_task(&state.active, schedule).await
                            == TaskStatus::Running
                        {
                            Self::abort_task(&state.active, schedule).await;
                        }

                        Self::start_task(
                            &state.active,
                            schedule,
                            &task,
                            Arc::clone(&state.completion_notifier),
                        )
                        .await;
                    }

                    tracing::debug!("Checking for running task");
                    // if there is an in-progress task, do nothing and continue to the next schedule/evaluation period wait
                    if Self::check_running_task(&state.active, schedule).await
                        == TaskStatus::Running
                    {
                        tracing::debug!("Schedule has a running task");
                        continue;
                    }

                    if cancellation_token.is_cancelled() {
                        break;
                    }

                    // run the evaluators to populate the queue
                    for evaluator_lock in schedule.evaluators() {
                        let evaluator = evaluator_lock.read().await;
                        if matches!(
                            evaluator.evaluator_type(),
                            EvaluatorType::Timed | EvaluatorType::Sequential
                        ) && Self::no_task_pending_for_evaluator(
                            &state.pending,
                            schedule,
                            evaluator.id(),
                        )
                        .await
                        {
                            drop(evaluator);
                            let mut evaluator = evaluator_lock.write().await;
                            if let Some(request) = evaluator.evaluate() {
                                tracing::debug!(
                                    "Got new task request from evaluator {}",
                                    evaluator.id()
                                );
                                Self::insert_new_request(&state.pending, schedule, request).await;
                            }
                        }
                    }

                    if cancellation_token.is_cancelled() {
                        break;
                    }

                    if let Some(task) =
                        Self::find_now_task(now, acceptable_window, &state.pending, schedule).await
                    {
                        tracing::debug!("Found a task to start");
                        Self::start_task(
                            &state.active,
                            schedule,
                            &task,
                            Arc::clone(&state.completion_notifier),
                        )
                        .await;
                    }
                }
            }

            Ok(())
        });

        let mut evaluation_handle = self.state.evaluation_handle.write().await;
        *evaluation_handle = Some(handle);
        drop(evaluation_handle);
        self.listen_for_interrupts()
            .await
            .listen_for_completed_tasks()
            .await
    }
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use async_trait::async_trait;
    use tracing_subscriber::EnvFilter;

    use super::*;
    use crate::{
        evaluators::{Evaluator, IntervalEvaluator, ManualInterrupt},
        tasks::ScheduledTask,
    };

    // ========== Test Setup and Helpers ==========
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

    // ========== Tests ==========
    #[tokio::test]
    async fn test_scheduler() {
        let schedule = Schedule::new()
            .add_component(Arc::new(TestComponent {
                name: Arc::from("test_scheduler"),
            }))
            .add_evaluator(Arc::new(RwLock::new(IntervalEvaluator::new(1))));

        let scheduler = Scheduler::<NotStarted>::new(
            "test_scheduler".into(),
            vec![Arc::new(schedule)],
            Duration::from_secs(1),
            Duration::from_nanos(15000),
        );
        let scheduler = scheduler.start().await;

        tokio::time::sleep(Duration::from_secs(5)).await;
        scheduler.stop().await;

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_scheduler")
            .expect("To get test execution count");

        assert!(
            *count == 4 || *count == 5,
            "Test component should have executed 4 or 5 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_scheduler_timing() {
        init_tracing(None);
        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(IntervalEvaluator::new(1))))
            .add_component(Arc::new(TimedComponent {
                name: "test_scheduler_timing".into(),
            }));

        let scheduler = Scheduler::new(
            "test_scheduler_timing".into(),
            vec![Arc::new(schedule)],
            Duration::from_secs(1),
            Duration::from_nanos(15_000),
        );
        let scheduler = scheduler.start().await;

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        scheduler.stop().await;

        let map_lock = TIMING_MAP.read().await;
        let timings = map_lock
            .get("test_scheduler_timing")
            .expect("To get test timings");

        let mut diffs = Vec::new();
        for i in 1..timings.len() {
            let diff = timings[i].duration_since(timings[i - 1]);
            diffs.push(diff);
        }

        assert!(
            diffs.len() == 8 || diffs.len() == 9,
            "There should be more than 8 or 9 timing differences, but got {diffs:?}"
        );

        // check that each diff is roughly 1 seconds
        for diff in diffs {
            assert!(
                diff.as_millis() >= 990 && diff.as_millis() <= 1010,
                "Timing difference should be around 1 second, but got {diff:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_multi_component_schedule() {
        let schedule = Schedule::new()
            .add_component(Arc::new(TestComponent {
                name: Arc::from("test_multi_component_schedule"),
            }))
            .add_component(Arc::new(TestComponent {
                name: Arc::from("test_multi_component_schedule"),
            }))
            .add_evaluator(Arc::new(RwLock::new(IntervalEvaluator::new(1))));

        let scheduler = Scheduler::<NotStarted>::new(
            "test_multi_component_schedule".into(),
            vec![Arc::new(schedule)],
            Duration::from_secs(1),
            Duration::from_nanos(15000),
        );
        let scheduler = scheduler.start().await;

        tokio::time::sleep(Duration::from_secs(5)).await;
        scheduler.stop().await;

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_multi_component_schedule")
            .expect("To get test execution count");

        assert!(
            *count == 8 || *count == 10,
            "Test component should have executed 8 or 10 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_multi_schedule() {
        let schedule_one = Schedule::new()
            .add_component(Arc::new(TestComponent {
                name: Arc::from("test_multi_schedule"),
            }))
            .add_evaluator(Arc::new(RwLock::new(IntervalEvaluator::new(1))));
        let schedule_two = Schedule::new()
            .add_component(Arc::new(TestComponent {
                name: Arc::from("test_multi_schedule"),
            }))
            .add_evaluator(Arc::new(RwLock::new(IntervalEvaluator::new(1))));

        let scheduler = Scheduler::<NotStarted>::new(
            "test_multi_schedule".into(),
            vec![Arc::new(schedule_one), Arc::new(schedule_two)],
            Duration::from_secs(1),
            Duration::from_nanos(15000),
        );
        let scheduler = scheduler.start().await;

        tokio::time::sleep(Duration::from_secs(5)).await;
        scheduler.stop().await;

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_multi_schedule")
            .expect("To get test execution count");

        assert!(
            *count == 8 || *count == 10,
            "Test component should have executed 8 or 10 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_multi_evaluator_multi_component() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);

        let manual_interrupt = ManualInterrupt::new(rx);
        let manual_interrupt_id = Arc::clone(&manual_interrupt.id());
        let manual_interrupt_lock = Arc::new(RwLock::new(manual_interrupt));
        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(IntervalEvaluator::new(1))))
            .add_evaluator(manual_interrupt_lock)
            .add_component(Arc::new(TestComponent {
                name: "test_multi_evaluator_multi_component".into(),
            }))
            .add_component(Arc::new(TestComponent {
                name: "test_multi_evaluator_multi_component".into(),
            }));

        let scheduler = Scheduler::new(
            "test_multi_evaluator_multi_component".into(),
            vec![Arc::new(schedule)],
            Duration::from_secs(1),
            Duration::from_nanos(15000),
        );

        let scheduler = scheduler.start().await;

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        tx.send(Some(Arc::new(
            TaskRequest::now(manual_interrupt_id).immediate_clear(),
        )))
        .await
        .expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        scheduler.stop().await;

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_multi_evaluator_multi_component")
            .expect("To get test execution count");

        assert!(
            *count == 8 || *count == 10,
            "Test component should have executed 8 or 10 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_manual_interrupts() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);

        let manual_interrupt = ManualInterrupt::new(rx);
        let manual_interrupt_lock = Arc::new(RwLock::new(manual_interrupt));
        let schedule = Schedule::new()
            .add_evaluator(manual_interrupt_lock)
            .add_component(Arc::new(TestComponent {
                name: "test_manual_interrupts".into(),
            }));

        let scheduler = Scheduler::new(
            "test_manual_interrupts".into(),
            vec![Arc::new(schedule)],
            Duration::from_secs(1),
            Duration::from_nanos(15000),
        );

        let scheduler = scheduler.start().await;

        tx.send(None).await.expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        tx.send(None).await.expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        tx.send(None).await.expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        scheduler.stop().await;

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_manual_interrupts")
            .expect("To get test execution count");

        assert!(
            *count == 3,
            "Test component should have executed 3 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_manual_queued_with_interrupt() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);

        let manual_interrupt = ManualInterrupt::new(rx);
        let manual_interrupt_id = Arc::clone(&manual_interrupt.id());
        let manual_interrupt_lock = Arc::new(RwLock::new(manual_interrupt));
        let schedule = Schedule::new()
            .add_evaluator(manual_interrupt_lock)
            .add_component(Arc::new(TestComponent {
                name: "test_manual_queued_with_interrupt".into(),
            }));

        let scheduler = Scheduler::new(
            "test_manual_queued_with_interrupt".into(),
            vec![Arc::new(schedule)],
            Duration::from_secs(1),
            Duration::from_nanos(15000),
        );

        let scheduler = scheduler.start().await;

        tx.send(Some(Arc::new(TaskRequest::from_secs(
            Arc::clone(&manual_interrupt_id),
            1,
        ))))
        .await
        .expect("To send task request");

        tx.send(Some(Arc::new(TaskRequest::from_secs(
            Arc::clone(&manual_interrupt_id),
            2,
        ))))
        .await
        .expect("To send task request");

        tx.send(Some(Arc::new(TaskRequest::from_secs(
            Arc::clone(&manual_interrupt_id),
            3,
        ))))
        .await
        .expect("To send task request");

        tx.send(Some(Arc::new(TaskRequest::from_secs(
            Arc::clone(&manual_interrupt_id),
            4,
        ))))
        .await
        .expect("To send task request");

        tx.send(Some(Arc::new(TaskRequest::from_secs(
            Arc::clone(&manual_interrupt_id),
            5,
        ))))
        .await
        .expect("To send task request");

        tokio::time::sleep(std::time::Duration::from_secs(6)).await;

        scheduler.stop().await;

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
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);

        let manual_interrupt = ManualInterrupt::new(rx);
        let manual_interrupt_id = Arc::clone(&manual_interrupt.id());
        let manual_interrupt_lock = Arc::new(RwLock::new(manual_interrupt));
        let schedule = Schedule::new()
            .add_evaluator(Arc::new(RwLock::new(IntervalEvaluator::new(1))))
            .add_evaluator(manual_interrupt_lock)
            .add_component(Arc::new(LongComponent {
                name: "test_manual_queue_clears_after_immediate".into(),
                wait: 5,
            }));

        let scheduler = Scheduler::new(
            "test_manual_queue_clears_after_immediate".into(),
            vec![Arc::new(schedule)],
            Duration::from_secs(1),
            Duration::from_nanos(15000),
        );

        let scheduler = scheduler.start().await;

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        tx.send(Some(Arc::new(
            TaskRequest::now(manual_interrupt_id).immediate_clear(),
        )))
        .await
        .expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_secs(8)).await;

        scheduler.stop().await;

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
