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

use std::any::Any;
use std::sync::Arc;

use crate::TaskRequest;

pub trait ScheduleEvaluator: Iterator<Item = Arc<TaskRequest>> + Any + Send + Sync {
    /// Whether this schedule evaluator can deliver a task request with a delivery policy of ``TaskDelivery::Immediate``.
    /// Evaluators with this property set to true are polled while a task is running, to determine if the task should be interrupted/cancelled.
    fn can_deliver_immediate_task(&self) -> bool {
        false
    }
}

#[allow(dead_code)]
pub struct CronSchedule(Arc<str>);

impl Iterator for CronSchedule {
    type Item = Arc<TaskRequest>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: implement cron schedule evaluation
        Some(Arc::new(TaskRequest::now()))
    }
}
impl ScheduleEvaluator for CronSchedule {}

pub struct IntervalSchedule(u64);

impl Iterator for IntervalSchedule {
    type Item = Arc<TaskRequest>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(Arc::new(TaskRequest::from_secs(self.0)))
    }
}

impl ScheduleEvaluator for IntervalSchedule {}

pub struct ManualInterrupt {
    rx: tokio::sync::mpsc::Receiver<Option<Arc<TaskRequest>>>,
}

impl ManualInterrupt {
    #[must_use]
    pub fn new(rx: tokio::sync::mpsc::Receiver<Option<Arc<TaskRequest>>>) -> Self {
        Self { rx }
    }
}

impl Iterator for ManualInterrupt {
    type Item = Arc<TaskRequest>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.try_recv() {
            Ok(Some(instant)) => Some(instant),
            Ok(None) => Some(Arc::new(TaskRequest::now().immediate())),
            Err(
                tokio::sync::mpsc::error::TryRecvError::Empty
                | tokio::sync::mpsc::error::TryRecvError::Disconnected,
            ) => None, // no interrupts to send
        }
    }
}
impl ScheduleEvaluator for ManualInterrupt {
    fn can_deliver_immediate_task(&self) -> bool {
        true
    }
}
