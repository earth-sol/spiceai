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
use std::future;
use std::hash::Hash;
use std::{sync::Arc, time::Instant};

use async_trait::async_trait;

use crate::TaskRequest;

pub trait ScheduleEvaluator: Iterator<Item = TaskRequest> + Any + Send + Sync {
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>
    where
        Self: Sized,
    {
        self
    }
}

pub struct CronSchedule(Arc<str>);

impl Iterator for CronSchedule {
    type Item = TaskRequest;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: implement cron schedule evaluation
        Some(TaskRequest::now())
    }
}
impl ScheduleEvaluator for CronSchedule {}

pub struct IntervalSchedule(u64);

impl Iterator for IntervalSchedule {
    type Item = TaskRequest;

    fn next(&mut self) -> Option<Self::Item> {
        Some(TaskRequest::from_secs(self.0))
    }
}

impl ScheduleEvaluator for IntervalSchedule {}

pub struct ManualInterrupt {
    rx: tokio::sync::mpsc::Receiver<Option<TaskRequest>>,
}

impl Iterator for ManualInterrupt {
    type Item = TaskRequest;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.try_recv() {
            Ok(Some(instant)) => Some(instant),
            Ok(None) => Some(TaskRequest::now().immediate()),
            Err(
                tokio::sync::mpsc::error::TryRecvError::Empty
                | tokio::sync::mpsc::error::TryRecvError::Disconnected,
            ) => None, // no interrupts to send
        }
    }
}
impl ScheduleEvaluator for ManualInterrupt {}
