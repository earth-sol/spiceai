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
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::Result;

#[async_trait]
pub trait ScheduledTask: Send + Sync {
    /// Executes the defined component.
    async fn execute(&self) -> Result<()>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TaskDelivery {
    /// The task is scheduled for immediate execution, and clears the pending task queue
    ImmediateAndClear,
    /// The task is scheduled for immediate execution, but does not clear the pending task queue
    Immediate,
    /// The task is queued for execution at a specific time
    Queued,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRequest {
    pub at: Instant,
    pub delivery: TaskDelivery,
    pub created_at: Instant,
    pub evaluator_id: Arc<Uuid>, // the evaluator that created this task
}

impl TaskRequest {
    #[must_use]
    pub fn now(evaluator_id: Arc<Uuid>) -> Self {
        let now = Instant::now();
        Self {
            at: now,
            delivery: TaskDelivery::Queued,
            created_at: now,
            evaluator_id,
        }
    }

    #[must_use]
    pub fn from_secs(evaluator_id: Arc<Uuid>, secs: u64) -> Self {
        let now = Instant::now();
        Self {
            at: now + Duration::from_secs(secs),
            delivery: TaskDelivery::Queued,
            created_at: now,
            evaluator_id,
        }
    }

    #[must_use]
    pub fn immediate(mut self) -> Self {
        self.delivery = TaskDelivery::Immediate;
        self
    }

    #[must_use]
    pub fn is_immediate(&self) -> bool {
        matches!(
            self.delivery,
            TaskDelivery::Immediate | TaskDelivery::ImmediateAndClear
        )
    }

    #[must_use]
    pub fn immediate_clear(mut self) -> Self {
        self.delivery = TaskDelivery::ImmediateAndClear;
        self
    }
}

#[derive(Debug)]
pub(crate) struct RunningTask {
    pub(crate) evaluator_id: Arc<Uuid>,
    pub(crate) handle: JoinHandle<Result<()>>,
}

impl RunningTask {
    #[must_use]
    pub(crate) fn new(evaluator_id: Arc<Uuid>, handle: JoinHandle<Result<()>>) -> Self {
        Self {
            evaluator_id,
            handle,
        }
    }

    #[must_use]
    pub(crate) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl RunningTask {
    #[must_use]
    pub fn consume_for_handle(self) -> JoinHandle<Result<()>> {
        self.handle
    }
}

#[derive(PartialEq)]
pub(crate) enum TaskStatus {
    NotStarted,
    Running,
    Finished(Arc<Uuid>),
}
