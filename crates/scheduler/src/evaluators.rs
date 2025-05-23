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
use std::time::Instant;

use uuid::Uuid;

use crate::tasks::TaskRequest;

#[derive(PartialEq)]
pub enum EvaluatorType {
    /// The evaluator returns the next request in the schedule, only if the current time is greater than the last request.
    /// E.g. `.next()` -> 1am, at 12:30am `.next()` -> 1am, at 1:00 am `.next()` -> 2am.
    Sequential,
    /// The evaluator returns requests based on a fixed interval, anchored from a specific time.
    /// E.g. `.next()` -> 1am, at 12:30am `.next()` -> 1:30am, at 1:00 am `.next()` -> 2:00am.
    /// Resetting a timed evaluator should reset the anchor time to the current time.
    Timed,
    /// The evaluator returns interrupt requests.
    /// Interrupts have the highest priority and should be delivered immediately.
    /// The scheduler should abort any in-progress evaluation wait timers and deliver the interrupt request.
    Interrupt,
}

pub trait Evaluator: Send + Sync {
    /// Returns a unique identifier for this evaluator.
    fn id(&self) -> Arc<Uuid>;
    /// Returns the type of this evaluator.
    fn evaluator_type(&self) -> EvaluatorType;
    /// Returns the next task request for this evaluator.
    fn evaluate(&mut self) -> Option<Arc<TaskRequest>>;
    /// Clears the internal state of this evaluator to reset it to its initial state.
    fn reset(&mut self);
}

pub struct IntervalEvaluator {
    id: Arc<Uuid>,
    interval: u64,
    last_evaluated: Option<Instant>,
}

impl IntervalEvaluator {
    #[must_use]
    pub fn new(interval: u64) -> Self {
        Self {
            id: Arc::new(Uuid::new_v4()),
            interval,
            last_evaluated: None,
        }
    }
}

impl Evaluator for IntervalEvaluator {
    fn evaluator_type(&self) -> EvaluatorType {
        EvaluatorType::Timed
    }

    fn id(&self) -> Arc<Uuid> {
        Arc::clone(&self.id)
    }

    fn evaluate(&mut self) -> Option<Arc<TaskRequest>> {
        let now = Instant::now();
        if let Some(last_evaluated) = self.last_evaluated {
            if now.duration_since(last_evaluated).as_secs() < self.interval {
                return None;
            }
        }

        self.last_evaluated = Some(now);
        Some(Arc::new(TaskRequest::from_secs(
            Arc::clone(&self.id),
            self.interval,
        )))
    }

    fn reset(&mut self) {
        self.last_evaluated = None;
    }
}

pub struct ManualInterrupt {
    id: Arc<Uuid>,
    rx: tokio::sync::mpsc::Receiver<Option<Arc<TaskRequest>>>,
}

impl ManualInterrupt {
    #[must_use]
    pub fn new(rx: tokio::sync::mpsc::Receiver<Option<Arc<TaskRequest>>>) -> Self {
        Self {
            id: Arc::new(Uuid::new_v4()),
            rx,
        }
    }
}

impl Evaluator for ManualInterrupt {
    fn evaluator_type(&self) -> EvaluatorType {
        EvaluatorType::Interrupt
    }

    fn id(&self) -> Arc<Uuid> {
        Arc::clone(&self.id)
    }

    fn evaluate(&mut self) -> Option<Arc<TaskRequest>> {
        match self.rx.try_recv() {
            Ok(Some(instant)) => Some(instant),
            Ok(None) => Some(Arc::new(TaskRequest::now(Arc::clone(&self.id)))),
            Err(
                tokio::sync::mpsc::error::TryRecvError::Empty
                | tokio::sync::mpsc::error::TryRecvError::Disconnected,
            ) => None, // no interrupts to send
        }
    }

    fn reset(&mut self) {}
}
