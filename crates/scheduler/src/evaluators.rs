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

use snafu::{OptionExt, ResultExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::tasks::{TaskDelivery, TaskRequest};
use crate::{Error, Result};

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

trait TaskRequestChannel: Send + Sync {
    fn needs_task_completion_notification(&self) -> bool;
    fn set_cancellation_token(&mut self, cancellation: Arc<CancellationToken>);
    fn set_task_completion_notification(&mut self, _notify: Arc<tokio::sync::Notify>) {}
    fn set_reset_notification(&mut self, notify: Arc<tokio::sync::Notify>);
    fn set_submission_channel(&mut self, tx: Arc<tokio::sync::mpsc::Sender<Arc<NewTaskRequest>>>);
    fn start(&self) -> Result<JoinHandle<Result<()>>>;
}

struct IntervalRequestChannel {
    cancellation: Option<Arc<CancellationToken>>,
    notify: Option<Arc<tokio::sync::Notify>>,
    reset: Option<Arc<tokio::sync::Notify>>,
    tx: Option<Arc<tokio::sync::mpsc::Sender<Arc<NewTaskRequest>>>>,
    interval: u64,
}

impl TaskRequestChannel for IntervalRequestChannel {
    fn needs_task_completion_notification(&self) -> bool {
        true
    }

    fn set_cancellation_token(&mut self, cancellation: Arc<CancellationToken>) {
        self.cancellation = Some(cancellation);
    }

    fn set_task_completion_notification(&mut self, notify: Arc<tokio::sync::Notify>) {
        self.notify = Some(notify);
    }

    fn set_reset_notification(&mut self, notify: Arc<tokio::sync::Notify>) {
        self.reset = Some(notify);
    }

    fn set_submission_channel(&mut self, tx: Arc<tokio::sync::mpsc::Sender<Arc<NewTaskRequest>>>) {
        self.tx = Some(tx);
    }

    fn start(&self) -> Result<JoinHandle<Result<()>>> {
        // cancellation token to cancel the background task
        let cancellation = self
            .cancellation
            .clone()
            .context(crate::CancellationTokenRequiredSnafu)?;
        // reset channel to advise the requestor to reset and wait for the next notification
        // e.g. another requestor has started a task, and the task is currently running
        let reset = self
            .reset
            .clone()
            .context(crate::NotificationChannelRequiredSnafu)?;
        // notification channel to notify the requestor that a task has been completed
        let notify = self
            .notify
            .clone()
            .context(crate::NotificationChannelRequiredSnafu)?;
        // request submission channel to send the request
        let tx = self
            .tx
            .clone()
            .context(crate::SubmissionChannelRequiredSnafu)?;
        let interval = self.interval;

        Ok(tokio::spawn(async move {
            let mut first_run = true;
            loop {
                if first_run {
                    first_run = false;
                } else {
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            tracing::debug!("Interval evaluator cancelled");
                            return Ok(());
                        }
                        () = reset.notified() => {
                            tracing::debug!("Interval evaluator reset");
                            continue;
                        }
                        () = notify.notified() => {
                            tracing::debug!("Interval evaluator notified");
                        }
                    }
                }

                tokio::select! {
                    () = cancellation.cancelled() => {
                        tracing::debug!("Interval evaluator cancelled");
                        return Ok(());
                    }
                    () = reset.notified() => {
                        tracing::debug!("Interval evaluator reset");
                        continue;
                    }
                    () = tokio::time::sleep(tokio::time::Duration::from_secs(interval)) => {
                        tracing::debug!("Interval evaluator interval elapsed");
                    }
                }

                tx.send(Arc::new(NewTaskRequest::now()))
                    .await
                    .context(crate::ChannelSendSnafu)?;
            }
        }))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewTaskRequest {
    pub at: Instant,
    pub delivery: TaskDelivery,
    pub created_at: Instant,
}

impl NewTaskRequest {
    #[must_use]
    pub fn now() -> Self {
        let now = Instant::now();
        Self {
            at: now,
            delivery: TaskDelivery::Queued,
            created_at: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_interval_request_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<NewTaskRequest>>(1);

        let cancellation = Arc::new(CancellationToken::new());
        let notify = Arc::new(tokio::sync::Notify::new());
        let reset = Arc::new(tokio::sync::Notify::new());

        let channel = IntervalRequestChannel {
            cancellation: Some(Arc::clone(&cancellation)),
            notify: Some(Arc::clone(&notify)),
            reset: Some(Arc::clone(&reset)),
            tx: Some(Arc::new(tx)),
            interval: 1,
        };

        let channel_handle = channel.start().expect("To start request channel");

        let now = Instant::now();
        let request = rx.recv().await.expect("To receive request");
        let elapsed = now.elapsed();
        let now = Instant::now();
        assert!(request.at <= now);
        assert!(request.at >= now.checked_sub(elapsed).expect("To subtract elapsed time"));
        assert!(elapsed.as_millis() >= 990 && elapsed.as_millis() <= 1010);
        assert!(request.delivery == TaskDelivery::Queued);

        // next request should wait for task notification
        tokio::select! {
            Some(_) = rx.recv() => {
                panic!("Should not receive next request");
            }
            () = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                // do nothing
            }
        }

        let now = Instant::now();
        notify.notify_one();
        let request = rx.recv().await.expect("To receive request");
        let elapsed = now.elapsed();
        let now = Instant::now();
        assert!(request.at <= now);
        assert!(request.at >= now.checked_sub(elapsed).expect("To subtract elapsed time"));
        assert!(elapsed.as_millis() >= 990 && elapsed.as_millis() <= 1010);
        assert!(request.delivery == TaskDelivery::Queued);

        cancellation.cancel();
        channel_handle
            .await
            .expect("To await channel handle")
            .expect("To end channel");
    }

    #[tokio::test]
    async fn test_multi_channel_requestors() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<NewTaskRequest>>(1);

        let cancellation = Arc::new(CancellationToken::new());
        let notify = Arc::new(tokio::sync::Notify::new());
        let reset = Arc::new(tokio::sync::Notify::new());

        let tx = Arc::new(tx);
        let channel_one = IntervalRequestChannel {
            cancellation: Some(Arc::clone(&cancellation)),
            notify: Some(Arc::clone(&notify)),
            reset: Some(Arc::clone(&reset)),
            tx: Some(Arc::clone(&tx)),
            interval: 1,
        };
        let channel_two = IntervalRequestChannel {
            cancellation: Some(Arc::clone(&cancellation)),
            notify: Some(Arc::clone(&notify)),
            reset: Some(Arc::clone(&reset)),
            tx: Some(Arc::clone(&tx)),
            interval: 1,
        };

        let handle_one = channel_one.start().expect("To start request channel");
        let handle_two = channel_two.start().expect("To start request channel");

        // each channel will send a first request, resulting in two requests at the 1st second mark
        let now = Instant::now();
        let request_one = rx.recv().await.expect("To receive request");
        let request_two = rx.recv().await.expect("To receive request");
        let elapsed = now.elapsed();
        let now = Instant::now();
        assert!(request_one.at <= now);
        assert!(request_one.at >= now.checked_sub(elapsed).expect("To subtract elapsed time"));
        assert!(request_two.at <= now);
        assert!(request_two.at >= now.checked_sub(elapsed).expect("To subtract elapsed time"));
        assert!(elapsed.as_millis() >= 990 && elapsed.as_millis() <= 1010);
        assert!(request_one.delivery == TaskDelivery::Queued);
        assert!(request_two.delivery == TaskDelivery::Queued);

        cancellation.cancel();
        handle_one
            .await
            .expect("To await channel handle")
            .expect("To end channel");

        handle_two
            .await
            .expect("To await channel handle")
            .expect("To end channel");
    }
}
