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
use std::{any::Any, str::FromStr};

use snafu::ResultExt;

use crate::{Result, TaskRequest};

pub trait ScheduleEvaluator: Iterator<Item = Arc<TaskRequest>> + Any + Send + Sync {
    /// Whether this schedule evaluator can deliver a task request with a delivery policy of ``TaskDelivery::Immediate``.
    /// Evaluators with this property set to true are polled while a task is running, to determine if the task should be interrupted/cancelled.
    fn can_deliver_immediate_task(&self) -> bool {
        false
    }
}

#[allow(dead_code)]
pub struct CronSchedule<Z: chrono::offset::TimeZone + Send + Sync + 'static> {
    schedule: cron::Schedule,
    tz: Z,
}

impl<Z: chrono::offset::TimeZone + Send + Sync + 'static> CronSchedule<Z> {
    /// Creates a new `CronSchedule` from a cron expression.
    ///
    /// # Errors
    ///
    /// If the provided cron expression is invalid.
    pub fn new(cron: &Arc<str>, timezone: Z) -> Result<Self> {
        let schedule = cron::Schedule::from_str(cron).context(super::UnableToParseCronSnafu {
            cron: cron.to_string(),
        })?;

        Ok(CronSchedule {
            schedule,
            tz: timezone,
        })
    }
}

impl<Z: chrono::offset::TimeZone + Send + Sync + 'static> Iterator for CronSchedule<Z> {
    type Item = Arc<TaskRequest>;

    fn next(&mut self) -> Option<Self::Item> {
        let now = chrono::Utc::now().timestamp();
        let next_cron = self.schedule.upcoming(self.tz.clone()).next();
        match next_cron {
            Some(next) => {
                let ts = next.timestamp();
                let duration = ts - now;
                if duration < 0 {
                    Some(Arc::new(TaskRequest::now()))
                } else {
                    #[allow(clippy::cast_sign_loss)] // safety: duration is always positive
                    Some(Arc::new(TaskRequest::from_secs(duration as u64)))
                }
            }
            None => None,
        }
    }
}

impl<Z: chrono::offset::TimeZone + Send + Sync + 'static> ScheduleEvaluator for CronSchedule<Z> {}

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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use chrono::Timelike;

    use super::*;

    #[tokio::test]
    async fn test_manual_interrupt() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut manual_interrupt = ManualInterrupt::new(rx);

        // Send an interrupt
        let request = Arc::new(TaskRequest::now());

        tx.send(Some(Arc::clone(&request)))
            .await
            .expect("To send interrupt");

        // Receive the interrupt
        let interrupt = manual_interrupt.next().expect("To get interrupt");
        // Check that the interrupt is delivered
        assert_eq!(interrupt, request);

        // Check that the next call to next() returns None
        assert!(manual_interrupt.next().is_none());

        // Send a None interrupt
        tx.send(None).await.expect("To send None interrupt");

        // Receive the interrupt which is an automatic Immediate at current time
        let now = Instant::now();
        let interrupt = manual_interrupt.next().expect("To get None interrupt");
        // Check that the interrupt is delivered
        assert!(interrupt.at.duration_since(now) <= Duration::from_nanos(10000));
    }

    #[tokio::test]
    async fn test_interval_schedule() {
        let interval = 5;
        let mut schedule = IntervalSchedule(interval);

        let task = schedule.next().expect("To get task");
        assert!(
            (Instant::now() + Duration::from_secs(interval)).duration_since(task.at)
                <= Duration::from_nanos(10000)
        );

        tokio::time::sleep(Duration::from_secs(1)).await;

        let task = schedule.next().expect("To get task");
        assert!(
            (Instant::now() + Duration::from_secs(interval)).duration_since(task.at)
                <= Duration::from_nanos(10000)
        );
    }

    #[tokio::test]
    async fn test_cron_evaluator_every_5_seconds() {
        let cron = "*/5 * * * * ? *".into();
        let timezone = chrono::Utc;
        let mut schedule = CronSchedule::new(&cron, timezone).expect("To create cron schedule");
        let now = Instant::now();
        let chrono_now = chrono::Utc::now();
        let task = schedule.next().expect("To get task");
        let time_till = task.at.duration_since(now);
        let time = chrono_now + time_till;

        assert!(
            time.second() % 5 == 0 && task.at.duration_since(now) > Duration::from_secs(0),
            "Expected task to be scheduled in the future on the 5th second, but got: {:?}",
            time.second()
        );

        let last_duration = task.at.duration_since(now);

        // cron returns the next schedule in the series, so the .next() item will be the next schedule point
        let task = schedule.next().expect("To get task");
        let time_till = task.at.duration_since(now);
        let time = chrono_now + time_till;

        assert!(
            time.second() % 5 == 0 && task.at.duration_since(now) > last_duration,
            "Expected task to be scheduled in the future on the 5th second, but got: {:?}",
            time.second()
        );
    }

    #[tokio::test]
    async fn test_cron_evaluator_every_13_seconds() {
        let cron = "*/13 * * * * ? *".into();
        let timezone = chrono::Utc;
        let mut schedule = CronSchedule::new(&cron, timezone).expect("To create cron schedule");
        let now = Instant::now();
        let chrono_now = chrono::Utc::now();
        let task = schedule.next().expect("To get task");
        let time_till = task.at.duration_since(now);
        let time = chrono_now + time_till;

        assert!(
            time.second() % 13 == 0 && task.at.duration_since(now) > Duration::from_secs(0),
            "Expected task to be scheduled in the future on the 5th second, but got: {:?}",
            time.second()
        );

        let last_duration = task.at.duration_since(now);

        // cron returns the next schedule in the series, so the .next() item will be the next schedule point
        let task = schedule.next().expect("To get task");
        let time_till = task.at.duration_since(now);
        let time = chrono_now + time_till;

        assert!(
            time.second() % 13 == 0 && task.at.duration_since(now) > last_duration,
            "Expected task to be scheduled in the future on the 5th second, but got: {:?}",
            time.second()
        );
    }
}
