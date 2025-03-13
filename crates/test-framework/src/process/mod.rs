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

use anyhow::{Context, Result};
use futures::{
    channel::mpsc::{Receiver, Sender},
    stream::FusedStream,
    SinkExt,
};
use rand::Rng;
use regex::Regex;
use std::{
    future::Future,
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    sync::LazyLock,
    time::{Duration, Instant},
};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct MemoryReading {
    timestamp: Instant,
    memory_usage: f64,
}

pub struct Process {
    pid: Pid,
    memory_readings: Option<Vec<MemoryReading>>,
    abort_channel: Option<Sender<()>>,
    reading_handle: Option<JoinHandle<Result<Vec<MemoryReading>>>>,
}

impl Process {
    #[must_use]
    pub fn new(pid: Pid) -> Self {
        Self {
            pid,
            memory_readings: None,
            abort_channel: None,
            reading_handle: None,
        }
    }

    #[must_use]
    pub fn start_watching(mut self) -> Self {
        let (tx, rx) = futures::channel::mpsc::channel(100);
        self.abort_channel = Some(tx);

        self.reading_handle = Some(tokio::spawn(async move {
            let mut readings = Vec::new();
            loop {
                if rx.is_terminated() {
                    break;
                }

                let memory_usage = Self::memory_usage(self.pid)?;
                let memory_usage_gb =
                    f64::from(u32::try_from(memory_usage / 1024 / 1024)?) / 1024.0;
                let reading = MemoryReading {
                    timestamp: Instant::now(),
                    memory_usage: memory_usage_gb,
                };

                readings.push(reading);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            Ok(readings)
        }));

        self
    }

    #[must_use]
    pub async fn stop_watching(mut self) -> Result<Self> {
        self.abort_channel = None; // drop the channel to stop the task
        let results = self
            .reading_handle
            .context("No reading handle is available")?
            .await??;
        self.memory_readings = Some(results);
        self.reading_handle = None;
        Ok(self)
    }

    pub fn max_observed_memory(&self) -> Result<f64> {
        Ok(self
            .memory_readings
            .iter()
            .next()
            .cloned()
            .context("No memory readings are available")?
            .iter()
            .map(|reading| reading.memory_usage)
            .fold(0.0, f64::max))
    }

    /// Returns the memory usage in bytes for the process
    pub fn memory_usage(pid: Pid) -> Result<u64> {
        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);

        let Some(process) = system.process(pid) else {
            return Err(anyhow::anyhow!("Failed to get process"));
        };

        Ok(process.memory())
    }

    /// Show the memory usage of the spiced instance in GB
    /// Also returns the memory usage in GB as a float
    pub fn show_memory_usage(pid: Pid) -> Result<f64> {
        let memory_usage = Self::memory_usage(pid)?;
        // drop memory usage to MB as a u32 before converting to GB as a float
        // we don't really care about the fractional memory usage of KB/MB
        let memory_usage_gb = f64::from(u32::try_from(memory_usage / 1024 / 1024)?) / 1024.0;
        println!("Memory usage: {memory_usage_gb:.2} GB");

        Ok(memory_usage_gb)
    }
}
