/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::marker::PhantomData;
use tokio::runtime::{Builder, EnterGuard, Handle, Runtime as TokioRuntime};
use tokio::task::JoinHandle;

// Marker types for each runtime
pub struct MainRuntime;
pub struct ServerRuntime;
pub struct BackgroundRuntime;

// Wrapper for Handle with associated runtime type. This ensures that tasks spawned on the respective runtime are only awaited on the same runtime.
pub struct TypedHandle<T> {
    handle: Handle,
    _phantom: PhantomData<T>,
}

impl<T> TypedHandle<T> {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            _phantom: PhantomData,
        }
    }

    pub fn enter(&self) -> EnterGuard<'_> {
        self.handle.enter()
    }

    // Spawn a task and return a typed JoinHandle
    pub fn spawn<F>(&self, future: F) -> TypedJoinHandle<T, F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        TypedJoinHandle {
            inner: self.handle.spawn(future),
            _phantom: PhantomData,
        }
    }

    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }
}

// Wrapper for JoinHandle with associated runtime type
pub struct TypedJoinHandle<T, O> {
    inner: JoinHandle<O>,
    _phantom: PhantomData<T>,
}

impl<T, O> TypedJoinHandle<T, O> {
    pub fn block_on(self, handle: &TypedHandle<T>) -> Result<O, tokio::task::JoinError> {
        handle.handle.block_on(self.inner)
    }
}

pub struct TokioRuntimeManager {
    main: TokioRuntime,
    server: TokioRuntime,
    background: TokioRuntime,
}

/// A manager for creating and accessing tokio runtimes. This must be called from the main thread before any other Tokio runtimes are created.
///
/// This type doesn't implement Clone so we can control the lifetimes of the runtimes.
impl TokioRuntimeManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            main: get_runtime("tokio-spiced-main"),
            server: get_runtime("tokio-spiced-server"),
            background: get_runtime("tokio-spiced-background"),
        }
    }

    pub fn main(&self) -> TypedHandle<MainRuntime> {
        TypedHandle::new(self.main_raw().clone())
    }

    pub fn main_raw(&self) -> &Handle {
        self.main.handle()
    }

    pub fn server(&self) -> TypedHandle<ServerRuntime> {
        TypedHandle::new(self.server_raw().clone())
    }

    pub fn server_raw(&self) -> &Handle {
        self.server.handle()
    }

    pub fn background(&self) -> TypedHandle<BackgroundRuntime> {
        TypedHandle::new(self.background_raw().clone())
    }

    pub fn background_raw(&self) -> &Handle {
        self.background.handle()
    }
}

impl Default for TokioRuntimeManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a new tokio runtime with default multi-threaded configuration.
///
/// # Panics
///
/// Panics if the runtime fails to be created, usually due to resource exhaustion.
///
/// Panics if called from within an existing Tokio runtime.
fn get_runtime(thread_name: &str) -> TokioRuntime {
    // Check if we're already in a Tokio runtime and panic if so.
    assert!(
        Handle::try_current().is_err(),
        "cannot create tokio runtime from within an existing runtime"
    );

    match Builder::new_multi_thread()
        .enable_all()
        .thread_name(thread_name)
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => panic!("failed to create tokio runtime: {e}"),
    }
}
