use std::{
    any::Any,
    fmt::Debug,
    mem::ManuallyDrop,
    panic::resume_unwind,
    pin::pin,
    ptr,
    task::{Context, Poll},
};

use crate::{
    queue::{Handle, TaskQueue},
    util::Receiver,
};

mod queue;
mod task;
#[cfg(test)]
mod tests;
mod util;

pub use queue::{Waker, WakerExt};

pub(crate) type Panic = Box<dyn Any + Send + 'static>;
pub(crate) type PanicResult<T> = Result<T, Panic>;

#[derive(Debug)]
pub struct Executor {
    queue: TaskQueue,
    config: ExecutorConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct ExecutorConfig {
    sync_queue_size: usize,
    local_queue_size: usize,
    max_interval: u32,
    extra: Option<fn() -> Box<dyn Any>>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            sync_queue_size: 64,
            local_queue_size: 63,
            max_interval: 32,
            extra: None,
        }
    }
}

impl Executor {
    pub fn new() -> Self {
        Self::with_config(ExecutorConfig::default())
    }

    pub fn with_config(config: ExecutorConfig) -> Self {
        Self {
            queue: TaskQueue::new(&config),
            config,
        }
    }

    pub fn block_on<F: Future + 'static>(&self, f: F) -> F::Output {
        let mut fut = pin!(f);
        let cx = &mut Context::from_waker(std::task::Waker::noop());
        loop {
            match fut.as_mut().poll(cx) {
                Poll::Ready(res) => return res,
                Poll::Pending => {
                    self.queue.flush_sync();
                    self.tick();
                }
            }
        }
    }

    pub fn tick(&self) {
        for id in self.queue.iter().take(self.config.max_interval as _) {
            self.queue.run(id);
        }
    }

    pub fn spawn<F: Future + 'static>(&self, f: F) -> JoinHandle<F::Output> {
        let (id, rx) = self.queue.push(f, self.config.extra.map(|f| f()));

        JoinHandle {
            handle: self.queue.handle(id),
            rx,
        }
    }
}

pub struct JoinHandle<T> {
    handle: Handle,
    rx: Receiver<PanicResult<T>>,
}

impl<T> Unpin for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    pub fn cancel(&self) {
        self.rx.set_canceled();
        self.handle.schedule();
    }

    pub fn is_canceled(&self) -> bool {
        self.rx.is_canceled()
    }

    pub fn detach(self) {
        let this = ManuallyDrop::new(self);
        _ = unsafe { ptr::read(&this.rx) };
    }
}

#[derive(Debug)]
pub enum JoinError {
    Canceled,
    Panicked(Panic),
}

pub trait ResumeUnwind {
    type Output;

    fn resume_unwind(self) -> Self::Output;
}

impl<T> ResumeUnwind for Result<T, JoinError> {
    type Output = Option<T>;

    fn resume_unwind(self) -> Self::Output {
        match self {
            Ok(res) => Some(res),
            Err(JoinError::Canceled) => None,
            Err(JoinError::Panicked(e)) => resume_unwind(e),
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let is_local = self.handle.is_local();
        let res = if is_local {
            unsafe { self.rx.poll_local(cx) }
        } else {
            self.rx.poll(cx)
        };
        match res {
            Poll::Pending => {
                if self.handle.schedule() {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(JoinError::Canceled))
                }
            }
            Poll::Ready(Some(Ok(res))) => Poll::Ready(Ok(res)),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Err(JoinError::Panicked(err))),
            Poll::Ready(None) => Poll::Ready(Err(JoinError::Canceled)),
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        self.cancel();
    }
}
