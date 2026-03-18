use std::{
    any::Any,
    mem::ManuallyDrop,
    sync::{Arc, Weak},
    task::{RawWaker, RawWakerVTable},
};

use crossfire::{MTx, Rx, mpsc::Array};
use slotmap::new_key_type;

use crate::{
    ExecutorConfig, PanicResult,
    task::Task,
    util::{Receiver, SendWrapper, SlotQueue, oneshot},
};

new_key_type! { pub struct TaskId; }

struct Shared {
    queue: SendWrapper<SlotQueue<TaskId, Task>>,
    sync_tx: MTx<Array<TaskId>>,
    sync_rx: Rx<Array<TaskId>>,
}

pub struct TaskQueue {
    shared: Arc<Shared>,
    _marker: std::marker::PhantomData<*const ()>,
}

#[derive(Debug)]
pub struct Handle {
    id: TaskId,
    shared: Weak<Shared>,
}

struct WakerInner {
    handle: Handle,
    extra: Option<Box<dyn Any>>,
}

pub struct Waker(Arc<WakerInner>);

impl TaskQueue {
    pub fn new(config: &ExecutorConfig) -> Self {
        let (sync_tx, sync_rx) = crossfire::mpsc::bounded_blocking(config.sync_queue_size);
        let inner = Shared {
            queue: SendWrapper::new(SlotQueue::new(config.local_queue_size)),
            sync_tx,
            sync_rx,
        };
        Self {
            shared: Arc::new(inner),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn handle(&self, id: TaskId) -> Handle {
        Handle {
            id,
            shared: Arc::downgrade(&self.shared),
        }
    }

    pub fn waker(&self, id: TaskId) -> Waker {
        let inner = WakerInner {
            handle: self.handle(id),
            extra: None,
        };
        Waker(Arc::new(inner))
    }

    pub fn flush_sync(&self) {
        let queue = self.queue();
        while let Ok(id) = self.shared.sync_rx.try_recv() {
            queue.make_hot(id);
        }
    }

    fn queue(&self) -> &SlotQueue<TaskId, Task> {
        // SAFETY: TaskQueue is !Send and !Sync
        unsafe { &self.shared.queue.get_unchecked() }
    }

    pub fn iter(&self) -> TaskIter<'_> {
        TaskIter {
            queue: self.queue(),
            head: self.queue().hot_head(),
            is_hot: true,
        }
    }

    pub fn push<F: Future + 'static>(&self, fut: F) -> (TaskId, Receiver<PanicResult<F::Output>>) {
        let (tx, rx) = oneshot();
        let id = self.queue().push_back_with(|id| {
            let waker = self.waker(id);
            let task = Task::new(fut, tx, waker);
            task
        });
        (id, rx)
    }

    pub fn run(&self, id: TaskId) {
        let queue = self.queue();

        let inner = match unsafe { queue.get(id) } {
            Some(task) => task.take().expect("Inner was not reset"),
            None => return,
        };

        queue.make_cold(id);
        match inner.poll() {
            Some(inner) => {
                unsafe { queue.get(id) }
                    .expect("Task removed during run")
                    .reset(inner);
            }
            None => {
                queue.remove(id);
            }
        }
    }
}

impl Shared {
    fn schedule(&self, id: TaskId) -> bool {
        if let Some(local) = self.queue.get() {
            // piggyback multi-thread wake-ups
            while let Ok(id) = self.sync_rx.try_recv() {
                local.make_hot(id);
            }
            local.make_hot(id);
            true
        } else {
            self.sync_tx.send(id).is_ok()
        }
    }
}

impl Handle {
    /// Enqueues the task for execution.
    ///
    /// Returns `true` if the task was enqueued successfully, or `false`
    /// otherwise, due to either executor or the task being dropped.
    pub fn schedule(&self) -> bool {
        let Some(queue) = self.shared.upgrade() else {
            // Executor has been dropped
            return false;
        };
        queue.schedule(self.id)
    }

    pub fn is_local(&self) -> bool {
        self.shared.upgrade().is_some_and(|q| q.queue.valid())
    }
}

impl Waker {
    const VTABLE: &'static RawWakerVTable = {
        #[inline(always)]
        unsafe fn clone_waker(waker: *const ()) -> RawWaker {
            unsafe { Arc::increment_strong_count(waker as *const WakerInner) };
            RawWaker::new(
                waker,
                &RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker),
            )
        }

        // Wake by value, moving the Arc into the Wake::wake function
        unsafe fn wake(waker: *const ()) {
            unsafe { Arc::from_raw(waker as *const WakerInner) }
                .handle
                .schedule();
        }

        // Wake by reference, wrap the waker in ManuallyDrop to avoid dropping it
        unsafe fn wake_by_ref(waker: *const ()) {
            ManuallyDrop::new(unsafe { Arc::from_raw(waker as *const WakerInner) })
                .handle
                .schedule();
        }

        // Decrement the reference count of the Arc on drop
        unsafe fn drop_waker(waker: *const ()) {
            unsafe { Arc::decrement_strong_count(waker as *const WakerInner) };
        }

        &RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker)
    };

    pub fn into_std(self) -> std::task::Waker {
        unsafe { std::task::Waker::new(Arc::into_raw(self.0) as _, Self::VTABLE) }
    }
}

pub trait WakerExt {
    fn try_as_compio(&self) -> Option<&Handle>;
    unsafe fn as_compio_unchecked(&self) -> &Handle;
}

impl WakerExt for std::task::Waker {
    fn try_as_compio(&self) -> Option<&Handle> {
        if self.vtable() == Waker::VTABLE {
            Some(unsafe { self.as_compio_unchecked() })
        } else {
            None
        }
    }

    unsafe fn as_compio_unchecked(&self) -> &Handle {
        unsafe { &*self.data().cast::<Handle>() }
    }
}

pub(crate) struct TaskIter<'a> {
    queue: &'a SlotQueue<TaskId, Task>,
    head: Option<TaskId>,
    is_hot: bool,
}

impl<'a> Iterator for TaskIter<'a> {
    type Item = TaskId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.head.is_none() && self.is_hot {
            self.head = self.queue.cold_head();
            self.is_hot = false;
        }

        let id = self.head?;
        self.head = self.queue.next(id);

        Some(id)
    }
}
