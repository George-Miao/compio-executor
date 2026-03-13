use std::{marker::PhantomData, sync::Arc};

use crossfire::{
    MTx, Rx, TrySendError,
    mpsc::{self, Array},
};

use crate::{Runnable, queue::local::LocalQueue, util::SendWrapper};

mod local;

#[derive(Clone)]
pub(crate) struct EnQueue {
    sync: MTx<Array<Runnable>>,
    local: Arc<SendWrapper<LocalQueue<Runnable>>>,
}

pub(crate) struct DeQueue {
    sync: Rx<Array<Runnable>>,
    local: Arc<SendWrapper<LocalQueue<Runnable>>>,
    _marker: PhantomData<*const ()>,
}

pub fn make_queue() -> (EnQueue, DeQueue) {
    let (tx, rx) = mpsc::bounded_blocking::<Runnable>(64);
    let local = Arc::new(SendWrapper::new(LocalQueue::with_capacity(64)));
    let enq = EnQueue {
        sync: tx,
        local: local.clone(),
    };
    let deq = DeQueue {
        sync: rx,
        local,
        _marker: PhantomData,
    };
    (enq, deq)
}

impl EnQueue {
    /// Try to enqueue, return Err(runnable) if we're on a different queue
    pub fn try_push(&self, runnable: Runnable) -> Result<(), Runnable> {
        // Local queue does not have a fixed size, will always success to push
        if let Some(local) = self.local.get() {
            local.push(runnable);
            return Ok(());
        };
        match self.sync.try_send(runnable) {
            Ok(_) => Ok(()),
            Err(TrySendError::Disconnected(_)) => panic!("Executor is dropped"),
            Err(TrySendError::Full(runnable)) => Err(runnable),
        }
    }

    /// Guaranteed successful enqueue, at the cost of potentially blocking
    /// current thread.
    pub fn push(&self, runnable: Runnable) {
        if let Some(local) = self.local.get() {
            local.push(runnable);
        } else {
            self.sync.send(runnable).expect("Executor is dropped")
        }
    }

    /// A hint for `Runnable` to decide whether eagerly reschedule pending task.
    pub fn is_empty(&self) -> bool {
        if let Some(local) = self.local.get() {
            local.is_empty()
        } else {
            self.sync.is_empty()
        }
    }
}

impl DeQueue {
    pub fn pop(&self) -> Option<Runnable> {
        // SAFETY: _marker ensures `DeQueue` cannot be sent to other thread
        let local = unsafe { self.local.get_unchecked() };
        local.pop().or_else(|| self.sync.try_recv().ok())
    }
}
