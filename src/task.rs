use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{PanicResult, queue::Waker, util::Sender};

pub struct Task {
    poll: Option<Box<dyn Pollable>>,
}

struct Concrete<F: Future> {
    future: F,
    tx: Sender<PanicResult<F::Output>>,
    waker: std::task::Waker,
    _marker: std::marker::PhantomData<*const ()>,
}

impl Task {
    pub fn new<F: Future + 'static>(
        future: F,
        tx: Sender<PanicResult<F::Output>>,
        waker: Waker,
    ) -> Self {
        let inner = Concrete::new(future, tx, waker);
        Self {
            poll: Some(Box::new(inner)),
        }
    }

    pub fn take(&mut self) -> Option<Box<dyn Pollable>> {
        self.poll.take()
    }

    pub fn reset(&mut self, inner: Box<dyn Pollable>) {
        self.poll = Some(inner);
    }
}

impl<F: Future> Concrete<F> {
    pub fn new(future: F, tx: Sender<PanicResult<F::Output>>, waker: Waker) -> Self {
        Self {
            tx,
            future,
            waker: waker.into_std(),
            _marker: std::marker::PhantomData,
        }
    }
}

/// Type-erased `Concrete`
pub trait Pollable {
    fn poll(self: Box<Self>) -> Option<Box<dyn Pollable>>;
}

impl<F: Future + 'static> Pollable for Concrete<F> {
    fn poll(mut self: Box<Self>) -> Option<Box<dyn Pollable>> {
        if self.tx.is_canceled() {
            return None;
        }
        let cx = &mut Context::from_waker(&self.waker);
        let mut fut = unsafe { Pin::new_unchecked(&mut self.future) };
        let res = catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx)));
        _ = match res {
            Ok(Poll::Pending) => return Some(self),
            Ok(Poll::Ready(res)) => self.tx.send(Ok(res)),
            Err(e) => self.tx.send(Err(e)),
        };

        None
    }
}
