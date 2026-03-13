use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll, Waker as StdWaker},
};

use thin_cell::sync::Ref;

use crate::{Panic, queue::EnQueue, state::State, util::abort_on_panic};

pub struct Inner<F: Future, E = ()> {
    state: State,
    queue: EnQueue,
    extra: Option<E>,
    waker: Option<StdWaker>,
    future: FutureState<F>,
}

pub type Guard<'a> = Ref<'a, dyn InnerDyn + 'static>;

enum FutureState<F: Future> {
    Running(F),
    Completed(Option<Result<F::Output, Panic>>),
}

impl<F: Future> Inner<F, ()> {
    pub fn new(queue: EnQueue, future: F) -> Self {
        Self {
            state: State::empty(),
            queue,
            extra: None,
            waker: None,
            future: FutureState::Running(future),
        }
    }
}

impl<F: Future, E> Inner<F, E> {
    pub fn with_extra(self, extra: E) -> Self {
        let Self {
            state,
            queue,
            waker,
            future,
            ..
        } = self;
        Self {
            state,
            queue,
            extra: Some(extra),
            waker,
            future,
        }
    }
}

pub trait InnerDyn {
    fn state(&mut self) -> &mut State;
    fn poll(&mut self) -> &mut dyn Pollable;
    fn waker(&mut self) -> &mut Option<StdWaker>;
    fn queue(&self) -> &EnQueue;
    fn extra(&mut self) -> Option<&mut dyn Any>;

    /// This function must be called on the same thread where `Inner` was
    /// created
    unsafe fn drop_future(&mut self);
    /// Take result, write to target, and return whether the result is set
    ///
    /// # Safety
    ///
    /// `target` must be a valid location for `Result<F::Output, Panic>`. This
    /// can be allocated with [`std::mem::MaybeUninit`].
    unsafe fn take_result(&mut self, target: *mut ()) -> bool;
}

/// Used to pass `FutureState` without needing to expose underlying future type
pub trait Pollable {
    fn poll(&mut self, waker: &StdWaker) -> Poll<()>;
}

impl<F: Future> Pollable for FutureState<F> {
    fn poll(&mut self, waker: &StdWaker) -> Poll<()> {
        let FutureState::Running(fut) = self else {
            return Poll::Ready(());
        };

        let mut cx = Context::from_waker(waker);
        let fut = unsafe { Pin::new_unchecked(fut) };

        match catch_unwind(AssertUnwindSafe(|| fut.poll(&mut cx))) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(res)) => *self = FutureState::Completed(Some(Ok(res))),
            Err(e) => *self = FutureState::Completed(Some(Err(e))),
        };

        Poll::Ready(())
    }
}

impl<F: Future, E: Send + 'static> InnerDyn for Inner<F, E> {
    fn state(&mut self) -> &mut State {
        &mut self.state
    }

    fn poll(&mut self) -> &mut dyn Pollable {
        &mut self.future
    }

    fn waker(&mut self) -> &mut Option<StdWaker> {
        &mut self.waker
    }

    fn queue(&self) -> &EnQueue {
        &self.queue
    }

    fn extra(&mut self) -> Option<&mut dyn Any> {
        self.extra.as_mut().map(|e| e as _)
    }

    unsafe fn drop_future(&mut self) {
        abort_on_panic(|| self.future = FutureState::Completed(None))
    }

    unsafe fn take_result(&mut self, target: *mut ()) -> bool {
        if let FutureState::Completed(res) = &mut self.future
            && let Some(res) = res.take()
        {
            // SAFETY: Caller guarantees target is a valid target for
            // `Result<F::Output, Panic>`
            unsafe { std::ptr::write(target as _, res) };

            return true;
        };

        return false;
    }
}
