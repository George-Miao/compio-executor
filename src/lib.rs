use std::{
    any::Any,
    fmt::Debug,
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ops::Deref,
    panic::resume_unwind,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker as StdWaker, ready},
};

use compio_log::{instrument, trace};
use thin_cell::sync::ThinCell;

use crate::{
    guard::RunningGuard,
    inner::{Guard, Inner, InnerDyn},
    queue::{DeQueue, EnQueue, make_queue},
    state::State,
    util::abort_on_panic,
};

mod guard;
mod inner;
mod queue;
mod state;
#[cfg(test)]
mod tests;
mod util;

pub(crate) type Panic = Box<dyn Any + Send + 'static>;

pub struct Executor {
    enq: EnQueue,
    deq: DeQueue,
}

impl Executor {
    pub fn new() -> Self {
        let (enq, deq) = make_queue();

        Self { enq, deq }
    }

    pub fn block_on<F: Future + 'static>(&self, f: F) -> F::Output {
        let waker = StdWaker::noop();
        let mut ctx = Context::from_waker(&waker);
        let mut f = std::pin::pin!(f);

        loop {
            if let Poll::Ready(res) = f.as_mut().poll(&mut ctx) {
                return res;
            }
            if let Some(runable) = self.deq.pop() {
                unsafe { runable.run() };
            };
        }
    }

    pub fn spawn<F: Future + 'static>(&self, f: F) -> JoinHandle<F::Output> {
        let task = Task::new(self.enq.clone(), f);
        task.schedule(None);
        JoinHandle {
            task: Some(task),
            _marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Task {
    cell: ThinCell<dyn InnerDyn>,
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("Task");
        match self.cell.try_borrow() {
            Some(mut g) => f
                .field("state", g.state())
                .field("waker", g.waker())
                .finish(),
            None => f
                .field("state", &"LOCKED")
                .field("waker", &"LOCKED")
                .finish(),
        }
    }
}

impl Task {
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::clone, Self::wake, Self::wake_by_ref, Self::drop);

    pub fn new<F: Future + 'static>(queue: EnQueue, future: F) -> Self {
        let cell = unsafe { ThinCell::new_unsize(Inner::new(queue, future), |ptr| ptr as _) };
        Self { cell }
    }

    pub fn as_std(&self) -> impl Deref<Target = StdWaker> {
        let waker = unsafe { StdWaker::from_raw(RawWaker::new(self.cell.as_ptr(), &Self::VTABLE)) };
        ManuallyDrop::new(waker)
    }

    fn close(&self) {
        self.cell.borrow().state().update(|st| {
            trace!(state = ?st, "Closing");

            if st.is_closed() | st.is_completed() {
                return;
            }
            st.insert(State::CLOSED)
        });
    }

    unsafe fn poll<T>(&self, cx: &mut Context<'_>) -> Poll<Option<Result<T, Panic>>> {
        instrument!(compio_log::Level::DEBUG, "poll", task = ?self, ?cx);

        let mut guard = self.cell.borrow();
        let state = *guard.state();
        if state.is_closed() {
            trace!("Closed");

            debug_assert!(!state.is_completed());

            if state.is_running() | state.is_scheduled() {
                guard.register(cx.waker());

                Poll::Pending
            } else {
                Poll::Ready(None)
            }
        } else if state.is_completed() {
            trace!("Completed");

            debug_assert!(!state.is_closed());
            debug_assert!(!state.is_running());
            debug_assert!(!state.is_scheduled());

            let mut res = MaybeUninit::<Result<T, Panic>>::uninit();
            let has_result = unsafe { guard.take_result(&raw mut res as _) };
            if has_result {
                // SAFETY: res is initialized guaranteed by `take_result`
                Poll::Ready(Some(unsafe { res.assume_init() }))
            } else {
                Poll::Ready(None)
            }
        } else if !state.is_scheduled() {
            trace!("Not scheduled, register");

            guard.register(cx.waker());
            Poll::Pending
        } else {
            Poll::Pending
        }
    }

    fn schedule(&self, guard: Option<Guard<'_>>) {
        let mut guard = guard.unwrap_or_else(|| self.cell.borrow());

        let state = *guard.state();

        // Don't schedule again if we cannot progress or have already scheduled.
        if state.is_closed() || state.is_completed() || state.is_scheduled() {
            return;
        }

        guard.state().insert(State::SCHEDULED);

        // Runnable::run will schedule after running when they found SCHEDULED bit is
        // set.
        if state.is_running() {
            return;
        }

        let runnable = self.runnable();
        if let Err(r) = guard.queue().try_push(runnable) {
            // Queue is full, release the lock and block current thread to enqueue
            let queue = guard.queue().clone();
            drop(guard);
            queue.push(r);
        }
    }

    fn runnable(&self) -> Runnable {
        Runnable { task: self.clone() }
    }

    unsafe fn reclaim(ptr: *const ()) -> Self {
        let cell = unsafe { ThinCell::from_raw(ptr as _) };
        Self { cell }
    }

    unsafe fn reclaim_ref(ptr: *const ()) -> ManuallyDrop<Self> {
        ManuallyDrop::new(unsafe { Self::reclaim(ptr) })
    }

    unsafe fn clone(ptr: *const ()) -> RawWaker {
        let this = unsafe { Self::reclaim_ref(ptr) };
        let clone = this.clone();
        RawWaker::new(ManuallyDrop::into_inner(clone).cell.leak(), &Self::VTABLE)
    }

    unsafe fn wake(ptr: *const ()) {
        unsafe { Self::wake_by_ref(ptr) };
        unsafe { Self::drop(ptr) };
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        let this = unsafe { Self::reclaim_ref(ptr) };
        this.schedule(None);
    }

    unsafe fn drop(ptr: *const ()) {
        _ = unsafe { Self::reclaim(ptr) };
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct JoinHandle<T> {
    task: Option<Task>,
    _marker: PhantomData<T>,
}

impl<T> Unpin for JoinHandle<T> {}

unsafe impl<T: Send + Sync> Send for JoinHandle<T> {}
unsafe impl<T: Send + Sync> Sync for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    pub fn cancel(&self) {
        instrument!(compio_log::Level::DEBUG, "JoinHandle::canel", task = ?self.task);

        if let Some(task) = self.task.as_ref() {
            task.close();
        }
    }

    pub fn detact(self) {
        let this = ManuallyDrop::new(self);
        unsafe { std::ptr::read(&this.task) };
    }
}

#[derive(Debug)]
pub enum JoinError {
    Canceled,
    Panicked(Panic),
}

#[derive(Debug)]
pub struct JoinResult<T>(pub Result<T, JoinError>);

impl<T> JoinResult<T> {
    pub fn resume_unwind(self) -> Option<T> {
        match self.0 {
            Ok(res) => Some(res),
            Err(JoinError::Canceled) => None,
            Err(JoinError::Panicked(e)) => resume_unwind(e),
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = JoinResult<T>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let task = self.task.as_ref().expect("Polled after finish");
        let res = ready!(unsafe { task.poll::<T>(cx) });
        let innner = match res {
            Some(Ok(res)) => {
                self.as_mut().task.take();
                Ok(res)
            }
            Some(Err(panic)) => Err(JoinError::Panicked(panic)),
            None => Err(JoinError::Canceled),
        };
        Poll::Ready(JoinResult(innner))
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[repr(transparent)]
#[derive(Debug)]
struct Runnable {
    task: Task,
}

unsafe impl Send for Runnable {}
unsafe impl Sync for Runnable {}

impl Drop for Runnable {
    fn drop(&mut self) {
        instrument!(compio_log::Level::DEBUG, "Runnable::drop", task = ?self.task);

        self.task.close();
    }
}

impl Runnable {
    /// # Safety
    ///
    /// Must be ran on the same thread where the `Task` was created
    pub unsafe fn run(self) {
        instrument!(compio_log::Level::DEBUG, "run", task = ?self.task);

        let task = self.into_inner();
        let mut guard = task.cell.borrow();
        let state = guard.state();
        debug_assert!(
            !state.is_running(),
            "There can only exist only one DeQueue, and it is single-threaded and non-overlapping"
        );
        state.remove(State::SCHEDULED);

        if state.is_closed() {
            // SAFETY: Guaranteed by caller
            unsafe { guard.on_close() };
            return;
        }

        // Critical Section - May take arbitrary long time
        let poll = RunningGuard::new(&task, guard).poll(&task.as_std());

        // Post process after running
        let mut guard = task.cell.borrow();
        let empty = guard.queue().is_empty();
        let state = guard.state();

        trace!(res = ?poll, is_empty = empty, "Polled");

        match poll {
            Poll::Ready(_) => {
                // The task may be waked up during running. Clear any stale SCHEDULED bit.
                state.remove(State::SCHEDULED);

                // Task closed during running
                if state.is_closed() {
                    // SAFETY: Guaranteed by caller
                    unsafe { guard.drop_future() }
                } else {
                    state.insert(State::COMPLETED)
                }

                guard.wake();
            }
            Poll::Pending if state.is_closed() => {
                // Task closed during running
                state.remove(State::SCHEDULED);
                // SAFETY: Guaranteed by caller
                unsafe { guard.drop_future() }

                guard.wake();
            }
            Poll::Pending if state.is_scheduled() => {
                // Task was scheduled but no Runnable was enqueue because we were running. It's
                // our responsibility to do so.
                let runnable = task.runnable();

                guard.queue().try_push(runnable).expect(
                    "We should be on the same thread where the Task was created. Local queue can \
                     insert as many Runnables as we want",
                );
                return;
            }
            Poll::Pending => {
                // If queue is empty, eagerly reschedule
                if guard.queue().is_empty() {
                    task.schedule(Some(guard));
                }
            }
        }
    }

    fn into_inner(self) -> Task {
        let this = ManuallyDrop::new(self);
        unsafe { std::ptr::read(&this.task) }
    }
}

trait RefExt {
    /// Post process if state is closed.
    ///
    /// # Safety
    ///
    /// - Must be called on the same thread where the `Task` was created
    /// - Current state must be CLOSED && !RUNNING && !SCHEDULED
    unsafe fn on_close(self);

    /// Register a waker
    fn register(&mut self, waker: &StdWaker);

    /// Wake up registered waker
    fn wake(self);
}

impl RefExt for Guard<'_> {
    unsafe fn on_close(mut self) {
        if cfg!(debug_assertions) {
            let st = *self.state();
            assert!(st.is_closed());
            assert!(!st.is_running());
            assert!(!st.is_scheduled());
        }

        unsafe { self.drop_future() };
        self.state().update(|state| state.remove(State::SCHEDULED));
        self.wake();
    }

    fn register(&mut self, waker: &StdWaker) {
        let w = self.waker();
        if w.as_ref().is_some_and(|w| w.will_wake(waker)) {
            return;
        }
        abort_on_panic(|| *w = Some(waker.clone()));
    }

    fn wake(mut self) {
        let Some(waker) = self.waker().take() else {
            return;
        };
        drop(self);
        abort_on_panic(|| waker.wake_by_ref());
    }
}
