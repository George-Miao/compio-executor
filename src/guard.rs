use std::task::{Poll, Waker as StdWaker};

use thin_cell::sync::Ref;

use crate::{
    Task,
    inner::{InnerDyn, Pollable},
    state::State,
};

/// A guard that holds a unique reference to `FutureState`
pub struct RunningGuard<'a> {
    task: &'a Task,
    pollable: &'a mut dyn Pollable,
}

impl<'a> RunningGuard<'a> {
    pub fn new(task: &'a Task, mut guard: Ref<'_, dyn InnerDyn>) -> Self {
        debug_assert!(!guard.state().is_running());
        guard.state().insert(State::RUNNING);

        // SAFETY: we've set `RUNNING` bit, so we're the only one accessing
        // `FutureState` before returning.
        let pollable =
            unsafe { std::mem::transmute::<&mut dyn Pollable, &'a mut dyn Pollable>(guard.poll()) };

        Self { task, pollable }
    }

    /// Poll `FutureState`
    ///
    /// This will ensure RUNNING bit be unset after running
    pub fn poll(self, waker: &StdWaker) -> Poll<()> {
        self.pollable.poll(waker)
    }
}

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.task.cell.borrow().state().remove(State::RUNNING);
    }
}
