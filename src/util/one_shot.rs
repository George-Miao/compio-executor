use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicU8, Ordering::*},
    task::{Context, Poll, Waker},
    thread::yield_now,
};

use crate::util::abort_on_panic;

const LOCKED: u8 = 1 << 1;
const CLOSED: u8 = 1 << 2;
const CANCELED: u8 = 1 << 3;
const WAKER_SET: u8 = 1 << 4;
const VALUE_SET: u8 = 1 << 5;

pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let inner = NonNull::from_mut(Box::leak(Box::new(Inner::new())));
    let sender = Sender { inner };
    let recv = Receiver { inner };
    (sender, recv)
}

pub struct Sender<T> {
    inner: NonNull<Inner<T>>,
}

pub struct Receiver<T> {
    inner: NonNull<Inner<T>>,
}

impl<T> Unpin for Sender<T> {}
impl<T> Unpin for Receiver<T> {}

unsafe impl<T: Send> Send for Receiver<T> {}

struct Inner<T> {
    state: AtomicU8,
    value: UnsafeCell<MaybeUninit<T>>,
    waker: UnsafeCell<MaybeUninit<Waker>>,
}

impl<T> Inner<T> {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(0),
            value: UnsafeCell::new(MaybeUninit::uninit()),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    fn lock(&self) -> Option<LockGaurd<'_>> {
        let curr = self.state.load(Acquire);

        if curr & VALUE_SET != 0 {
            return Some(self.lock_anyway(Some(curr)));
        }

        if curr & CLOSED != 0 {
            return None;
        }

        Some(self.lock_anyway(Some(curr)))
    }

    /// Get a mutable reference to the state without locking.
    unsafe fn state_unsynced(&self) -> &mut u8 {
        unsafe { &mut *self.state.as_ptr() }
    }

    /// Lock without checking if currently it's closed
    #[inline(always)]
    fn lock_anyway(&self, curr: Option<u8>) -> LockGaurd<'_> {
        let mut curr = curr.unwrap_or_else(|| self.state.load(Relaxed));

        loop {
            match self
                .state
                .compare_exchange_weak(curr & !LOCKED, curr | LOCKED, Acquire, Relaxed)
            {
                Ok(state) => {
                    return LockGaurd {
                        inner: &self.state,
                        state,
                    };
                }
                Err(actual) => {
                    curr = actual;
                    yield_now();
                }
            }
        }
    }

    fn set_value(&self, value: T) -> Option<T> {
        let mut guard = self.lock_anyway(None);

        if guard.state & CLOSED != 0 {
            return Some(value);
        }

        debug_assert!(guard.state & VALUE_SET == 0, "Value set twice");

        unsafe { &mut *self.value.get() }.write(value);
        guard.state |= VALUE_SET;

        if guard.state & WAKER_SET != 0 {
            abort_on_panic(|| unsafe { (*self.waker.get()).assume_init_ref() }.wake_by_ref());
        }

        None
    }

    fn close(&self) -> bool {
        let Some(mut guard) = self.lock() else {
            return true;
        };

        if guard.state & WAKER_SET != 0 {
            abort_on_panic(|| unsafe { (*self.waker.get()).assume_init_ref() }.wake_by_ref());
        }

        guard.state |= CLOSED;

        false
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut guard = self.lock_anyway(None);

        if guard.state & VALUE_SET == 0 {
            if guard.state & CLOSED != 0 {
                return Poll::Ready(None);
            }

            let waker_set = guard.state & WAKER_SET == WAKER_SET;

            if waker_set {
                let current = unsafe { (*self.waker.get()).assume_init_ref() };
                if current.will_wake(cx.waker()) {
                    return Poll::Pending;
                }
            };

            drop(guard);

            let waker = abort_on_panic(|| cx.waker().clone());

            let Some(mut guard) = self.lock() else {
                // Sender dropped during cloning
                return Poll::Ready(None);
            };

            if guard.state & VALUE_SET != 0 {
                // Value set during cloning
                return Poll::Ready(Some(unsafe { (*self.value.get()).assume_init_read() }));
            }

            if waker_set {
                unsafe { (*self.waker.get()).assume_init_drop() };
            }

            unsafe { &mut *self.waker.get() }.write(waker);
            guard.state |= WAKER_SET;

            Poll::Pending
        } else {
            let value = unsafe { (*self.value.get()).assume_init_read() };
            guard.state &= !VALUE_SET;

            Poll::Ready(Some(value))
        }
    }

    /// Poll by assuming the receiver was not moved to another thread.
    unsafe fn poll_local(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let state = unsafe { self.state_unsynced() };
        let curr = *state;
        debug_assert!(curr & LOCKED == 0, "Poll when locked");

        if curr & VALUE_SET == 0 {
            if curr & CLOSED != 0 {
                return Poll::Ready(None);
            }

            if curr & WAKER_SET == WAKER_SET {
                let current = unsafe { (*self.waker.get()).assume_init_ref() };
                if current.will_wake(cx.waker()) {
                    return Poll::Pending;
                }
            }

            let waker = abort_on_panic(|| cx.waker().clone());

            unsafe { &mut *self.waker.get() }.write(waker);
            *state |= WAKER_SET;

            Poll::Pending
        } else {
            let value = unsafe { (*self.value.get()).assume_init_read() };
            *state &= !VALUE_SET;

            Poll::Ready(Some(value))
        }
    }

    unsafe fn release(ptr: NonNull<Self>) {
        let this = unsafe { Box::from_raw(ptr.as_ptr()) };
        let state = this.state.load(Acquire);
        debug_assert!(state & LOCKED == 0, "Lock held when releasing");
        if state & WAKER_SET != 0 {
            abort_on_panic(|| unsafe { (*this.waker.get()).assume_init_drop() });
        }
        if state & VALUE_SET != 0 {
            abort_on_panic(|| unsafe { (*this.value.get()).assume_init_drop() });
        }
    }
}

struct LockGaurd<'a> {
    inner: &'a AtomicU8,
    state: u8,
}

impl Drop for LockGaurd<'_> {
    fn drop(&mut self) {
        self.inner.store(self.state & !LOCKED, Release);
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        unsafe {
            if self.inner.as_ref().close() {
                Inner::release(self.inner);
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        unsafe {
            if self.inner.as_ref().close() {
                Inner::release(self.inner);
            }
        }
    }
}

impl<T> Sender<T> {
    pub fn send(self, value: T) -> Result<(), T> {
        match unsafe { self.inner.as_ref() }.set_value(value) {
            Some(res) => Err(res),
            None => Ok(()),
        }
    }

    pub fn is_canceled(&self) -> bool {
        unsafe { self.inner.as_ref() }.state.load(Relaxed) & CANCELED != 0
    }
}

impl<T> Receiver<T> {
    pub fn set_canceled(&self) {
        unsafe { self.inner.as_ref() }
            .state
            .fetch_or(CANCELED, Relaxed);
    }

    pub fn is_canceled(&self) -> bool {
        unsafe { self.inner.as_ref() }.state.load(Relaxed) & CANCELED != 0
    }

    pub fn poll(self: &Self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        unsafe { self.inner.as_ref() }.poll(cx)
    }

    pub unsafe fn poll_local(self: &Self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        unsafe { self.inner.as_ref().poll_local(cx) }
    }
}

#[cfg(test)]
mod tests {
    use std::{pin::pin, task::Waker, thread};

    use super::*;

    #[test]
    fn simple() {
        let (tx, rx) = oneshot();
        tx.send(1).unwrap();

        let waker = Waker::noop();
        let mut cx = Context::from_waker(&waker);

        let mut rx = pin!(rx);
        assert_eq!(rx.as_mut().poll(&mut cx), Poll::Ready(Some(1)));
    }

    #[test]
    fn close() {
        let (tx, rx) = oneshot::<i32>();
        drop(tx);

        let waker = Waker::noop();
        let mut cx = Context::from_waker(&waker);

        let mut rx = pin!(rx);
        assert_eq!(rx.as_mut().poll(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn send() {
        let (tx, rx) = oneshot();
        tx.send(1).unwrap();

        thread::spawn(move || {
            let waker = Waker::noop();
            let mut cx = Context::from_waker(&waker);

            let mut rx = pin!(rx);
            assert_eq!(rx.as_mut().poll(&mut cx), Poll::Ready(Some(1)));
        })
        .join()
        .unwrap();
    }
}
