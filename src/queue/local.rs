use std::{cell::UnsafeCell, collections::VecDeque, marker::PhantomData};

/// A queue that is `!Sync` with interior mutability.
#[derive(Debug)]
pub(crate) struct LocalQueue<T> {
    queue: UnsafeCell<VecDeque<T>>,
    _marker: PhantomData<*const ()>,
}

impl<T> LocalQueue<T> {
    /// Creates an empty `LocalQueue` with capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            queue: UnsafeCell::new(VecDeque::with_capacity(cap)),
            _marker: PhantomData,
        }
    }

    /// Pushes an item to the back of the queue.
    pub fn push(&self, item: T) {
        // SAFETY:
        // Exclusive mutable access because:
        // - The mutable reference is created and used immediately within this scope.
        // - `LocalQueue` is `!Sync`, so no other threads can access it concurrently.
        let queue = unsafe { &mut *self.queue.get() };
        queue.push_back(item);
    }

    /// Pops an item from the front of the queue, returning `None` if empty.
    pub fn pop(&self) -> Option<T> {
        // SAFETY:
        // Exclusive mutable access because:
        // - The mutable reference is created and used immediately within this scope.
        // - `LocalQueue` is `!Sync`, so no other threads can access it concurrently.
        let queue = unsafe { &mut *self.queue.get() };
        queue.pop_front()
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        // SAFETY:
        // Exclusive mutable access because:
        // - The mutable reference is created and used immediately within this scope.
        // - `LocalQueue` is `!Sync`, so no other threads can access it concurrently.
        let queue = unsafe { &mut *self.queue.get() };
        queue.is_empty()
    }
}
