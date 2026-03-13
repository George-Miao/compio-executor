bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct State: u8 {
        /// Task is being waked
        const SCHEDULED = 1 << 0;
        /// Polling underlying future
        const RUNNING = 1 << 1;
        /// Underlyfing future has finished but the result is not retrieved yet
        const COMPLETED = 1 << 2;
        /// 1. It gets canceled by `Runnable::drop()` or `Task::drop()`.
        /// 2. Its output gets awaited by the `Task`.
        /// 3. It panics while polling the future.
        /// 4. It is completed and the `Task` gets dropped.
        const CLOSED = 1 << 3;
    }
}

impl State {
    pub fn update<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut State) -> R,
    {
        f(self)
    }

    pub fn is_scheduled(&self) -> bool {
        self.contains(Self::SCHEDULED)
    }

    pub fn is_running(&self) -> bool {
        self.contains(Self::RUNNING)
    }

    pub fn is_completed(&self) -> bool {
        self.contains(Self::COMPLETED)
    }

    pub fn is_closed(&self) -> bool {
        self.contains(Self::CLOSED)
    }
}
