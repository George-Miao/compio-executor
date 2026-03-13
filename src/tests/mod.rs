use std::{
    cell::{Cell, RefCell},
    future::Future,
    task::{Context, Poll},
};

use super::*;

std::thread_local! {
    static EXE: Executor = Executor::new();
}

fn spawn<F: Future + 'static>(f: F) -> JoinHandle<F::Output> {
    EXE.with(|exe| exe.spawn(f))
}

fn block_on<F: Future + 'static>(f: F) -> F::Output {
    EXE.with(|exe| exe.block_on(f))
}

struct Yield(bool);

impl Future for Yield {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn yield_now() {
    Yield(false).await
}

#[test]
fn test_executor_runs_to_completion() {
    block_on(async {
        let values = std::rc::Rc::new(RefCell::new(Vec::new()));

        let a = {
            let values = values.clone();
            spawn(async move {
                for i in 0..5 {
                    values.borrow_mut().push(("a", i));
                    yield_now().await;
                }
                10usize
            })
        };

        let b = {
            let values = values.clone();
            spawn(async move {
                for i in 0..3 {
                    values.borrow_mut().push(("b", i));
                    yield_now().await;
                }
                20usize
            })
        };

        let ra = a.await.0.unwrap();
        let rb = b.await.0.unwrap();

        assert_eq!(ra, 10);
        assert_eq!(rb, 20);

        let values = values.borrow();
        assert_eq!(values.iter().filter(|(t, _)| *t == "a").count(), 5);
        assert_eq!(values.iter().filter(|(t, _)| *t == "b").count(), 3);
    });
}

#[test]
fn test_cancel_before_poll_returns_canceled() {
    block_on(async {
        let hit = std::rc::Rc::new(Cell::new(false));
        let hit_task = hit.clone();

        let handle = spawn(async move {
            hit_task.set(true);
            1usize
        });

        handle.cancel();
        let res = handle.await.0;
        assert!(matches!(res, Err(JoinError::Canceled)));
        assert!(
            !hit.get(),
            "future body should never run after cancel-before-poll"
        );
    });
}

#[test]
fn test_cancel_during_execution_returns_canceled() {
    block_on(async {
        let entered = std::rc::Rc::new(Cell::new(false));
        let done = std::rc::Rc::new(Cell::new(false));

        let entered_task = entered.clone();
        let done_task = done.clone();

        let handle = spawn(async move {
            entered_task.set(true);
            yield_now().await;
            done_task.set(true);
            42usize
        });

        while !entered.get() {
            yield_now().await;
        }

        handle.cancel();
        let res = handle.await.0;
        assert!(matches!(res, Err(JoinError::Canceled)));
        assert!(
            !done.get(),
            "future should be dropped after cancellation before completing"
        );
    });
}

#[test]
fn test_join_handle_drop_cancels_task() {
    block_on(async {
        let completed = std::rc::Rc::new(Cell::new(false));
        let completed_task = completed.clone();

        let handle = spawn(async move {
            for _ in 0..3 {
                yield_now().await;
            }
            completed_task.set(true);
        });

        drop(handle);

        for _ in 0..8 {
            yield_now().await;
        }

        assert!(
            !completed.get(),
            "dropping JoinHandle should cancel and prevent completion"
        );
    });
}

#[test]
fn test_panic_is_captured_in_join_error() {
    block_on(async {
        let handle = spawn(async move {
            panic!("intentional panic from task");
        });

        let res = handle.await.0;
        match res {
            Err(JoinError::Panicked(payload)) => {
                if let Some(msg) = payload.downcast_ref::<&'static str>() {
                    assert_eq!(*msg, "intentional panic from task");
                } else if let Some(msg) = payload.downcast_ref::<String>() {
                    assert_eq!(msg, "intentional panic from task");
                } else {
                    panic!("unexpected panic payload type");
                }
            }
            _ => panic!("expected panicked join error"),
        }
    });
}

#[test]
fn test_detach_allows_task_to_continue() {
    block_on(async {
        let completed = std::rc::Rc::new(Cell::new(false));
        let completed_task = completed.clone();

        let handle = spawn(async move {
            for _ in 0..4 {
                yield_now().await;
            }
            completed_task.set(true);
        });

        handle.detact();

        for _ in 0..10 {
            yield_now().await;
        }

        assert!(
            completed.get(),
            "detached task should continue running to completion"
        );
    });
}

#[test]
fn test_multiple_cancels_are_idempotent() {
    block_on(async {
        let ran = std::rc::Rc::new(Cell::new(false));
        let ran_task = ran.clone();

        let handle = spawn(async move {
            ran_task.set(true);
            yield_now().await;
        });

        handle.cancel();
        handle.cancel();
        handle.cancel();

        let res = handle.await.0;
        assert!(matches!(res, Err(JoinError::Canceled)));
        assert!(!ran.get(), "task should not run after repeated cancels");
    });
}
