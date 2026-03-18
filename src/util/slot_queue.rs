use std::cell::UnsafeCell;

use slotmap::{Key, SlotMap};

pub struct SlotQueue<K: Key, V> {
    inner: UnsafeCell<Inner<K, V>>,
    _marker: std::marker::PhantomData<*const ()>,
}

struct Inner<K: Key, V> {
    map: SlotMap<K, Item<K, V>>,
    hot: List<K>,
    cold: List<K>,
}

#[derive(Clone, Copy, Default)]
struct List<K> {
    head: Option<K>,
    tail: Option<K>,
}

struct Item<K, V> {
    prev: Option<K>,
    next: Option<K>,
    value: V,
    is_hot: bool,
}

impl<K: Key, V> Inner<K, V> {
    fn new(size: usize) -> Self {
        Self {
            map: SlotMap::with_capacity_and_key(size),
            hot: List::default(),
            cold: List::default(),
        }
    }

    fn link_tail(&mut self, key: K, is_hot: bool) {
        let list = if is_hot {
            &mut self.hot
        } else {
            &mut self.cold
        };
        let old_tail = list.tail;

        list.tail = Some(key);
        if list.head.is_none() {
            list.head = Some(key);
        }

        let item = self.map.get_mut(key).expect("item exists");
        item.prev = old_tail;
        item.next = None;
        item.is_hot = is_hot;

        if let Some(tail_key) = old_tail {
            self.map.get_mut(tail_key).expect("tail exists").next = Some(key);
        }
    }

    fn unlink(&mut self, key: K, is_hot: bool) {
        let list = if is_hot {
            &mut self.hot
        } else {
            &mut self.cold
        };

        let (prev, next) = {
            let item = self.map.get(key).expect("item exists");
            debug_assert_eq!(item.is_hot, is_hot);
            (item.prev, item.next)
        };

        if list.head == Some(key) {
            list.head = next;
        }
        if list.tail == Some(key) {
            list.tail = prev;
        }

        if let Some(prev_key) = prev {
            self.map.get_mut(prev_key).expect("prev exists").next = next;
        }
        if let Some(next_key) = next {
            self.map.get_mut(next_key).expect("next exists").prev = prev;
        }
    }

    fn make_hot(&mut self, key: K) {
        let Some(item) = self.map.get(key) else {
            return;
        };
        if !item.is_hot {
            self.unlink(key, false);
            self.link_tail(key, true);
        }
    }

    fn make_cold(&mut self, key: K) {
        let Some(item) = self.map.get(key) else {
            return;
        };
        if item.is_hot {
            self.unlink(key, true);
        }
        self.link_tail(key, false);
    }
}

impl<K: Key, V> SlotQueue<K, V> {
    pub fn new(size: usize) -> Self {
        Self {
            inner: UnsafeCell::new(Inner::new(size)),
            _marker: std::marker::PhantomData,
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that no concurrent access to the queue occurs
    /// while this reference is active.
    pub unsafe fn get(&self, key: K) -> Option<&mut V> {
        unsafe { self.get_inner() }
            .map
            .get_mut(key)
            .map(|item| &mut item.value)
    }

    /// # Safety
    ///
    /// The caller must ensure that no concurrent access to the queue occurs
    /// while this reference is active.
    unsafe fn get_inner(&self) -> &mut Inner<K, V> {
        // SAFETY: Caller must ensure no concurrent access to the queue.
        unsafe { &mut *self.inner.get() }
    }

    pub fn push_back_with(&self, value: impl FnOnce(K) -> V) -> K {
        let inner = unsafe { self.get_inner() };
        let key = inner.map.insert_with_key(|key| Item {
            prev: None,
            next: None,
            value: value(key),
            is_hot: true,
        });
        inner.link_tail(key, true);
        key
    }

    pub fn make_hot(&self, key: K) {
        unsafe { self.get_inner() }.make_hot(key)
    }

    pub fn make_cold(&self, key: K) {
        unsafe { self.get_inner() }.make_cold(key)
    }

    pub fn next(&self, key: K) -> Option<K> {
        let inner = unsafe { self.get_inner() };
        inner.map.get(key).and_then(|item| item.next)
    }

    pub fn hot_head(&self) -> Option<K> {
        let inner = unsafe { self.get_inner() };
        inner.hot.head
    }

    pub fn cold_head(&self) -> Option<K> {
        let inner = unsafe { self.get_inner() };
        inner.cold.head
    }

    pub fn remove(&self, key: K) -> Option<V> {
        let inner = unsafe { self.get_inner() };
        let is_hot = inner.map.get(key)?.is_hot;
        inner.unlink(key, is_hot);
        inner.map.remove(key).map(|item| item.value)
    }
}
