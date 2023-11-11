use core::marker::PhantomData;
use std::{
    ptr::{null_mut, NonNull},
    sync::atomic::{AtomicPtr, Ordering::*},
};

pub struct Queue<T> {
    size: usize,
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        let node = Node::new(None);
        let node = Box::into_raw(Box::new(node));
        Self {
            head: AtomicPtr::new(node),
            tail: AtomicPtr::new(node),
            size: 0,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, item: T) {
        let node = Node::new(Some(item));
        let node_ptr = Box::into_raw(Box::new(node));
        let prev_tail = self.tail.load(Relaxed);
        unsafe {
            self.tail.swap(node_ptr, Release);
            (*prev_tail).next.store(node_ptr, Release);
        }

        self.size += 1;
    }

    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        let mut head_ptr = NonNull::new(self.head.load(Relaxed)).unwrap();

        loop {
            match unsafe { head_ptr.as_mut().item.take() } {
                Some(val) => {
                    unsafe {
                        let next = head_ptr.as_ref().next.load(Acquire);
                        if !next.is_null() {
                            self.head.swap(next, Relaxed);
                        }
                    };
                    self.size -= 1;
                    return Some(val);
                }

                None => unsafe {
                    let next = head_ptr.as_ref().next.load(Acquire);
                    head_ptr = NonNull::new(next).map(|next_nnptr| {
                        self.head.swap(next, Relaxed);
                        next_nnptr
                    })?;
                },
            }
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn iter(&mut self) -> Iter<T> {
        Iter {
            len: self.len(),
            ptr: unsafe { self.head.load(Relaxed).as_ref().unwrap() },
            phantom: PhantomData,
        }
    }
}

pub struct Iter<'a, T: 'a> {
    len: usize,
    ptr: *const Node<T>,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        if self.len == 0 {
            return None;
        }

        loop {
            unsafe {
                let node_ref = self.ptr.as_ref().unwrap();
                let key = &(*node_ref).item;

                match (key, node_ref.next.load(Relaxed).is_null()) {
                    (Some(key), true) => {
                        self.len -= 1;
                        return Some(&key);
                    }

                    (Some(key), false) => {
                        self.len -= 1;
                        self.ptr = node_ref.next.load(Relaxed);
                        return Some(&key);
                    }

                    (None, true) => {
                        return None;
                    }

                    (None, false) => {
                        self.ptr = node_ref.next.load(Relaxed);
                    }
                }
            };
        }
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        let head = self.head.get_mut();
        while let Some(nnptr) = NonNull::new(*head) {
            let mut node = unsafe { Box::from_raw(nnptr.as_ptr()) };
            *head = *node.next.get_mut();
        }
    }
}

#[repr(align(/* at least */ 2))]
struct Node<T> {
    item: Option<T>,
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    fn new(item: Option<T>) -> Self {
        Self {
            item,
            next: AtomicPtr::new(null_mut()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn empty_queue() {
        let mut queue = Queue::<usize>::new();
        assert!(queue.pop().is_none());
    }

    #[test]
    fn push_pop() {
        let mut queue = Queue::new();
        queue.push(1);
        queue.push(2);
        queue.push(3);
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert!(queue.pop().is_none());
        assert!(queue.pop().is_none());
    }

    #[test]
    fn iter() {
        let mut queue = Queue::new();
        queue.push(1);
        queue.push(2);
        queue.push(3);
        queue.push(4);

        let mut iter = queue.iter();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&4));
        assert_eq!(iter.next(), None);

        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), Some(4));
    }

    #[test]
    fn empty_iter() {
        let mut queue: Queue<u32> = Queue::new();
        let mut iter = queue.iter();
        assert_eq!(iter.next(), None);
    }
}
