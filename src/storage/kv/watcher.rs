use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::Mutex;

struct Watcher {
    done_upto: u64,
    mutex: Mutex<()>,
    waiters: HashMap<u64, Mark>,
}

struct Mark {
    ch: Option<Sender<()>>,
    closer: Receiver<()>,
}

impl Watcher {
    fn new(done_upto: u64) -> Self {
        Watcher {
            waiters: HashMap::new(),
            done_upto,
            mutex: Mutex::new(()),
        }
    }

    fn done_upto(&mut self, t: u64) {
        let mut _guard = self.mutex.lock().unwrap();

        if self.done_upto >= t {
            return;
        }

        for i in (self.done_upto + 1)..=t {
            if let Some(wp) = self.waiters.get_mut(&i) {
                wp.ch.take();
                self.waiters.remove(&i);
            }
        }

        self.done_upto = t;
    }

    fn wait_for(&mut self, t: u64) {
        let mut _guard = self.mutex.lock().unwrap();

        if self.done_upto >= t {
            return;
        }

        let wp = self.waiters.entry(t).or_insert_with(|| {
            let (tx, rx) = bounded(1);
            Mark {
                ch: Some(tx),
                closer: rx,
            }
        });

        drop(_guard);

        matches!(wp.closer.recv(), Err(crossbeam_channel::RecvError));
    }

    fn done_until(&self) -> u64 {
        let _guard = self.mutex.lock().unwrap();
        self.done_upto
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_waiters_hub() {
        let mut hub = Watcher::new(0);

        hub.done_upto(10);
        let t2 = hub.done_until();
        assert_eq!(t2, 10);
        hub.wait_for(1);
        hub.wait_for(10);
    }
}
