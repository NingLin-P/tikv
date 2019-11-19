use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

pub struct Broacast<T> {
    inner: Arc<BroacastInner<T>>,
}

#[derive(Default)]
struct BroacastInner<T> {
    value: RwLock<T>,
    version: AtomicU64,
}

pub struct Receiver<T> {
    inner: Arc<BroacastInner<T>>,
    // value: T,
    recved: u64,
}

impl<T> Broacast<T> {
    pub fn new(value: T) -> Self {
        Broacast {
            inner: Arc::new(BroacastInner {
                value: RwLock::new(value),
                version: AtomicU64::new(1),
            }),
        }
    }

    pub fn broacast(&self, incomming: T) {
        // TODO: add comment about correctness
        {
            let mut value = self.inner.value.write().unwrap();
            *value = incomming;
        }
        self.inner.version.fetch_add(1, Ordering::Relaxed); // TODO: use correct order
    }

    pub fn add_recv(&self) -> Receiver<T> {
        Receiver {
            inner: self.inner.clone(),
            recved: 0,
        }
    }
}

impl<T: Clone> Receiver<T> {
    pub fn any_new(&mut self) -> Option<T> {
        let version = self.inner.version.load(Ordering::Relaxed);
        if self.recved >= version {
            return None;
        }
        self.recved = version;
        let incomming = self.inner.value.read().unwrap();
        Some(incomming.clone())
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver {
            inner: self.inner.clone(),
            recved: 0,
        }
    }
}
