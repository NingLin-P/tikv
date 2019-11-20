use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Clone, Default)]
pub struct VersionCache<T> {
    inner: Arc<VersionCacheInner<T>>,
}

#[derive(Default)]
struct VersionCacheInner<T> {
    value: RwLock<T>,
    version: AtomicU64,
}

#[derive(Clone, Default)]
pub struct Getter<T> {
    inner: Arc<VersionCacheInner<T>>,
    cache: T,
    version: u64,
}

impl<T: Clone> VersionCache<T> {
    pub fn new(value: T) -> Self {
        VersionCache {
            inner: Arc::new(VersionCacheInner {
                value: RwLock::new(value),
                version: AtomicU64::new(1),
            }),
        }
    }

    pub fn update(&self, incomming: T) {
        // TODO: add comment about correctness
        {
            let mut value = self.inner.value.write().unwrap();
            *value = incomming;
        }
        self.inner.version.fetch_add(1, Ordering::Relaxed); // TODO: use correct order
    }

    pub fn update_with<F>(&self, mut f: F)
    where
        F: FnMut(&mut T),
    {
        f(&mut self.inner.value.write().unwrap());
        self.inner.version.fetch_add(1, Ordering::Relaxed);
    }

    pub fn getter(&self) -> Getter<T> {
        Getter {
            inner: self.inner.clone(),
            cache: (*self.inner.value.read().unwrap()).clone(),
            version: self.inner.version.load(Ordering::Relaxed),
        }
    }
}

impl<T: Clone> Getter<T> {
    pub fn any_new(&self) -> bool {
        self.version < self.inner.version.load(Ordering::Relaxed)
    }

    pub fn refresh(&mut self) -> bool {
        let v = self.inner.version.load(Ordering::Relaxed);
        if self.version >= v {
            return false;
        }
        self.version = v;
        self.cache = self.inner.value.read().unwrap().clone();
        true
    }

    pub fn setter(&self) -> VersionCache<T> {
        VersionCache {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Deref for Getter<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}
