use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Default)]
struct VersionCacheInner<T> {
    value: RwLock<T>,
    version: AtomicU64,
}

#[derive(Clone, Default)]
pub struct VersionCache<T> {
    inner: Arc<VersionCacheInner<T>>,
}

#[derive(Clone, Default)]
pub struct Getter<T: Clone> {
    inner: Arc<VersionCacheInner<T>>,
    cache: T,
    version: u64,
}

#[derive(Clone, Default)]
pub struct Observer<T> {
    inner: Arc<VersionCacheInner<T>>,
    version: u64,
}

impl<T> VersionCacheInner<T> {
    fn any_new(&self, version: u64) -> (bool, u64) {
        let v = self.version.load(Ordering::Relaxed);
        if version < v {
            (true, v)
        } else {
            (false, version)
        }
    }
}

impl<T> VersionCache<T> {
    pub fn new(value: T) -> Self {
        VersionCache {
            inner: Arc::new(VersionCacheInner {
                value: RwLock::new(value),
                version: AtomicU64::new(1),
            }),
        }
    }

    /// Replace the whole value
    pub fn replace(&self, incomming: T) {
        // The update of `value` and `version` is not atomic
        // reader may read a updated `value` with stale `version`
        // which cause an addition read to update `version`
        {
            let mut value = self.inner.value.write().unwrap();
            *value = incomming;
        }
        self.inner.version.fetch_add(1, Ordering::Relaxed); // TODO: use correct order
    }

    /// Update partial of the value
    pub fn update<F>(&self, f: F)
    where
        F: Fn(&mut T),
    {
        f(&mut self.inner.value.write().unwrap());
        self.inner.version.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observer(&self) -> Observer<T> {
        Observer {
            inner: self.inner.clone(),
            version: self.inner.version.load(Ordering::Relaxed),
        }
    }
}

impl<T: Clone> VersionCache<T> {
    pub fn getter(&self) -> Getter<T> {
        Getter {
            inner: self.inner.clone(),
            cache: (*self.inner.value.read().unwrap()).clone(),
            version: self.inner.version.load(Ordering::Relaxed),
        }
    }
}

impl<T> Observer<T> {
    /// Observe the change of value without clone the whole value
    /// return true means there have new value
    pub fn observe<F>(&mut self, mut f: F) -> bool
    where
        F: FnMut(&T),
    {
        match self.inner.any_new(self.version) {
            (true, v) => {
                self.version = v;
                f(&self.inner.value.read().unwrap());
                true
            }
            _ => false,
        }
    }
}

impl<T: Clone> Getter<T> {
    /// Refresh the cache to the newest value if there are any.
    /// return true means there have new value
    pub fn refresh(&mut self) -> bool {
        match self.inner.any_new(self.version) {
            (true, v) => {
                self.version = v;
                self.cache = self.inner.value.read().unwrap().clone();
                true
            }
            _ => false,
        }
    }
}

impl<T: Clone> Deref for Getter<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}
