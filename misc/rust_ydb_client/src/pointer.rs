use std::sync::atomic;
use std::sync::atomic::Ordering::Relaxed;

pub(crate) trait Pointer {
    fn ensure_valid(&self);
    fn free(self);
}

pub(crate) struct CPointer<TData> {
    pub data: TData,
    freed: atomic::AtomicBool,
}

impl<TData> CPointer<TData> {
    pub fn new(data: TData) -> Self {
        Self {
            data,
            freed: atomic::AtomicBool::new(false),
        }
    }
}

impl<TData> Pointer for CPointer<TData> {
    fn ensure_valid(&self) {
        if self.freed.load(Relaxed) {
            panic!("the object freed")
        }
    }

    fn free(self) {
        self.freed.store(true, Relaxed);
        drop(self.data);
    }
}
