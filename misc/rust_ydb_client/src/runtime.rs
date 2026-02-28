use std::sync::{Arc, Mutex, Once};

static RUNTIME: Mutex<Option<Arc<tokio::runtime::Runtime>>> = Mutex::new(None);

static RUNTIME_ONCE: Once = Once::new();

pub(crate) fn runtime_init() -> Arc<tokio::runtime::Runtime> {
    RUNTIME_ONCE.call_once(|| {
        let mut rt_opt = RUNTIME.lock().unwrap();
        *rt_opt = Some(Arc::new(tokio::runtime::Runtime::new().unwrap()));
    });

    let m_guard = RUNTIME.lock().unwrap();
    let arc_runtime = m_guard.clone().unwrap();
    arc_runtime
}
