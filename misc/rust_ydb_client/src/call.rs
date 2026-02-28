use crate::errors::YDBCError;
use std::sync::mpsc::{SyncSender, TryRecvError};
use std::sync::{mpsc, Mutex};

pub(crate) struct CallState<T> {
    event: Mutex<mpsc::Receiver<Empty>>,
    pub err: Mutex<Option<YDBCError>>,
    pub data: Mutex<Option<T>>,
}

impl<T> CallState<T> {
    pub fn new() -> (Self, SyncSender<Empty>) {
        let (sender, receiver) = mpsc::sync_channel(0);

        (
            Self {
                event: Mutex::new(receiver),
                err: Mutex::new(None),
                data: Mutex::new(None),
            },
            sender,
        )
    }

    pub fn new_with_error(err: YDBCError) -> Self {
        let (_, receiver) = mpsc::sync_channel(0);
        Self {
            event: Mutex::new(receiver),
            err: Mutex::new(Some(err)),
            data: Mutex::new(None),
        }
    }

    #[allow(dead_code)]
    pub fn new_with_result(res: T) -> Self {
        let (_, receiver) = mpsc::sync_channel(0);
        Self {
            event: Mutex::new(receiver),
            err: Mutex::new(None),
            data: Mutex::new(Some(res)),
        }
    }

    pub fn is_done(&self) -> bool {
        if let Err(TryRecvError::Disconnected) = self.event.lock().unwrap().try_recv() {
            true
        } else {
            false
        }
    }

    pub fn wait_done(&self) {
        _ = self.event.lock().unwrap().recv()
    }

    pub fn get_err(&self) -> Option<YDBCError> {
        self.err.lock().unwrap().clone()
    }
}

pub(crate) struct Empty {}
