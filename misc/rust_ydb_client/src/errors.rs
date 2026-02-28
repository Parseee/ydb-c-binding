use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

#[allow(dead_code)]
pub(crate) type YDBCResult<T> = Result<T, YDBCError>;

pub(crate) struct YDBCError {
    message: String,
}

impl YDBCError {
    pub fn new<T: Into<String>>(message: T) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn from_err<T: Error>(err: T) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

impl Clone for YDBCError {
    fn clone(&self) -> Self {
        Self {
            message: self.message.clone(),
        }
    }
}

impl Debug for YDBCError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl Display for YDBCError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for YDBCError {}
