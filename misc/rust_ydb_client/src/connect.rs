use crate::call::CallState;
use crate::errors::YDBCError;
use std::sync::Arc;
use std::time::Duration;

pub(crate) const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn connect(connection_string: &str) -> Arc<CallState<ydb::Client>> {
    let builder = match ydb::ClientBuilder::new_from_connection_string(connection_string) {
        Err(err) => return Arc::new(CallState::new_with_error(YDBCError::from_err(err))),
        Ok(builder) => builder,
    };

    let client = match builder.client() {
        Err(err) => return Arc::new(CallState::new_with_error(YDBCError::from_err(err))),
        Ok(client) => client,
    };

    let (state, sender) = CallState::new();
    let arc_state = Arc::new(state);

    let arc_state_copy = arc_state.clone();

    tokio::spawn(async move {
        let res = match tokio::time::timeout(OPERATION_TIMEOUT, client.wait()).await {
            Ok(client_wait_result) => match client_wait_result {
                Ok(_) => Ok(client),
                Err(err) => Err(YDBCError::from_err(err)),
            },
            Err(_) => Err(YDBCError::new("operation timeout")),
        };

        match res {
            Ok(client) => *arc_state_copy.data.lock().unwrap() = Some(client),
            Err(err) => *arc_state_copy.err.lock().unwrap() = Some(err),
        }

        drop(sender);
    });

    return arc_state;
}
