use crate::call::CallState;
use crate::errors::YDBCError;
use std::collections::VecDeque;
use std::sync::Arc;
use ydb::Query;

pub(crate) struct BindingQueryResult {
    pub err: Option<YDBCError>,
    pub query_result: Option<ydb::QueryResult>,
    pub query_result_set: Option<ydb::ResultSet>,
    pub rows: Option<VecDeque<ydb::Row>>,
}

pub(crate) fn start_query(
    table_client: ydb::TableClient,
    query: String,
) -> Arc<CallState<BindingQueryResult>> {
    let table_client = table_client
        .clone_with_transaction_options(ydb::TransactionOptions::new().with_autocommit(true));

    let (state, sender) = CallState::new();
    let arc_state = Arc::new(state);
    let arc_state_copy = arc_state.clone();

    tokio::spawn(async move {
        let tx_result = table_client
            .retry_transaction(|tx| async {
                let mut tx = tx;
                Ok(tx.query(Query::new(query.clone())).await?)
            })
            .await;

        match tx_result {
            Ok(res) => {
                *arc_state_copy.data.lock().unwrap() = Some(BindingQueryResult {
                    err: None,
                    query_result: Some(res),
                    query_result_set: None,
                    rows: None,
                })
            }
            Err(err) => *arc_state_copy.err.lock().unwrap() = Some(YDBCError::from_err(err)),
        }

        drop(sender);
    });

    arc_state
}
