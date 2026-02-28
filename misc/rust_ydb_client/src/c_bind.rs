use crate::call::CallState;
use crate::connect::connect;
use crate::errors::YDBCError;
use crate::pointer::{CPointer, Pointer};
use crate::query::{start_query, BindingQueryResult};
use crate::runtime::runtime_init;
use std::ffi::{c_char, c_int, CStr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

const C_TRUE: c_int = 1;
const C_FALSE: c_int = 0;

fn get_call_state<T>(arg: &CPointer<Mutex<Arc<CallState<T>>>>) -> Arc<CallState<T>> {
    arg.data.lock().unwrap().clone()
}

fn get_locked_value<T>(state: &Arc<CallState<T>>) -> MutexGuard<Option<T>> {
    state.data.lock().unwrap()
}

#[repr(C)]
pub struct YdbConnection {
    client: CPointer<Mutex<Arc<CallState<ydb::Client>>>>,
}

#[no_mangle]
pub extern "C" fn ydb_check_linked_library() -> c_int {
    2
}

#[no_mangle]
pub unsafe extern "C" fn ydb_connect(connection_string: *const c_char) -> Pin<Box<YdbConnection>> {
    let rt = runtime_init();
    let rt_guard = rt.enter();

    let c_str = CStr::from_ptr(connection_string);
    let rust_connection_string = c_str.to_str().expect("connection string was bad");
    let conn_state = connect(rust_connection_string);

    let ydb_connection = YdbConnection {
        client: CPointer::new(Mutex::new(conn_state)),
    };

    drop(rt_guard);
    return Pin::new(Box::new(ydb_connection));
}

#[no_mangle]
pub extern "C" fn ydb_connect_has_result(ydb_connection: &YdbConnection) -> c_int {
    ydb_connection.client.ensure_valid();

    if get_call_state(&ydb_connection.client).is_done() {
        C_TRUE
    } else {
        C_FALSE
    }
}

#[no_mangle]
pub extern "C" fn ydb_connect_wait(ydb_connection: &YdbConnection) -> c_int {
    ydb_connection.client.ensure_valid();
    let state = get_call_state(&ydb_connection.client);
    state.wait_done();
    let has_errors = if let Some(err) = state.err.lock().unwrap().as_ref() {
        println!("{}", err);
        C_TRUE
    } else {
        C_FALSE
    };
    has_errors
}

#[no_mangle]
pub extern "C" fn ydb_connect_free(ydb_connection: Pin<Box<YdbConnection>>) {
    ydb_connection.client.ensure_valid();
    drop(ydb_connection)
}

#[repr(C)]
pub struct YdbResult {
    query_result: CPointer<Mutex<Arc<CallState<BindingQueryResult>>>>,
}

#[no_mangle]
pub unsafe extern "C" fn ydb_query(
    ydb_connection: &YdbConnection,
    query: *const c_char,
) -> Pin<Box<YdbResult>> {
    ydb_connection.client.ensure_valid();

    let rt = runtime_init();
    let rt_guard = rt.enter();

    let c_str = CStr::from_ptr(query);
    let rust_query = c_str.to_str().expect("query string was bad");

    let connection_call_state = get_call_state(&ydb_connection.client);
    connection_call_state.wait_done();

    let table_client = match connection_call_state.get_err() {
        None => get_locked_value(&connection_call_state)
            .as_ref()
            .unwrap()
            .table_client(),
        Some(err) => {
            return Pin::new(Box::new(YdbResult {
                query_result: CPointer::new(Mutex::new(Arc::new(CallState::new_with_error(err)))),
            }))
        }
    };

    let arc_result = start_query(table_client, rust_query.to_string());
    drop(rt_guard);
    return Pin::new(Box::new(YdbResult {
        query_result: CPointer::new(Mutex::new(arc_result)),
    }));
}

#[no_mangle]
pub extern "C" fn ydb_result_free(query_result: Pin<Box<YdbResult>>) {
    query_result.query_result.ensure_valid();

    drop(query_result)
}

#[no_mangle]
pub extern "C" fn ydb_result_has_result(query_result: &YdbResult) -> c_int {
    query_result.query_result.ensure_valid();

    if get_call_state(&query_result.query_result).is_done() {
        C_TRUE
    } else {
        C_FALSE
    }
}

#[no_mangle]
pub extern "C" fn ydb_result_wait(query_result: &YdbResult) {
    query_result.query_result.ensure_valid();

    get_call_state(&query_result.query_result).wait_done()
}

#[no_mangle]
pub extern "C" fn ydb_result_has_errors(query_result: &YdbResult) -> c_int {
    query_result.query_result.ensure_valid();

    if let Some(err) = get_call_state(&query_result.query_result).get_err() {
        println!("{}", err);
        C_TRUE
    } else {
        C_FALSE
    }
}

#[no_mangle]
pub extern "C" fn ydb_result_next_readset(query_result: &mut YdbResult) -> c_int {
    query_result.query_result.ensure_valid();

    let state = get_call_state(&query_result.query_result);
    state.wait_done();

    if state.get_err().is_some() {
        return C_FALSE;
    }

    let mut locked_data = get_locked_value(&state);
    let binding_res = locked_data.as_mut().unwrap();

    if let Some(result) = binding_res.query_result.take() {
        match result.into_only_result() {
            Ok(res) => {
                binding_res.query_result_set = Some(res);
                binding_res.rows = None;
            }
            Err(err) => {
                binding_res.err = Some(YDBCError::from_err(err));
                return C_FALSE;
            }
        }
        C_TRUE
    } else {
        C_FALSE
    }
}

#[no_mangle]
pub extern "C" fn ydb_result_next_row(query_result: &mut YdbResult) -> c_int {
    query_result.query_result.ensure_valid();

    let state = get_call_state(&query_result.query_result);
    if state.get_err().is_some() {
        return C_FALSE;
    };

    let mut bind_res_locked = get_locked_value(&state);
    let bind_res = bind_res_locked.as_mut().unwrap();
    if let Some(result_set) = bind_res.query_result_set.take() {
        bind_res.rows = Some(result_set.rows().collect());
        return C_TRUE;
    };

    if bind_res.rows.is_some() {
        let rows = bind_res.rows.as_mut().unwrap();
        if rows.len() > 0 {
            rows.pop_front();
            return C_TRUE;
        }
    }

    C_FALSE
}

// extern int ydb_result_read_first_field_text(struct YdbResult* res, void* dstBuffer, int dstBufferLen);

#[no_mangle]
pub unsafe extern "C" fn ydb_result_read_first_field_text(
    query_result: &mut YdbResult,
    dst_buffer: *mut c_char,
    dst_buffer_len: c_int,
) -> c_int {
    query_result.query_result.ensure_valid();

    let state = get_call_state(&query_result.query_result);
    state.wait_done();

    if let Some(_) = state.get_err() {
        return C_FALSE;
    };

    let mut bind_res_locked = get_locked_value(&state);
    let bind_res = bind_res_locked.as_mut().unwrap();

    if let Some(rows) = bind_res.rows.as_mut() {
        if let Some(row) = rows.front_mut() {
            if let Ok(field) = row.remove_field(0) {
                let val = match String::try_from(field) {
                    Ok(val) => val,
                    Err(_) => return C_FALSE,
                };
                let val = val.as_bytes();
                if val.len() < dst_buffer_len as usize {
                    unsafe {
                        let our_ptr = val.as_ptr();
                        // This is true for any modern architecture.
                        assert_eq!(std::mem::size_of::<c_char>(), 1);

                        let dst = dst_buffer as *mut c_char;
                        std::ptr::copy_nonoverlapping(our_ptr, dst.cast(), val.len());
                        *dst.add(val.len()) = 0;
                    }
                } else {
                    println!(
                        "small buffer. buffer size: {}, need: {}",
                        dst_buffer_len,
                        val.len() + 1
                    );
                }
            } else {
                println!("no field")
            }
        } else {
            println!("no rows")
        }
    } else {
        println!("rows is None");
    };

    C_FALSE
}
