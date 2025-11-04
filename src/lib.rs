use std::error::Error;

use duckdb::ffi;
use duckdb::{Connection, Result};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;

pub mod minhash;

use minhash::{MinHash, MinHash32};

trait HashOutput: Copy + 'static {
    fn from_u64(value: u64) -> Self;
}

impl HashOutput for u64 {
    fn from_u64(value: u64) -> Self {
        value
    }
}

impl HashOutput for u32 {
    fn from_u64(value: u64) -> Self {
        value as u32
    }
}

fn validate_constant_param<T: Copy + PartialEq>(
    slice: &[T],
    param_name: &str,
) -> Result<T, Box<dyn Error>> {
    let value = slice[0];
    if !slice.iter().all(|&v| v == value) {
        return Err(format!("{} must be a constant value, not vary per row", param_name).into());
    }
    Ok(value)
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<MinHash>("minhash")
        .expect("Failed to register minhash function");
    con.register_scalar_function::<MinHash32>("minhash32")
        .expect("Failed to register minhash32 function");
    Ok(())
}
