use std::error::Error;

use rand::rngs::StdRng;
use rand::SeedableRng;

use duckdb::ffi;
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;

pub mod minihasher;
pub mod shingleset;

use crate::minihasher::MinHasher;
use crate::shingleset::ShingleSet;

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

unsafe fn minhash_invoke_generic<T: HashOutput>(
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
) -> Result<(), Box<dyn Error>> {
    let input_strings = input.flat_vector(0);
    let input_ngram_width = input.flat_vector(1);
    let input_band_count = input.flat_vector(2);
    let input_band_size = input.flat_vector(3);
    let input_seed = input.flat_vector(4);

    let strings = input_strings
        .as_slice_with_len::<duckdb_string_t>(input.len())
        .iter()
        .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string());

    let ngram_width = validate_constant_param(
        input_ngram_width.as_slice_with_len::<usize>(input.len()),
        "ngram_width",
    )?;

    let band_count = validate_constant_param(
        input_band_count.as_slice_with_len::<usize>(input.len()),
        "band_count",
    )?;

    let band_size = validate_constant_param(
        input_band_size.as_slice_with_len::<usize>(input.len()),
        "band_size",
    )?;

    let seed = validate_constant_param(input_seed.as_slice_with_len::<u64>(input.len()), "seed")?;

    let mut output_hashes = output.list_vector();
    let total_len: usize = band_count * input.len();
    let mut hashes_vec = output_hashes.child(total_len);

    let hashes: &mut [T] = hashes_vec.as_mut_slice_with_len(total_len);
    let mut offset = 0;
    for (row_idx, string) in strings.enumerate().take(input.len()) {
        let shingle_set = ShingleSet::new(&string, ngram_width, row_idx, None);
        let mut rng = StdRng::seed_from_u64(seed);
        for band_idx in 0..band_count {
            let hasher = MinHasher::new(band_size, &mut rng);
            hashes[offset + band_idx] = T::from_u64(hasher.hash(&shingle_set));
        }
        output_hashes.set_entry(row_idx, offset, band_count);
        offset += band_count;
    }
    output_hashes.set_len(input.len());

    Ok(())
}

struct MinHash {}

impl VScalar for MinHash {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        minhash_invoke_generic::<u64>(input, output)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
            ],
            LogicalTypeHandle::list(&LogicalTypeId::UBigint.into()),
        )]
    }
}

struct MinHash32 {}

impl VScalar for MinHash32 {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        minhash_invoke_generic::<u32>(input, output)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
            ],
            LogicalTypeHandle::list(&LogicalTypeId::UInteger.into()),
        )]
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<MinHash>("minhash")
        .expect("Failed to register minhash function");
    con.register_scalar_function::<MinHash32>("minhash32")
        .expect("Failed to register minhash32 function");
    Ok(())
}
