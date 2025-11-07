use std::error::Error;

use rand::rngs::StdRng;
use rand::SeedableRng;

use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Result,
};

use super::{validate_constant_param, HashOutput};

pub mod minhasher;
pub mod shingleset;

use minhasher::MinHasher;
use shingleset::ShingleSet;

unsafe fn minhash_invoke_generic<T: HashOutput>(
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
) -> Result<(), Box<dyn Error>> {
    // Prepare `strings` input
    let input_strings = input.flat_vector(0);
    let strings = input_strings
        .as_slice_with_len::<duckdb_string_t>(input.len())
        .iter()
        .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string());

    // Prepare `ngram_width` input
    let ngram_width = validate_constant_param(
        input.flat_vector(1).as_slice_with_len::<usize>(input.len()),
        "ngram_width",
    )?;

    // Prepare `band_count` input
    let band_count = validate_constant_param(
        input.flat_vector(2).as_slice_with_len::<usize>(input.len()),
        "band_count",
    )?;

    // Prepare `band_size` input
    let band_size = validate_constant_param(
        input.flat_vector(3).as_slice_with_len::<usize>(input.len()),
        "band_size",
    )?;

    // Prepare `seed` input
    let seed = validate_constant_param(
        input.flat_vector(4).as_slice_with_len::<u64>(input.len()),
        "seed",
    )?;

    // Prepare output
    let mut output_hashes = output.list_vector();
    let hashes_len_sum: usize = band_count * input.len(); // Initial estimate assuming no NULLs
    let mut hashes_vec = output_hashes.child(hashes_len_sum);
    let hashes: &mut [T] = hashes_vec.as_mut_slice_with_len(hashes_len_sum);

    // Perform hashing
    let mut hash_offset = 0;
    for (row_idx, string) in strings.enumerate().take(input.len()) {
        if input_strings.row_is_null(row_idx as u64) {
            output_hashes.set_null(row_idx);
            continue; // Skip further processing
        }
        let shingle_set = ShingleSet::new(&string, ngram_width, row_idx, None);
        let mut rng = StdRng::seed_from_u64(seed);
        for band_idx in 0..band_count {
            let hasher = MinHasher::new(band_size, &mut rng);
            hashes[hash_offset + band_idx] = T::from_u64(hasher.hash(&shingle_set));
        }
        output_hashes.set_entry(row_idx, hash_offset, band_count);
        hash_offset += band_count;
    }
    output_hashes.set_len(hash_offset); // Corrects initial estimate if NULLs exist

    Ok(())
}

pub struct MinHash {}

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

pub struct MinHash32 {}

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
