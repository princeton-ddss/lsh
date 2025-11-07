use std::error::Error;

use rand::rngs::StdRng;
use rand::SeedableRng;

use duckdb::ffi::duckdb_list_entry;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Result,
};

use super::{validate_constant_param, HashOutput};

pub mod euclidean_hasher;

use euclidean_hasher::EuclideanHasher;

unsafe fn euclidean_hash_invoke_generic<T: HashOutput>(
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
) -> Result<(), Box<dyn Error>> {
    // Prepare `arrays` input
    let input_arrays_meta = input.flat_vector(0);
    let input_arrays_data = input.list_vector(0);
    let arrays_meta = input_arrays_meta.as_slice_with_len::<duckdb_list_entry>(input.len());
    let arrays_len_max = arrays_meta.iter().map(|meta| meta.length).max().unwrap();
    for (row_idx, meta) in arrays_meta.iter().enumerate() {
        if !input_arrays_meta.row_is_null(row_idx as u64) {
            if meta.length != arrays_len_max {
                return Err("All input arrays must have the same length".into());
            }
        }
    }
    let arrays_len_sum = arrays_meta.iter().map(|meta| meta.length).sum::<u64>() as usize;
    let arrays_vec = input_arrays_data.child(arrays_len_sum);
    let arrays: &[f64] = arrays_vec.as_slice_with_len(arrays_len_sum);

    // Prepare `bucket_width` input
    let bucket_width = validate_constant_param(
        input.flat_vector(1).as_slice_with_len::<f64>(input.len()),
        "bucket_width",
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
    for (row_idx, meta) in arrays_meta.iter().enumerate() {
        if input_arrays_meta.row_is_null(row_idx as u64) {
            output_hashes.set_null(row_idx);
            continue; // Skip further processing
        }
        let arr_offset = meta.offset as usize;
        let arr_length = meta.length as usize;
        let mut rng = StdRng::seed_from_u64(seed);
        for band_idx in 0..band_count {
            let hasher = EuclideanHasher::new(bucket_width, band_size, arr_length, &mut rng);
            let arr = &arrays[arr_offset..(arr_offset + arr_length)];
            hashes[hash_offset + band_idx] = T::from_u64(hasher.hash(arr.into()));
        }
        output_hashes.set_entry(row_idx, hash_offset, band_count);
        hash_offset += band_count;
    }
    output_hashes.set_len(hash_offset); // Corrects initial estimate if NULLs exist

    Ok(())
}

pub struct EuclideanHash {}

impl VScalar for EuclideanHash {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        euclidean_hash_invoke_generic::<u64>(input, output)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::list(&LogicalTypeId::Double.into()),
                LogicalTypeId::Double.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
            ],
            LogicalTypeHandle::list(&LogicalTypeId::UBigint.into()),
        )]
    }
}

pub struct EuclideanHash32 {}

impl VScalar for EuclideanHash32 {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        euclidean_hash_invoke_generic::<u32>(input, output)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::list(&LogicalTypeId::Double.into()),
                LogicalTypeId::Double.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
                LogicalTypeId::UBigint.into(),
            ],
            LogicalTypeHandle::list(&LogicalTypeId::UInteger.into()),
        )]
    }
}
