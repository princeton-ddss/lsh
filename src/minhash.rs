use std::error::Error;

use rand::rngs::StdRng;
use rand::SeedableRng;

use duckdb::ffi::{duckdb_list_entry, duckdb_string_t};
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

unsafe fn minhash_from_text<T: HashOutput>(
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
) -> Result<(), Box<dyn Error>> {
    // Prepare text input
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
    for (row_idx, string) in strings.enumerate() {
        if input_strings.row_is_null(row_idx as u64) {
            output_hashes.set_null(row_idx);
            continue; // Skip to the next row
        }
        let shingle_set = ShingleSet::from_text(&string, ngram_width, None);
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

unsafe fn minhash_from_shingles<T: HashOutput>(
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
) -> Result<(), Box<dyn Error>> {
    // Prepare shingles array input
    let input_arrays_meta = input.flat_vector(0);
    let input_arrays_data = input.list_vector(0);
    let arrays_meta = input_arrays_meta.as_slice_with_len::<duckdb_list_entry>(input.len());
    let arrays_vec = input_arrays_data.child(input_arrays_data.len());
    let arrays: Vec<String> = arrays_vec
        .as_slice_with_len::<duckdb_string_t>(input_arrays_data.len())
        .iter()
        .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string())
        .collect();

    // Prepare `band_count` input
    let band_count = validate_constant_param(
        input.flat_vector(1).as_slice_with_len::<usize>(input.len()),
        "band_count",
    )?;

    // Prepare `band_size` input
    let band_size = validate_constant_param(
        input.flat_vector(2).as_slice_with_len::<usize>(input.len()),
        "band_size",
    )?;

    // Prepare `seed` input
    let seed = validate_constant_param(
        input.flat_vector(3).as_slice_with_len::<u64>(input.len()),
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
            continue; // Skip to the next row
        }

        let arr_offset = meta.offset as usize;
        let arr_length = meta.length as usize;
        let arr = &arrays[arr_offset..(arr_offset + arr_length)];
        let arr_refs: Vec<&str> = arr.iter().map(|s| s.as_str()).collect();
        let shingle_set = ShingleSet::from_shingles(&arr_refs, None);

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
        match input.flat_vector(0).logical_type().id() {
            LogicalTypeId::Varchar => minhash_from_text::<u64>(input, output),
            LogicalTypeId::List => minhash_from_shingles::<u64>(input, output),
            _ => Err("Unsupported argument type for MinHash".into()),
        }
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                ],
                LogicalTypeHandle::list(&LogicalTypeId::UBigint.into()),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::list(&LogicalTypeId::Varchar.into()),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                ],
                LogicalTypeHandle::list(&LogicalTypeId::UBigint.into()),
            ),
        ]
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
        match input.flat_vector(0).logical_type().id() {
            LogicalTypeId::Varchar => minhash_from_text::<u32>(input, output),
            LogicalTypeId::List => minhash_from_shingles::<u32>(input, output),
            _ => Err("Unsupported argument type for MinHash".into()),
        }
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                ],
                LogicalTypeHandle::list(&LogicalTypeId::UInteger.into()),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::list(&LogicalTypeId::Varchar.into()),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                    LogicalTypeId::UBigint.into(),
                ],
                LogicalTypeHandle::list(&LogicalTypeId::UInteger.into()),
            ),
        ]
    }
}

pub struct JaccardSimilarity {}

impl VScalar for JaccardSimilarity {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        // Prepare `strings_left` input
        let input_strings_left = input.flat_vector(0);
        let strings_left = input_strings_left
            .as_slice_with_len::<duckdb_string_t>(input.len())
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string());

        // Prepare `strings_right` input
        let input_strings_right = input.flat_vector(1);
        let strings_right = input_strings_right
            .as_slice_with_len::<duckdb_string_t>(input.len())
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string());

        // Prepare `ngram_width` input
        let ngram_width = validate_constant_param(
            input.flat_vector(2).as_slice_with_len::<usize>(input.len()),
            "ngram_width",
        )?;

        // Calculate Jaccard similarity for each pair
        let mut output_measures = output.flat_vector();
        for (row_idx, (s_left, s_right)) in strings_left.zip(strings_right).enumerate() {
            if input_strings_left.row_is_null(row_idx as u64)
                || input_strings_right.row_is_null(row_idx as u64)
            {
                output_measures.set_null(row_idx);
                continue; // Skip to the next row
            }

            let shingle_set_left = ShingleSet::from_text(&s_left, ngram_width, None);
            let shingle_set_right = ShingleSet::from_text(&s_right, ngram_width, None);

            let measures = output_measures.as_mut_slice_with_len::<f64>(input.len());
            measures[row_idx] = shingle_set_left.jaccard_similarity(&shingle_set_right);
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::UBigint.into(),
            ],
            LogicalTypeId::Double.into(),
        )]
    }
}
