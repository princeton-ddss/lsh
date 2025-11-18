use nohash_hasher::IntSet;
use std::hash::{Hash, Hasher};

use rustc_hash::FxHasher;

#[derive(Debug, Clone)]
pub struct ShingleSet {
    pub shingles: IntSet<u32>,
}

impl ShingleSet {
    pub fn from_shingles(shingles: &[&str], salt: Option<&str>) -> Self {
        let mut out_set: IntSet<u32> = IntSet::default();

        for shin in shingles {
            let char_vec: Vec<char> = shin.chars().collect();
            let result = Self::hash_chars(&char_vec, salt);
            out_set.insert(result);
        }

        Self { shingles: out_set }
    }

    pub fn from_text(text: &str, ngram_width: usize, salt: Option<&str>) -> Self {
        let mut out_set: IntSet<u32> = IntSet::default();

        let char_vec: Vec<char> = text.chars().collect();

        for window in char_vec.windows(ngram_width) {
            let result = Self::hash_chars(window, salt);
            out_set.insert(result);
        }

        Self { shingles: out_set }
    }

    fn hash_chars(chars: &[char], salt: Option<&str>) -> u32 {
        let mut hasher = FxHasher::default();

        if let Some(salt_str) = salt {
            salt_str.hash(&mut hasher);
        };

        chars.hash(&mut hasher);

        hasher.finish() as u32
    }

    #[inline]
    pub fn jaccard_similarity(&self, b: &Self) -> f64 {
        if self.shingles.is_empty() | b.shingles.is_empty() {
            0.0
        } else {
            self.shingles.intersection(&b.shingles).count() as f64
                / self.shingles.union(&b.shingles).count() as f64
        }
    }
}
