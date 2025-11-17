# Locality-Sensitive Hashing (LSH) DuckDB Extension

DuckDB extension for [locality-sensitive hashing (LSH)](https://en.wikipedia.org/wiki/Locality-sensitive_hashing),
using the Rust implementations from the [`zoomerjoin`](https://github.com/beniaminogreen/zoomerjoin) R package.
(For a conceptual review and a description of that package,
see [https://doi.org/10.21105/joss.05693](https://doi.org/10.21105/joss.05693).)

## Installation

`lsh` is a [DuckDB Community Extension](https://github.com/duckdb/community-extensions).

It can be installed and loaded in DuckDB like so:

```sql
INSTALL lsh FROM community;
LOAD lsh;
```

## Available Functions

### MinHash (for Strings)

- 64-bit: `lsh_min(string, ngram_width, band_count, band_size, seed)`
- 32-bit: `lsh_min32(string, ngram_width, band_count, band_size, seed)`

```sql
CREATE TEMPORARY TABLE temp_names (
    name VARCHAR
);

INSERT INTO temp_names (name) VALUES
    ('Alice Johnson'),
    ('Robert Smith'),
    (NULL),
    ('Charlotte Brown'),
    ('David Martinez'),
    ('Emily Davis'),
    ('Michael Wilson'),
    ('Sophia Taylor'),
    (NULL),
    ('James Anderson'),
    ('Olivia Thomas'),
    ('Benjamin Lee');

SELECT lsh_min(name, 2, 3, 2, 123) AS hash FROM temp_names;
```

```
┌──────────────────────────────────────────────────────────────────┐
│                               hash                               │
│                             uint64[]                             │
├──────────────────────────────────────────────────────────────────┤
│ [13571929851950895096, 9380027513982184887, 2973452616913389687] │
│ [8779492002049334510, 6213046290947405081, 13321761559668221936] │
│ NULL                                                             │
│ [17147317566672094549, 9868884775472345505, 9544039307031965287] │
│ [8205471107123956470, 3856457550471365223, 160978381860159594]   │
│ [5031590273592478399, 2643794611755346220, 10496886524478706543] │
│ [7351019434982270461, 11969544284460938578, 1096653296545732983] │
│ [947309311728102588, 6485027977500841069, 11465726828575944543]  │
│ NULL                                                             │
│ [6511242524203601686, 5368660891928216176, 4531328875985401258]  │
│ [6134578107120707744, 8471287122008225606, 13561556383590060017] │
│ [7926739398273580158, 2501438919389423193, 17085734390799214704] │
├──────────────────────────────────────────────────────────────────┤
│                             12 rows                              │
└──────────────────────────────────────────────────────────────────┘
```

### Euclidean Hashing (for Points)

- 64-bit: `lsh_euclidean(array, bucket_width, band_count, band_size, seed)`
- 32-bit: `lsh_euclidean32(array, bucket_width, band_count, band_size, seed)`

```sql
CREATE OR REPLACE TEMPORARY TABLE temp_vals (
    val DOUBLE[5],
);

INSERT INTO temp_vals (val) VALUES
    (ARRAY[1.1, 2.2, 3.3, 5.8, 3.9]),
    (NULL),
    (ARRAY[4.5, 5.5, 2.3, 1.8, 6.3]),
    (ARRAY[7.3, 1.2, 9.6, 5.5, 7.8]),
    (NULL),
    (ARRAY[6.7, 4.5, 1.3, 0.6, 4.6]);

SELECT lsh_euclidean(val, 0.5, 2, 3, 123) AS hash FROM temp_vals;
```

```
┌─────────────────────────────────────────────┐
│                    hash                     │
│                  uint64[]                   │
├─────────────────────────────────────────────┤
│ [4153593470791884295, 13333357882440433242] │
│ NULL                                        │
│ [9539244981710099531, 8978554412800410753]  │
│ [3765229951789618540, 13117769235920535871] │
│ NULL                                        │
│ [13333357882440433242, 2643848813970240984] │
└─────────────────────────────────────────────┘
```

### Jaccard Similarity (for Strings)

- `lsh_jaccard(string_left, string_right, ngram_width)`

```sql
CREATE TEMPORARY TABLE temp_names (
    name_a VARCHAR,
    name_b VARCHAR
);

INSERT INTO temp_names (name_a, name_b) VALUES
    ('Charlotte Brown', 'Charlene Browning'),
    ('David Martinez', 'Davis Martin'),
    ('Olivia Thomas', 'Olive Thomason'),
    ('Alice Johnson', NULL),
    (NULL, 'Roberta Mills'),
    ('Emily Davis', 'Laura Bennett'),
    ('Michael Wilson', 'Mike Wilson'),
    ('James Anderson', 'Jamie Anders'),
    ('Sophia Taylor', NULL),
    ('Benjamin Lee', 'Christopher Grant');

SELECT lsh_jaccard(name_a, name_b, 2) AS similarity FROM temp_names;
```

```
┌────────────┐
│ similarity │
│   double   │
├────────────┤
│        0.5 │
│        0.6 │
│     0.5625 │
│       NULL │
│       NULL │
│        0.0 │
│     0.4375 │
│        0.5 │
│       NULL │
│        0.0 │
├────────────┤
│  10 rows   │
└────────────┘
```

## Suggested Usage

We do not recommend creating and storing the full `ARRAY::[band_count]`-type columns,
as they become large very quickly. Instead, we recommend generating bands on-the-fly
in join conditions (i.e., when generating comparisons/potential matches). This reduces
storage needs and memory consumption. Further, we note that statements generating a set
of *unique* row pairs based on the output of these functions may be slower than producing
comparison pairs *then filtering to matches* within each band (*then* taking the union)
if the filtering/comparison function(s) are not computationally intensive.

For example, to identify record pairs satisfying `Jaccard(A.col, B.col) > 0.8` between
tables `A` and `B` using bigram MinHashing (`band_count = 2, band_size = 3`) to generate
comparison pairs, we recommend the following syntax, where each call to `lsh_min()` produces
a single-element array. Holding the seed fixed within join calls and rotating it across
calls fixes the hash functions *within* each join but effectively produces additional bands
*across* each join.

```sql
SELECT A.ind, B.id
FROM A
INNER JOIN B
ON lsh_min(A.col, 2, 1, 3, 1)[1] = lsh_min(A.col, 2, 1, 3, 1)[1]
WHERE lsh_jaccard(A.col, B.col, 2) > 0.8

UNION

SELECT A.ind, B.id
FROM A
INNER JOIN B
ON lsh_min(A.col, 2, 1, 3, 2)[1] = lsh_min(A.col, 2, 1, 3, 2)[1]
WHERE lsh_jaccard(A.col, B.col, 2) > 0.8
```
