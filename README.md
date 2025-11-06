# Locality-Sensitive Hashing (LSH) DuckDB Extension

DuckDB extension for [locality-sensitive hashing (LSH)](https://en.wikipedia.org/wiki/Locality-sensitive_hashing),
using the Rust implementations from [`zoomerjoin`](https://github.com/beniaminogreen/zoomerjoin).

## Cloning

Clone the repo with submodules

```shell
git clone --recurse-submodules <repo>
```

## Dependencies
In principle, these extensions can be compiled with the Rust toolchain alone. However, this template relies on some additional
tooling to make life a little easier and to be able to share CI/CD infrastructure with extension templates for other languages:

- Python3
- Python3-venv
- [Make](https://www.gnu.org/software/make)
- Git

Installing these dependencies will vary per platform:
- For Linux, these come generally pre-installed or are available through the distro-specific package manager.
- For MacOS, [homebrew](https://formulae.brew.sh/).
- For Windows, [chocolatey](https://community.chocolatey.org/).

## Building
After installing the dependencies, building is a two-step process. Firstly run:
```shell
make configure
```
This will ensure a Python venv is set up with DuckDB and DuckDB's test runner installed. Additionally, depending on configuration,
DuckDB will be used to determine the correct platform for which you are compiling.

Then, to build the extension run:
```shell
make debug
```
This delegates the build process to cargo, which will produce a shared library in `target/debug/<shared_lib_name>`. After this step,
a script is run to transform the shared library into a loadable extension by appending a binary footer. The resulting extension is written
to the `build/debug` directory.

To create optimized release binaries, simply run `make release` instead.

### Running the extension
To run the extension code, start `duckdb` with `-unsigned` flag. This will allow you to load the local extension file.

```sh
duckdb -unsigned
```

After loading the extension by the file path, you can use the functions provided by the extension:

#### MinHash

- 64-bit: `lsh_min(string, ngram_width, band_count, band_size, seed)`
- 32-bit: `lsh_min32(string, ngram_width, band_count, band_size, seed)`

```sql
LOAD './build/debug/extension/lsh/lsh.duckdb_extension';

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

#### Euclidean Hashing

- 64-bit: `lsh_euclidean(string, bucket_width, band_count, band_size, seed)`
- 32-bit: `lsh_euclidean32(string, bucket_width, band_count, band_size, seed)`

```sql
LOAD './build/debug/extension/lsh/lsh.duckdb_extension';

CREATE OR REPLACE TEMPORARY TABLE temp_vals (
    val DOUBLE[],
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

### Known issues
This is a bit of a footgun, but the extensions produced by this template may (or may not) be broken on windows on python3.11
with the following error on extension load:
```shell
IO Error: Extension '<name>.duckdb_extension' could not be loaded: The specified module could not be found
```
This was resolved by using python 3.12
