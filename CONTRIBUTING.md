# Contributing to `lsh`

## Cloning

Clone the repo with submodules:

```shell
git clone --recurse-submodules <repo>
```

## Dependencies

In principle, the extension can be compiled with the Rust toolchain alone. However, the repo
relies on some additional tooling to make life a little easier and to be able to share CI/CD
infrastructure with extensions in other languages:

- Python3
- Python3-venv
- [Make](https://www.gnu.org/software/make)
- Git

Installing these dependencies will vary by platform:

- For Linux, these come generally pre-installed or are available through the distro-specific package manager.
- For MacOS, [homebrew](https://formulae.brew.sh/).
- For Windows, [chocolatey](https://community.chocolatey.org/).

## Building

After installing the dependencies, building is a two-step process. Firstly run:

```shell
make configure
```

This will ensure a Python `venv` is set up with DuckDB and DuckDB's test runner installed. Additionally,
depending on configuration, DuckDB will be used to determine the correct platform for compiling.

Then, to build the extension run:

```shell
make debug
```

This delegates the build process to Cargo, which will produce a shared library in `target/debug/<shared_lib_name>`.
After this step, a script is run to transform the shared library into a loadable extension by appending a binary footer.
The resulting extension is written to the `build/debug` directory.

To create optimized release binaries, simply run `make release` instead.

## Running

To run the extension code, start `duckdb` with `-unsigned` flag:

```shell
duckdb -unsigned
```

This will allow loading the local extension file, like so:

```sql
LOAD './build/debug/extension/lsh/lsh.duckdb_extension';
-- or for optimized release binaries
LOAD './build/release/extension/lsh/lsh.duckdb_extension';
```

We can then run functions available in the extension:

```sql
SELECT lsh_min('Princeton University', 2, 3, 2, 123) AS example_hash;
```

```
┌──────────────────────────────────────────────────────────────────┐
│                           example_hash                           │
│                             uint64[]                             │
├──────────────────────────────────────────────────────────────────┤
│ [6891191098855684803, 6484452798683863108, 14488917645112899542] │
└──────────────────────────────────────────────────────────────────┘
```

## Development

New LSH functions can be added under `src/`. Check existing implementations for reference.

## Testing

The extension uses the DuckDB Python client for testing. This should be automatically
installed in the `make configure` step. The tests themselves are written in the
[SQLLogicTest](https://duckdb.org/docs/stable/dev/sqllogictest/intro) format, just like
most of DuckDB's tests. Existing tests can be found in `test/sql/lsh/`.

To run the tests:

```shell
make test_debug
## or for optimized release binaries
make test_release
```
