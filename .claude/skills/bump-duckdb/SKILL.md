---
name: bump-duckdb
description: Bump all DuckDB version references to adapt the extension to a new DuckDB release
disable-model-invocation: true
argument-hint: <new-duckdb-version e.g. v1.5.2>
allowed-tools: Read, Edit, Bash(git *), Bash(cargo update *)
---

# Bump DuckDB Version

Adapt this extension to a new DuckDB release version `$ARGUMENTS`.

## Version Derivation

Given a DuckDB version like `vX.Y.Z`:
- **DuckDB version**: `vX.Y.Z` (used in CI workflow and Makefile)
- **Rust crate version**: `1.{X * 10000 + Y * 100 + Z}.0` (e.g., `v1.5.1` → `1.10501.0`)
- **Extension version**: read the current version from `Cargo.toml` `[package] version` and increment its patch number by 1

## Files to Update

### 1. `.github/workflows/MainDistributionPipeline.yml`
Update all three version references:
- The workflow `uses:` ref: `duckdb/extension-ci-tools/...@vX.Y.Z`
- `duckdb_version: vX.Y.Z`
- `ci_tools_version: vX.Y.Z`

### 2. `Cargo.toml`
- Bump `[package] version` (increment patch by 1)
- Update `duckdb` dependency version to the new crate version
- Update `duckdb-loadable-macros` dependency version to the new crate version
- Update `libduckdb-sys` dependency version to the new crate version

### 3. `Makefile`
- Update `TARGET_DUCKDB_VERSION=vX.Y.Z`

### 4. `extension-ci-tools` submodule
Run: `git -C extension-ci-tools fetch --tags && git -C extension-ci-tools checkout vX.Y.Z`

### 5. `Cargo.lock`
Run: `cargo update duckdb duckdb-loadable-macros libduckdb-sys`

## Procedure

1. Parse the DuckDB version from `$ARGUMENTS` and derive the crate version and new extension version
2. Read the current files to confirm current values
3. Apply all edits
4. Update the submodule
5. Update Cargo.lock
6. **Validate**: Re-read all updated files and verify that:
   - `.github/workflows/MainDistributionPipeline.yml` contains the new DuckDB version in all three places and no references to the old version
   - `Cargo.toml` has the new crate version for `duckdb`, `duckdb-loadable-macros`, and `libduckdb-sys`
   - `Cargo.lock` has the new crate version for `duckdb`, `duckdb-loadable-macros`, and `libduckdb-sys`
   - `Makefile` has the new `TARGET_DUCKDB_VERSION`
   - `extension-ci-tools` submodule points to the new version tag (verify with `git -C extension-ci-tools describe --tags`)
   - Report any mismatches as errors
7. Show a summary of all changes made
