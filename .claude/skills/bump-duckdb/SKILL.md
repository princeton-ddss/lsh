---
name: bump-duckdb
description: Bump all DuckDB version references to adapt the extension to a new DuckDB release
argument-hint: <new-duckdb-version e.g. v1.5.2>
allowed-tools: Read, Edit, Bash(git *), Bash(cargo *)
---

# Bump DuckDB Version

Adapt this extension to DuckDB version `$ARGUMENTS` (denoted `vX.Y.Z` below).

## Derived versions

- **Crate version**: `1.{X*10000 + Y*100 + Z}.0` (e.g. `v1.5.1` → `1.10501.0`)
- **New extension version**: read `[package] version` in `Cargo.toml` and increment patch by 1

## Steps

### 1. Probe availability

Run both checks, then branch on results:

```bash
cargo search duckdb | grep '^duckdb = "1\.{X*10000+Y*100+Z}\.0"'
git -C extension-ci-tools fetch --tags
git -C extension-ci-tools tag -l vX.Y.Z
```

- **Crate not published** → **abort**. Report: "Crate `1.{...}.0` not on crates.io yet; wait for the release." Do not modify any files.
- **`extension-ci-tools` tag missing** → continue, but mark CI-tooling updates as _skipped_ (see step 2).

### 2. Apply edits

| File                                             | Change                                                                  | Skip if `extension-ci-tools` tag missing? |
| ------------------------------------------------ | ----------------------------------------------------------------------- | ----------------------------------------- |
| `.github/workflows/MainDistributionPipeline.yml` | `duckdb_version: vX.Y.Z`                                                | no                                        |
| `.github/workflows/MainDistributionPipeline.yml` | `uses: duckdb/extension-ci-tools/...@vX.Y.Z`                            | yes                                       |
| `.github/workflows/MainDistributionPipeline.yml` | `ci_tools_version: vX.Y.Z`                                              | yes                                       |
| `Cargo.toml`                                     | `[package] version` → new extension version                             | no                                        |
| `Cargo.toml`                                     | `duckdb`, `duckdb-loadable-macros`, `libduckdb-sys` → new crate version | no                                        |
| `Makefile`                                       | `TARGET_DUCKDB_VERSION=vX.Y.Z`                                          | no                                        |

Then run:

```bash
cargo update duckdb duckdb-loadable-macros libduckdb-sys   # always
git -C extension-ci-tools checkout vX.Y.Z                  # skip if tag missing
```

### 3. Validate

Re-read every file modified in step 2 and confirm the new values are present. Verify the submodule tag (only if checked out):

```bash
git -C extension-ci-tools describe --tags
```

Report any mismatch as an error.

### 4. Summarize

Report:

- What was updated
- What was skipped, with reason
- If anything was skipped: remind the user to re-run this skill once the missing release is available

## Why some updates can be skipped

The CI tooling (`extension-ci-tools` submodule + workflow `uses:` ref + `ci_tools_version`) just downloads the DuckDB binary specified by `duckdb_version`, so older tooling works fine with a newer DuckDB. Everything else is compiled into the extension binary and must match exactly to avoid ABI mismatches.
