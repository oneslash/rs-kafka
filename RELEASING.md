# Releasing

Releases are created from GitHub Actions. The release workflow publishes the crate to crates.io, then creates the git tag and GitHub release.

## Prerequisites

- The repository secret `CARGO_REGISTRY_TOKEN` must contain a crates.io API token with publish access to `kafkang`.
- The commit being released must be on `main` or `master`.
- The `Rust` GitHub Actions workflow must have passed for the exact commit being released.

## Release Steps

1. Update the version in `Cargo.toml`.
2. Update the dependency example in `README.md` if the public version changed.
3. Open a pull request and merge it after CI passes.
4. In GitHub, open **Actions** and select the **Release** workflow.
5. Click **Run workflow**.
6. Enter the version from `Cargo.toml`, for example `0.3.1`.
7. Keep `dry_run` enabled for the first run.
8. If the dry run succeeds, run the workflow again with `dry_run` disabled.

The non-dry-run release publishes to crates.io first. If publishing succeeds, the workflow creates an annotated `vX.Y.Z` tag and a GitHub release with generated release notes.

## Recovery

If publishing succeeds but tag or GitHub release creation fails, create the missing `vX.Y.Z` tag or release manually from the same commit. Do not rerun the workflow unchanged after a successful publish, because crates.io will reject publishing the same version again.
