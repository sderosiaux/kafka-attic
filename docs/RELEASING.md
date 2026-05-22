# Releasing kafka-attic

This document is the operational guide for cutting a release. The pipeline is
driven by [GoReleaser](https://goreleaser.com) via
`.github/workflows/release.yml`, triggered by pushing a `v*` tag.

## Prerequisites

These need to exist **before** the first release tag is pushed.

### Repositories

The Homebrew formula and Scoop manifest are committed back into this same
repository under `Formula/` and `Scoop/` on every release. No external tap or
bucket repositories are required — the release pipeline writes:

- `Formula/kafka-attic.rb` — Homebrew formula
- `Scoop/kafka-attic.json` — Scoop manifest

via GoReleaser's `brews.repository.directory` and `scoops.repository.directory`
fields targeting the `main` branch of this repo.

### Secrets

The release workflow needs the following secrets configured at
`Settings → Secrets and variables → Actions` on `sderosiaux/kafka-attic`:

| Secret | Source | Scope | Required for |
|---|---|---|---|
| `GITHUB_TOKEN` | Auto-provided by GitHub Actions | n/a | Creating the release, pushing to GHCR |
| `HOMEBREW_TAP_GITHUB_TOKEN` | Personal Access Token (classic) | `repo` | Pushing formula/manifest back to this repo's `main` branch (GitHub Actions default `GITHUB_TOKEN` cannot push directly to the branch under most branch-protection setups) |

`HOMEBREW_TAP_GITHUB_TOKEN` cannot be set from the `gh` CLI for security
reasons — add it via the GitHub web UI. Use a long-lived classic PAT (fine
grained PATs do not currently support cross-repo writes the way GoReleaser
needs them). Scope it to **only** this repo.

### Permissions

The workflow already grants:

- `contents: write` — create GitHub releases
- `packages: write` — push container images to `ghcr.io`
- `id-token: write` — reserved for future OIDC signing (cosign / sigstore)

## Cutting a release

1. **Update `CHANGELOG.md`.** Move entries from `Unreleased` into a new
   `vX.Y.Z` section with the date. Conventional Commit prefixes (`feat:`,
   `fix:`, `docs:`, `chore:`) are honored by GoReleaser's GitHub changelog
   generator, but a hand-curated changelog is still the source of truth.

2. **Commit and merge.** Land the changelog update on `main`.

3. **Tag.** From a clean `main` checkout:

   ```sh
   git pull --ff-only
   git tag -s vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

   The `-s` flag signs the tag with your GPG key. Configure
   `user.signingkey` in git if you haven't already.

4. **Monitor.** Watch
   https://github.com/sderosiaux/kafka-attic/actions for the Release run.
   On success, the GitHub release will be published with archives, checksums,
   container images (`ghcr.io/sderosiaux/kafka-attic:vX.Y.Z` and `:latest`),
   and the Homebrew + Scoop formulae will be updated automatically.

5. **Smoke test.** From a clean machine or container:

   ```sh
   brew tap sderosiaux/kafka-attic https://github.com/sderosiaux/kafka-attic
   brew install kafka-attic
   kattic --version
   ```

## Versioning

The project follows [Semantic Versioning 2.0](https://semver.org).

- `v0.x.y` — pre-1.0. Public API (CLI flags, config schema, output shape) may
  change between minor versions. Breaking changes are called out in the
  changelog under a `Breaking` heading.
- `v1.0.0` and beyond — backward compatibility is guaranteed within a major
  version. Deprecations are announced at least one minor version before
  removal, mirroring the Apache Kafka changelog conventions.

## Recovering from a failed release

If the workflow fails partway through:

- **Before binaries are uploaded:** delete the tag locally and on the remote
  (`git tag -d vX.Y.Z && git push --delete origin vX.Y.Z`), fix the issue, and
  re-tag.
- **After the GitHub release was created:** delete the draft/published
  release from the UI, delete the tag as above, fix, re-tag.
- **After the Homebrew tap was updated but Scoop failed (or vice versa):**
  it's safest to revert the tap commit manually, fix the underlying issue,
  re-run the release workflow with `workflow_dispatch` if it has been added,
  or cut a patch release.

## Snapshot releases (local validation)

To validate the release pipeline locally without publishing anything:

```sh
goreleaser release --snapshot --clean --skip=publish --skip=docker
```

Artifacts land in `dist/`. Use this to verify that binaries cross-compile and
checksums are generated before pushing a tag.
