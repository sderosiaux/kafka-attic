# Contributing to kafka-attic

Welcome, and thank you for your interest in contributing to kafka-attic. This project is sponsored by Conduktor SAS and developed in the open under the Apache License 2.0. Contributions of code, documentation, tests, and bug reports are all valued.

## Code of Conduct

By participating in this project you agree to abide by the [Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md). Report unacceptable behaviour to `community@conduktor.io`.

## License permanence

kafka-attic is licensed under the [Apache License 2.0](./LICENSE) and will remain so. The project will not be relicensed without the unanimous consent of every contributor whose work is still present in the tree. There is no CLA — contributors retain their copyright and license their work to the project under Apache 2.0 via the DCO sign-off below.

## Developer Certificate of Origin (DCO)

Every commit must carry a Developer Certificate of Origin sign-off. This is a single line at the end of the commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

The easiest way to add it is to pass `-s` (or `--signoff`) to `git commit`:

```bash
git commit -s -m "feat(scanner): add per-topic last-activity probe"
```

`git commit -s` uses the `user.name` and `user.email` from your local git config. The DCO check on every pull request will block merges if any commit is missing a sign-off. By signing off, you certify the four points of the [Developer Certificate of Origin](https://developercertificate.org/).

## Building

Requires Go 1.22+.

```bash
go build ./...
```

The CLI entrypoint lives under `./cmd/kattic`.

## Testing

Unit tests:

```bash
go test ./...
```

Integration tests (require Docker, use testcontainers to spin up ephemeral Kafka clusters):

```bash
go test ./... -tags=integration -timeout=15m
```

## Linting

```bash
golangci-lint run
```

The lint configuration lives in `.golangci.yml` and CI enforces it on every pull request.

## Commit messages

We prefer [Conventional Commits](https://www.conventionalcommits.org/). Common types:

- `feat:` — user-visible feature
- `fix:` — bug fix
- `docs:` — documentation only
- `refactor:` — non-behavioural change
- `test:` — tests only
- `chore:` — build / tooling / dependencies

Subject in imperative voice. Reference issues with `Closes #123` where relevant.

## Pull request review

- At least one maintainer approval is required before merge.
- CI (build, unit tests, lint) must be green.
- The DCO check must be green — every commit signed off.
- Squash-merge is the default; the PR title becomes the squashed commit subject, so keep it conventional-commits shaped.

## Security issues

Do not open public issues for security reports. See [SECURITY.md](./SECURITY.md).
