# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [1.0.0] - 2026-05-22

### Added

- 4-subcommand CLI: `scan`, `audit`, `inspect`, `diff`.
- ATTIC Score v1.0.0 with five sub-signals: **A**ctivity, **T**onnage, **T**raffic, **I**ntent, **C**onfiguration.
- 5 authentication types: `sasl_plain`, `scram` (SHA-256 and SHA-512), `mtls`, `iam` (AWS — supports `AWS_PROFILE`, `assume_role`, and `web_identity`), and `oauth`.
- Confluent Schema Registry integration for schema-aware audit signals.
- 4 owner-resolution sources: static file, topic config key, Backstage catalog, and arbitrary JSON endpoint.
- Output renderers: terminal (default), JSON, CSV, HTML.
- HTML report with embedded cleanup script section.
- Local SQLite history database and `kattic diff` to compare consecutive scans.
- Managed Kafka detection for AWS MSK, Confluent Cloud, Aiven, and Redpanda.
- Opt-in anonymous telemetry.
- `--share` flag producing a shareable summary URL.

### Security

- Read-only by design: the binary does not link a producer client and never issues destructive Admin API calls.
- No record contents are fetched at any point; last-activity is derived from the offset-by-timestamp API only.
- Opt-in SHA-256 redaction of topic names in shared and exported output.
- Cleanup script inclusion rules explicitly exclude topics marked `COMPACTED`, `REMOTE_STORAGE`, or `MISSING_SIGNAL`.

[Unreleased]: https://github.com/conduktor/kafka-attic/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/conduktor/kafka-attic/releases/tag/v1.0.0
