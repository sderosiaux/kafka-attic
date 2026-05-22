# Security Policy

## Reporting a Vulnerability

If you believe you have found a security vulnerability in kafka-attic, please report it privately. **Do not file a public GitHub issue.**

Email: **security@conduktor.io**

Include, where possible:

- A description of the vulnerability and its impact.
- Steps to reproduce, ideally with a minimal proof-of-concept.
- Affected versions and platforms.
- Any suggested remediation.

We will acknowledge receipt within five business days.

## Disclosure timeline

kafka-attic follows a **90-day responsible disclosure** default. From the date of acknowledged receipt:

- We aim to provide an initial assessment within ten business days.
- We will work with the reporter on a fix and a coordinated release.
- If a fix is not available within 90 days, we will discuss disclosure timing with the reporter in good faith. Extensions are granted when a fix is in flight and the issue is not being actively exploited.

We credit reporters in release notes unless they request anonymity.

## Supported versions

| Version | Supported          |
|---------|--------------------|
| 1.x     | Yes                |
| < 1.0   | No                 |

Security fixes are released on the latest 1.x line. Older minor versions receive fixes only for critical issues at maintainer discretion.

## In scope

- Vulnerabilities in the kafka-attic scanner itself (parsing, authentication handling, credential storage, output rendering, SQLite history).
- False-positive cleanup recommendations that could plausibly cause data loss if a user followed the generated cleanup script blindly.
- Telemetry leaks — any case where the opt-in telemetry path emits data that was not documented, or where data is sent when telemetry is disabled.
- Credential exposure through logs, error messages, or report artifacts.

## Out of scope

- Vulnerabilities in user-supplied Kafka clusters, brokers, ZooKeeper, KRaft controllers, or schema registries. kafka-attic is a client; please report those to your broker vendor.
- Social-engineering attacks against contributors or maintainers.
- Automated scanner reports (Dependabot-style noise, SCA tool output, header-grading sites) submitted without an accompanying proof-of-concept demonstrating real impact on kafka-attic.
- Issues that require attacker-controlled local execution on the user's machine with privileges that already grant access to Kafka credentials.

## Read-only by design

kafka-attic is read-only against your Kafka clusters. The binary does not link a producer client and never issues destructive Admin API calls (no `DeleteTopics`, no `DeleteRecords`, no `AlterConfigs` that mutate broker state). Cleanup recommendations are emitted as a separate shell script that the operator must read and execute themselves — kafka-attic never executes them. Any code change that breaks this invariant is itself treated as a security issue.
