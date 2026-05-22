# kafka-attic vs Confluent Health+ — Kafka topic cleanup vs cluster-health monitoring

kafka-attic scores stale Kafka topics across any cluster (self-managed, MSK, Confluent Cloud, Aiven, Redpanda); Confluent Health+ monitors cluster health, Confluent-only, as a paid SKU. Different coverage, different lock-in.

[Confluent Health+](https://docs.confluent.io/cloud/current/monitoring/health-plus.html) is Confluent's paid monitoring add-on for Confluent Platform and Confluent Cloud. It collects broker telemetry, surfaces cluster-level alerts, and feeds Confluent Support with diagnostics. kafka-attic occupies a different point on the lock-in, coverage, cost, and deployment axes.

## Lock-in axis

Health+ is bound to Confluent's runtime. It does not run against Amazon MSK, Aiven, Redpanda, IBM Event Streams, or self-managed Apache Kafka. The data model assumes Confluent's broker plus the Confluent Telemetry Reporter, and the dashboards live inside Confluent's UI.

kafka-attic is vendor-neutral by design. It speaks the open Kafka admin protocol via [franz-go](https://github.com/twmb/franz-go) and runs against every cluster type listed in the [managed-Kafka matrix](/README.md#managed-kafka-support-msk-confluent-cloud-aiven-redpanda): self-managed, MSK Provisioned, MSK Serverless, Confluent Cloud, Aiven, and Redpanda. There is no vendor binding at the protocol level and no proprietary telemetry shipper.

## Coverage axis

Health+ focuses on **cluster health**: broker availability, under-replicated partitions, request latency, throughput, disk usage at the broker level. Its question is *"is the cluster healthy right now?"*.

kafka-attic focuses on **topic hygiene**: which topics are stale, empty, oversized, or orphan to a schema. Its question is *"which of my topics should not exist anymore?"*. There is essentially no overlap between the two coverage areas: a perfectly healthy cluster can be full of dead topics, and a topic-clean cluster can still have broker issues.

| Surface area                  | Confluent Health+   | kafka-attic                                           |
|-------------------------------|---------------------|-------------------------------------------------------|
| Broker availability           | Yes                 | No                                                    |
| Under-replicated partitions   | Yes                 | No                                                    |
| Topic-level disuse scoring    | No                  | Yes — ATTIC Score with verdict bands                  |
| Cleanup script generation     | No                  | Yes — with inclusion rules and preflights             |
| Schema-orphan detection       | No                  | Yes — Confluent SR in v1; Glue / Apicurio in v1.1     |
| Storage reclaim accounting    | No                  | Yes — headline `N TB reclaimable` summary             |

## Cost axis

Health+ is a paid SKU layered on top of an already paid Confluent product. Pricing scales with cluster size and the broker count it monitors. The cost model is recurring per month per cluster.

kafka-attic is Apache 2.0, free, and has no recurring cost. The binary is a one-time download. The runtime cost is whatever it costs to schedule a CI job once a day or once a week on whatever runner you already have.

## Deployment axis

Health+ requires the Confluent Telemetry Reporter to be configured on the broker side, then a connection to Confluent's hosted backend, then a Confluent account with the right entitlement. It is a long-running monitoring path that lives outside your cluster.

kafka-attic is a single static Go binary distributed via brew, scoop, docker, and the GitHub release page. It connects to the broker, runs a scan, writes an artifact, and exits. No agent, no telemetry reporter, no broker-side configuration, no hosted backend, no account.

## Recommendation

If the cluster is Confluent-only and the question is *"is the cluster healthy?"*, Health+ is the right answer. If the question is *"are my topics still useful?"* — on any cluster type, including Confluent Cloud — kafka-attic is the right answer. The two can run side by side without contention; they share neither data plane nor control plane.

## Related

- [kafka-attic vs AKHQ](/docs/vs/akhq.md) — topic browser vs topic cleanup
- [kafka-attic vs Cruise Control](/docs/vs/cruise-control.md) — broker rebalancing vs topic cleanup
- [README](/README.md) — kafka-attic overview
- [Landing page](https://sderosiaux.github.io/kafka-attic/) — canonical home

---

Last updated: 2026-05-22
