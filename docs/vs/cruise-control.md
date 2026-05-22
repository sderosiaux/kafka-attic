# kafka-attic vs Cruise Control

[Cruise Control](https://github.com/linkedin/cruise-control) is LinkedIn's Kafka cluster-balancing service. It models broker resource usage (CPU, network, disk) and computes partition reassignments to satisfy goal-based optimization. kafka-attic does not balance partitions, does not move replicas, and does not look at brokers as a unit of analysis. The two tools operate at different layers and answer different questions.

## Different layers

| Layer                           | Cruise Control                                                  | kafka-attic                                              |
|---------------------------------|-----------------------------------------------------------------|----------------------------------------------------------|
| Unit of analysis                | Brokers, partitions, replicas                                   | Topics                                                   |
| Question answered               | *Is my cluster balanced? Should I move partitions?*             | *Which topics have stopped being useful?*                |
| Mutates the cluster             | Yes — partition reassignment is the product                     | Never — read-only by construction                        |
| Time horizon                    | Continuous (long-running service)                               | Single scan, on demand                                   |
| Goals                           | Resource utilisation, replica placement, rack awareness         | Disuse evidence, reclaim accounting                      |
| Input                           | Broker JMX metrics, partition-level load                        | Admin APIs, offsets, group state, optional SR + metrics  |
| Deployment                      | JVM service with its own metric sampler                         | Static Go binary                                         |

Cruise Control answers *"my broker disks are uneven, what partitions should move?"*. kafka-attic answers *"my topic count grew 4x in two years, which of these are dead?"*. The two questions are independent: a perfectly balanced cluster can still have thousands of abandoned topics, and a cluster full of active topics can still be unbalanced.

## Complementary, not competitive

Cruise Control reads broker metrics and produces a partition movement plan. kafka-attic reads admin APIs and produces a topic cleanup plan. The artefacts are unrelated and the runtimes do not conflict. A common operating pattern:

1. **kafka-attic** runs first to delete or archive the topics that should not exist at all. Fewer topics means a smaller partition count, less metadata churn, and a smaller search space for everything downstream.
2. **Cruise Control** runs second to rebalance whatever remains. The fewer dead topics in the cluster, the more useful Cruise Control's goal-based optimisation is — every partition it considers actually carries traffic.

There is no overlap in the change surface: kafka-attic never touches partition placement, Cruise Control never touches topic lifecycle. They share read-only access requirements (`Describe` on cluster, topics, groups) and can run from the same operator credentials without conflict.

## What kafka-attic does not do

kafka-attic does not compute broker balance, does not score replication factor, does not detect rack-affinity violations, and does not propose partition reassignments. The `SKEWED` flag in the report — *"partition load uneven"* — is a topic-level observation (largest partition > N × average partition size) that tells the operator *"this topic is a candidate for repartitioning"*. It is not a substitute for Cruise Control's broker-aware planner; it is a hint that surfaces from the same scan and points at a different tool.
