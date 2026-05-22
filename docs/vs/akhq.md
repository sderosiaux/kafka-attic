# kafka-attic vs AKHQ

[AKHQ](https://akhq.io) is a browser-based UI for exploring Apache Kafka topics, consumer groups, schemas, ACLs, and connectors. It is the most common answer when someone asks *"how do I see what is in my Kafka cluster?"*. kafka-attic answers a different question: *"which of these topics have stopped being useful?"*.

## At a glance

| Dimension                          | AKHQ                                              | kafka-attic                                                  |
|------------------------------------|---------------------------------------------------|--------------------------------------------------------------|
| Shape                              | Long-running web app (JVM) with a UI              | Single static Go binary, one-shot CLI                        |
| Primary use                        | Browse topics, inspect records, manage groups     | Score topics, surface cleanup candidates, prove disuse       |
| Reads record contents              | Yes — that is the point of the topic browser      | No — last-activity comes from `ListOffsets`, never `Fetch`   |
| Mutates the cluster                | Optional (delete topics, reset offsets, edit ACLs) via UI | Never — producer client statically excluded                  |
| Output                             | Interactive UI                                    | Terminal table, JSON, CSV, single-file HTML report           |
| Scoring / methodology              | None                                              | ATTIC Score v1.0.0 (versioned, CC BY 4.0 spec)               |
| Cleanup workflow                   | Per-topic clicks                                  | Generated cleanup script with inclusion rules and preflights |
| Audit trail                        | None built-in                                     | Reproducible JSON snapshot, `kattic diff` for week-over-week |
| Deployment footprint               | JVM server + config                               | Brew / scoop / docker / curl one binary                      |

## When to use AKHQ

AKHQ is the right tool when an engineer needs to inspect a record, peek at headers, replay a message, watch consumer-group lag scroll, or grant an ACL through a UI. It is interactive, multi-user, and ergonomic for the everyday Kafka workflow that involves a human reading data. Teams that already run AKHQ as a shared internal tool should keep doing so — kafka-attic does not replace any of that.

## When to use kafka-attic

kafka-attic is the right tool when the question is *"what is in our topic graveyard and how much storage can we reclaim?"*. It is non-interactive, batch-shaped, and produces an artifact that survives the session: a JSON snapshot, a CSV, an HTML report you can attach to a ticket. The ATTIC Score gives every topic a single comparable number; the verdict caps protect against deleting topics whose evidence is weak; the cleanup script is a shell artifact that goes through code review, not a button in a browser.

The CLI shape also makes it easy to schedule: a nightly CI job runs `kattic audit --output report.html`, uploads the JSON snapshot to S3, and (with `kattic diff`) reports reclaim progress week over week. AKHQ is not built for that workflow.

## Can they coexist?

Yes — they answer different questions on the same cluster. A common pattern is: kafka-attic finds candidates in batch, an engineer opens AKHQ to sanity-check a specific topic's recent records before approving the delete, and the actual deletion runs from the kafka-attic cleanup script under a peer review. The two tools share read-only ACL requirements and run against the same brokers without coordination.
