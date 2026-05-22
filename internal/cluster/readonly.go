package cluster

import "errors"

// ErrProduceForbidden is the sentinel error every code path that might be
// tempted to call into a producer code path must return. kafka-attic is a
// read-only tool: the binary statically refuses to compile a producer client
// (enforced by readonly_test.go's source scan of the non-test corpus).
var ErrProduceForbidden = errors.New("kafka-attic is read-only: producing to Kafka is forbidden by design")
