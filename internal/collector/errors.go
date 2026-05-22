package collector

import "errors"

// Sentinel errors returned only when the collector cannot proceed at all.
// Auth-style failures on individual APIs are absorbed into the snapshot and
// surface as evidence degradation — they do NOT cause Collect to return an
// error.
var (
	errEmptyClient = errors.New("collector: cluster client is nil; call cluster.Connect first")
	errNilConfig   = errors.New("collector: nil config")
)
