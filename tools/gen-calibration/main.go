// Command gen-calibration produces anonymized synthetic snapshot fixtures
// for the ATTIC Score calibration dataset.
//
// The output is intentionally distribution-only (no raw topic-level rows)
// so the file stays small and reviewable. It is deterministic given a
// seed; rerun the same seed to reproduce the same JSON.
//
// Usage:
//
//	go run ./tools/gen-calibration > testdata/calibration/synthetic-clusters.json
//	go run ./tools/gen-calibration --seed 7 > out.json
//
// The data is SYNTHETIC and does not represent any real cluster.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"

	"github.com/conduktor/kafka-attic/pkg/atticspec"
)

// clusterProfile drives one synthetic cluster summary.
type clusterProfile struct {
	ID                 string
	SizeBucket         string // small | medium | large
	TopicCount         int
	ClusterType        string // self_managed | msk_provisioned | msk_serverless | confluent_cloud | redpanda | aiven
	SchemaRegistry     bool
	TonnageDefault     atticspec.Evidence
	// Mix knobs (sum < 1; rest goes to active).
	FracLikelyUnused float64
	FracCandidate    float64
	FracInspect      float64
	// Storage knobs.
	TotalBytes int64
	// Flag prevalence rates.
	FlagRates map[atticspec.Flag]float64
}

// clusterSummary is the per-cluster output row.
type clusterSummary struct {
	ID               string                     `json:"id"`
	SizeBucket       string                     `json:"size_bucket"`
	ClusterType      string                     `json:"cluster_type"`
	SchemaRegistry   bool                       `json:"schema_registry"`
	TonnageDefault   atticspec.Evidence         `json:"tonnage_default_evidence"`
	TopicCount       int                        `json:"topic_count"`
	TotalBytes       int64                      `json:"total_bytes"`
	ReclaimableBytes int64                      `json:"reclaimable_bytes"`
	Verdicts         map[atticspec.Verdict]int  `json:"verdicts"`
	ScoreHistogram   map[string]int             `json:"score_histogram"`
	FlagCounts       map[atticspec.Flag]int     `json:"flag_counts"`
	MeanScore        float64                    `json:"mean_score"`
	MedianScore      float64                    `json:"median_score"`
}

// document is the top-level JSON envelope.
type document struct {
	Notice       string                     `json:"notice"`
	SpecVersion  string                     `json:"attic_spec_version"`
	Seed         uint64                     `json:"seed"`
	Generator    string                     `json:"generator"`
	Clusters     []clusterSummary           `json:"clusters"`
	Aggregate    map[atticspec.Verdict]int  `json:"aggregate_verdicts"`
	AggregateFlg map[atticspec.Flag]int     `json:"aggregate_flag_counts"`
}

var defaultProfiles = []clusterProfile{
	{
		ID:               "synthetic-small-01",
		SizeBucket:       "small",
		TopicCount:       52,
		ClusterType:      "self_managed",
		SchemaRegistry:   true,
		TonnageDefault:   atticspec.EvidenceKnown,
		FracLikelyUnused: 0.10,
		FracCandidate:    0.15,
		FracInspect:      0.20,
		TotalBytes:       420 * gb,
		FlagRates: map[atticspec.Flag]float64{
			atticspec.FlagOrphanSchema:     0.18,
			atticspec.FlagCompacted:        0.10,
			atticspec.FlagPurged:           0.06,
			atticspec.FlagSkewed:           0.08,
			atticspec.FlagAppearsNeverUsed: 0.04,
		},
	},
	{
		ID:               "synthetic-medium-01",
		SizeBucket:       "medium",
		TopicCount:       487,
		ClusterType:      "msk_provisioned",
		SchemaRegistry:   true,
		TonnageDefault:   atticspec.EvidenceKnown,
		FracLikelyUnused: 0.16,
		FracCandidate:    0.22,
		FracInspect:      0.18,
		TotalBytes:       6_800 * gb,
		FlagRates: map[atticspec.Flag]float64{
			atticspec.FlagOrphanSchema:     0.22,
			atticspec.FlagCompacted:        0.12,
			atticspec.FlagPurged:           0.08,
			atticspec.FlagSkewed:           0.11,
			atticspec.FlagAppearsNeverUsed: 0.06,
			atticspec.FlagRemoteStorage:    0.03,
		},
	},
	{
		ID:               "synthetic-large-01",
		SizeBucket:       "large",
		TopicCount:       5142,
		ClusterType:      "confluent_cloud",
		SchemaRegistry:   true,
		TonnageDefault:   atticspec.EvidenceUnknown,
		FracLikelyUnused: 0.24,
		FracCandidate:    0.28,
		FracInspect:      0.16,
		TotalBytes:       82_000 * gb,
		FlagRates: map[atticspec.Flag]float64{
			atticspec.FlagOrphanSchema:     0.27,
			atticspec.FlagCompacted:        0.14,
			atticspec.FlagPurged:           0.11,
			atticspec.FlagSkewed:           0.13,
			atticspec.FlagAppearsNeverUsed: 0.09,
			atticspec.FlagRemoteStorage:    0.21,
			atticspec.FlagMissingSignal:    0.18,
		},
	},
}

const (
	kb = int64(1024)
	mb = 1024 * kb
	gb = 1024 * mb
)

func main() {
	seed := flag.Uint64("seed", 42, "PRNG seed (idempotent: same seed → same JSON)")
	flag.Parse()

	rng := rand.New(rand.NewPCG(*seed, *seed^0x9E3779B97F4A7C15))

	clusters := make([]clusterSummary, 0, len(defaultProfiles))
	aggregate := map[atticspec.Verdict]int{}
	aggFlags := map[atticspec.Flag]int{}

	for _, p := range defaultProfiles {
		cs := simulate(rng, p)
		clusters = append(clusters, cs)
		for v, n := range cs.Verdicts {
			aggregate[v] += n
		}
		for f, n := range cs.FlagCounts {
			aggFlags[f] += n
		}
	}

	doc := document{
		Notice:       "SYNTHETIC DATA. Generated for ATTIC Score calibration. Not derived from any real customer cluster. Distributions are illustrative only.",
		SpecVersion:  atticspec.SpecVersion,
		Seed:         *seed,
		Generator:    "tools/gen-calibration",
		Clusters:     clusters,
		Aggregate:    aggregate,
		AggregateFlg: aggFlags,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		fmt.Fprintln(os.Stderr, "encode:", err)
		os.Exit(1)
	}
}

func simulate(rng *rand.Rand, p clusterProfile) clusterSummary {
	verdicts := map[atticspec.Verdict]int{
		atticspec.VerdictLikelyUnused: 0,
		atticspec.VerdictCandidate:    0,
		atticspec.VerdictInspect:      0,
		atticspec.VerdictActive:       0,
	}
	hist := map[string]int{
		"0-9": 0, "10-19": 0, "20-29": 0, "30-39": 0, "40-49": 0,
		"50-59": 0, "60-69": 0, "70-79": 0, "80-89": 0, "90-100": 0,
	}
	flagCounts := map[atticspec.Flag]int{}

	scores := make([]float64, 0, p.TopicCount)
	var reclaim int64

	// Per-topic size: lognormal-ish. Most topics small, fat tail.
	for i := 0; i < p.TopicCount; i++ {
		// Pick verdict by stratification.
		r := rng.Float64()
		var v atticspec.Verdict
		var score float64
		switch {
		case r < p.FracLikelyUnused:
			v = atticspec.VerdictLikelyUnused
			score = 90 + rng.Float64()*10
		case r < p.FracLikelyUnused+p.FracCandidate:
			v = atticspec.VerdictCandidate
			score = 70 + rng.Float64()*20
		case r < p.FracLikelyUnused+p.FracCandidate+p.FracInspect:
			v = atticspec.VerdictInspect
			score = 40 + rng.Float64()*30
		default:
			v = atticspec.VerdictActive
			score = rng.Float64() * 40
		}
		verdicts[v]++
		bucket := histBucket(score)
		hist[bucket]++
		scores = append(scores, score)

		// Flag emission. Independent draws per flag.
		topicFlags := map[atticspec.Flag]bool{}
		for f, rate := range p.FlagRates {
			if rng.Float64() < rate {
				topicFlags[f] = true
				flagCounts[f]++
			}
		}

		// Storage: lognormal in MB → bytes. Larger clusters skew larger.
		base := int64(0)
		switch p.SizeBucket {
		case "small":
			base = int64(rng.ExpFloat64() * float64(50*mb))
		case "medium":
			base = int64(rng.ExpFloat64() * float64(500*mb))
		case "large":
			base = int64(rng.ExpFloat64() * float64(2*gb))
		}
		// LIKELY_UNUSED / CANDIDATE topics tend to be smaller than ACTIVE
		// (the "long tail of forgotten topics" pattern). Scale down.
		if v == atticspec.VerdictLikelyUnused || v == atticspec.VerdictCandidate {
			base = base / 3
		}
		if v == atticspec.VerdictLikelyUnused {
			// REMOTE_STORAGE / MISSING_SIGNAL would block reclaim — skip.
			if !topicFlags[atticspec.FlagRemoteStorage] && !topicFlags[atticspec.FlagMissingSignal] && !topicFlags[atticspec.FlagCompacted] {
				reclaim += base
			}
		}
	}

	sort.Float64s(scores)
	mean := 0.0
	for _, s := range scores {
		mean += s
	}
	if len(scores) > 0 {
		mean /= float64(len(scores))
	}
	median := 0.0
	if n := len(scores); n > 0 {
		if n%2 == 1 {
			median = scores[n/2]
		} else {
			median = (scores[n/2-1] + scores[n/2]) / 2
		}
	}

	return clusterSummary{
		ID:               p.ID,
		SizeBucket:       p.SizeBucket,
		ClusterType:      p.ClusterType,
		SchemaRegistry:   p.SchemaRegistry,
		TonnageDefault:   p.TonnageDefault,
		TopicCount:       p.TopicCount,
		TotalBytes:       p.TotalBytes,
		ReclaimableBytes: reclaim,
		Verdicts:         verdicts,
		ScoreHistogram:   hist,
		FlagCounts:       flagCounts,
		MeanScore:        round2(mean),
		MedianScore:      round2(median),
	}
}

func histBucket(score float64) string {
	switch {
	case score < 10:
		return "0-9"
	case score < 20:
		return "10-19"
	case score < 30:
		return "20-29"
	case score < 40:
		return "30-39"
	case score < 50:
		return "40-49"
	case score < 60:
		return "50-59"
	case score < 70:
		return "60-69"
	case score < 80:
		return "70-79"
	case score < 90:
		return "80-89"
	default:
		return "90-100"
	}
}

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
