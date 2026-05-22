package history

import (
	"sort"

	"github.com/conduktor/kafka-attic/internal/types"
)

// TopicDelta describes the change between two recorded states for a single
// topic. Either Before or After can be nil to signal "did not exist".
type TopicDelta struct {
	Name          string        `json:"name"`
	BeforeVerdict types.Verdict `json:"before_verdict,omitempty"`
	AfterVerdict  types.Verdict `json:"after_verdict,omitempty"`
	BeforeScore   *float64      `json:"before_score,omitempty"`
	AfterScore    *float64      `json:"after_score,omitempty"`
	BeforeBytes   *int64        `json:"before_bytes,omitempty"`
	AfterBytes    *int64        `json:"after_bytes,omitempty"`
}

// DiffReport summarises the per-topic transitions between two snapshots
// (a is the older scan, b is the newer scan). The four categories are the
// ones called out in the spec's diff use-case.
type DiffReport struct {
	// A and B identify the two compared snapshots.
	A SnapshotRef `json:"a"`
	B SnapshotRef `json:"b"`

	// NewlyLikelyUnused: topics that were not LIKELY_UNUSED in A and are
	// LIKELY_UNUSED in B. Newly created topics already at LIKELY_UNUSED also
	// land here.
	NewlyLikelyUnused []TopicDelta `json:"newly_likely_unused"`

	// Regressions: topics whose verdict moved towards "more used"
	// (LIKELY_UNUSED → anything less severe, or CANDIDATE → INSPECT/ACTIVE,
	// or INSPECT → ACTIVE).
	Regressions []TopicDelta `json:"regressions"`

	// Deletions: topics present in A but absent in B.
	Deletions []TopicDelta `json:"deletions"`

	// ReclaimedBytes is the sum, across all Deletions for which both A had a
	// known storage size, of the bytes that disappeared between A and B.
	// Topics with unknown storage in A contribute zero (and are listed in
	// ReclaimedUnknownTopics).
	ReclaimedBytes int64 `json:"reclaimed_bytes"`

	// ReclaimedUnknownTopics lists topic names that were deleted between A
	// and B but whose A-side storage was unknown. They are excluded from
	// ReclaimedBytes so the number stays trustworthy.
	ReclaimedUnknownTopics []string `json:"reclaimed_unknown_topics,omitempty"`
}

// SnapshotRef identifies one side of a diff in the report output.
type SnapshotRef struct {
	GeneratedAt string `json:"generated_at"`
	TopicCount  int    `json:"topic_count"`
	Cluster     string `json:"cluster,omitempty"`
}

// verdictRank orders verdicts from "least used / most active" (0) to
// "most likely unused" (3). A negative delta = regression (less unused),
// a positive delta = progression towards likely-unused.
func verdictRank(v types.Verdict) int {
	switch v {
	case types.VerdictActive:
		return 0
	case types.VerdictInspect:
		return 1
	case types.VerdictCandidate:
		return 2
	case types.VerdictLikelyUnused:
		return 3
	default:
		// Unknown verdicts sort as ACTIVE so they don't spuriously appear
		// in NewlyLikelyUnused or Regressions.
		return 0
	}
}

// Diff compares two snapshots and produces a DiffReport. a is treated as
// the earlier scan, b as the later scan. Neither input is mutated.
func Diff(a, b *types.Snapshot) *DiffReport {
	report := &DiffReport{
		A: snapshotRef(a),
		B: snapshotRef(b),
	}
	if a == nil && b == nil {
		return report
	}

	aTopics := indexTopics(a)
	bTopics := indexTopics(b)

	// 1. New / progressed: iterate b, classify based on presence in a.
	for name, bt := range bTopics {
		at, existed := aTopics[name]

		if !existed {
			// Brand new topic.
			if bt.Attic.Verdict == types.VerdictLikelyUnused {
				report.NewlyLikelyUnused = append(report.NewlyLikelyUnused, TopicDelta{
					Name:         name,
					AfterVerdict: bt.Attic.Verdict,
					AfterScore:   ptrFloat(bt.Attic.RawScore),
					AfterBytes:   copyBytes(bt.Storage.Bytes),
				})
			}
			continue
		}

		bRank := verdictRank(bt.Attic.Verdict)
		aRank := verdictRank(at.Attic.Verdict)

		switch {
		case at.Attic.Verdict != types.VerdictLikelyUnused && bt.Attic.Verdict == types.VerdictLikelyUnused:
			report.NewlyLikelyUnused = append(report.NewlyLikelyUnused, TopicDelta{
				Name:          name,
				BeforeVerdict: at.Attic.Verdict,
				AfterVerdict:  bt.Attic.Verdict,
				BeforeScore:   ptrFloat(at.Attic.RawScore),
				AfterScore:    ptrFloat(bt.Attic.RawScore),
				BeforeBytes:   copyBytes(at.Storage.Bytes),
				AfterBytes:    copyBytes(bt.Storage.Bytes),
			})
		case bRank < aRank:
			report.Regressions = append(report.Regressions, TopicDelta{
				Name:          name,
				BeforeVerdict: at.Attic.Verdict,
				AfterVerdict:  bt.Attic.Verdict,
				BeforeScore:   ptrFloat(at.Attic.RawScore),
				AfterScore:    ptrFloat(bt.Attic.RawScore),
				BeforeBytes:   copyBytes(at.Storage.Bytes),
				AfterBytes:    copyBytes(bt.Storage.Bytes),
			})
		}
	}

	// 2. Deletions + reclaimed bytes.
	for name, at := range aTopics {
		if _, stillThere := bTopics[name]; stillThere {
			continue
		}
		d := TopicDelta{
			Name:          name,
			BeforeVerdict: at.Attic.Verdict,
			BeforeScore:   ptrFloat(at.Attic.RawScore),
			BeforeBytes:   copyBytes(at.Storage.Bytes),
		}
		report.Deletions = append(report.Deletions, d)
		if at.Storage.Bytes != nil {
			report.ReclaimedBytes += *at.Storage.Bytes
		} else {
			report.ReclaimedUnknownTopics = append(report.ReclaimedUnknownTopics, name)
		}
	}

	sortDeltas(report.NewlyLikelyUnused)
	sortDeltas(report.Regressions)
	sortDeltas(report.Deletions)
	sort.Strings(report.ReclaimedUnknownTopics)

	return report
}

func indexTopics(s *types.Snapshot) map[string]*types.Topic {
	out := make(map[string]*types.Topic)
	if s == nil {
		return out
	}
	for i := range s.Topics {
		t := &s.Topics[i]
		out[t.Name] = t
	}
	return out
}

func snapshotRef(s *types.Snapshot) SnapshotRef {
	if s == nil {
		return SnapshotRef{}
	}
	return SnapshotRef{
		GeneratedAt: s.GeneratedAt.UTC().Format("2006-01-02T15:04:05Z"),
		TopicCount:  len(s.Topics),
		Cluster:     s.Cluster.Name,
	}
}

func sortDeltas(d []TopicDelta) {
	sort.Slice(d, func(i, j int) bool { return d[i].Name < d[j].Name })
}

func ptrFloat(v float64) *float64 { return &v }

func copyBytes(b *int64) *int64 {
	if b == nil {
		return nil
	}
	v := *b
	return &v
}
