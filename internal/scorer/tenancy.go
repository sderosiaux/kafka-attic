package scorer

import (
	"github.com/conduktor/kafka-attic/internal/types"
)

// Consumer-group states reported by Kafka (KIP-211 + KIP-839). We match these
// case-sensitively because kadm returns them verbatim from the broker.
const (
	groupStable               = "Stable"
	groupPreparingRebalance   = "PreparingRebalance"
	groupCompletingRebalance  = "CompletingRebalance"
	groupEmpty                = "Empty"
	groupDead                 = "Dead"
)

// scoreTenancy implements the cascade in SPEC §4.2. The first matching rule
// wins, top-down. Inputs are the per-topic consumer-group view from the
// collector plus the per-topic latest_offset_sum (used in rules 3 and 4).
//
// describeAuthOk and fetchAuthOk are the two permissions that must both be
// available for evidence KNOWN. When either failed → UNKNOWN + neutral 50
// (caller flags MISSING_SIGNAL).
func scoreTenancy(
	groups []types.ConsumerGroupInfo,
	latestOffsetSum int64,
	describeAuthOk bool,
	fetchAuthOk bool,
) (score int, evidence types.Evidence, ok bool) {
	if !describeAuthOk || !fetchAuthOk {
		return neutralScore, types.EvidenceUnknown, false
	}

	// Rule 6 short-circuit: no groups → 100.
	if len(groups) == 0 {
		return 100, types.EvidenceKnown, true
	}

	// Rule 1: Stable with members.
	for _, g := range groups {
		if g.State == groupStable && g.MemberCount > 0 {
			return 0, types.EvidenceKnown, true
		}
	}

	// Rule 2: any group Stable / PreparingRebalance / CompletingRebalance
	// (regardless of member count).
	for _, g := range groups {
		switch g.State {
		case groupStable, groupPreparingRebalance, groupCompletingRebalance:
			return 0, types.EvidenceKnown, true
		}
	}

	// Rule 3: any Empty with committed < latest.
	for _, g := range groups {
		if g.State == groupEmpty && g.CommittedOffsetSum < latestOffsetSum {
			return 50, types.EvidenceKnown, true
		}
	}

	// Rule 4: any Empty with committed == latest AND all other groups must be
	// Dead or Empty. SPEC §4.2 row 4: "others must be Dead or Empty". If we
	// reached this point, rules 1 and 2 already ruled out Stable / rebalancing
	// groups — so the "others Dead or Empty" condition is implicitly satisfied.
	for _, g := range groups {
		if g.State == groupEmpty && g.CommittedOffsetSum == latestOffsetSum {
			return 80, types.EvidenceKnown, true
		}
	}

	// Rule 5: all groups Dead.
	allDead := true
	for _, g := range groups {
		if g.State != groupDead {
			allDead = false
			break
		}
	}
	if allDead {
		return 100, types.EvidenceKnown, true
	}

	// Fallthrough: ambiguous mix we did not explicitly enumerate. Behave as
	// rule 5 / rule 6 conservatively — but the SPEC's six rows are exhaustive
	// over the documented states, so this branch should be unreachable in
	// well-formed input.
	return 100, types.EvidenceKnown, true
}
