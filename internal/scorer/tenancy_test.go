package scorer

import (
	"testing"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

func grp(state string, members int, committed, latest int64) types.ConsumerGroupInfo {
	return types.ConsumerGroupInfo{
		GroupID:            "g",
		State:              state,
		MemberCount:        members,
		CommittedOffsetSum: committed,
		LagSum:             latest - committed,
	}
}

func TestTenancy_Rule1_StableWithMembers(t *testing.T) {
	groups := []types.ConsumerGroupInfo{grp("Stable", 3, 100, 100)}
	got, ev, ok := scoreTenancy(groups, 100, true, true)
	if got != 0 || ev != types.EvidenceKnown || !ok {
		t.Errorf("got %d %v %v", got, ev, ok)
	}
}

func TestTenancy_Rule2_StableNoMembers(t *testing.T) {
	// Stable with 0 members → still rule 2 (active rebalance class) → 0.
	groups := []types.ConsumerGroupInfo{grp("Stable", 0, 100, 100)}
	got, _, _ := scoreTenancy(groups, 100, true, true)
	if got != 0 {
		t.Errorf("got %d want 0", got)
	}
}

func TestTenancy_Rule2_PreparingRebalance(t *testing.T) {
	groups := []types.ConsumerGroupInfo{grp("PreparingRebalance", 0, 0, 100)}
	got, _, _ := scoreTenancy(groups, 100, true, true)
	if got != 0 {
		t.Errorf("got %d want 0", got)
	}
}

func TestTenancy_Rule2_CompletingRebalance(t *testing.T) {
	groups := []types.ConsumerGroupInfo{grp("CompletingRebalance", 1, 50, 100)}
	got, _, _ := scoreTenancy(groups, 100, true, true)
	if got != 0 {
		t.Errorf("got %d want 0", got)
	}
}

func TestTenancy_Rule3_EmptyWithLag(t *testing.T) {
	groups := []types.ConsumerGroupInfo{grp("Empty", 0, 50, 100)}
	got, ev, _ := scoreTenancy(groups, 100, true, true)
	if got != 50 || ev != types.EvidenceKnown {
		t.Errorf("got %d %v want 50 KNOWN", got, ev)
	}
}

func TestTenancy_Rule4_EmptyCaughtUp(t *testing.T) {
	groups := []types.ConsumerGroupInfo{grp("Empty", 0, 100, 100)}
	got, _, _ := scoreTenancy(groups, 100, true, true)
	if got != 80 {
		t.Errorf("got %d want 80", got)
	}
}

func TestTenancy_Rule4_EmptyCaughtUp_MixWithDead(t *testing.T) {
	groups := []types.ConsumerGroupInfo{
		grp("Empty", 0, 100, 100),
		grp("Dead", 0, 0, 100),
	}
	got, _, _ := scoreTenancy(groups, 100, true, true)
	if got != 80 {
		t.Errorf("got %d want 80 (mix Empty+Dead, Empty caught up)", got)
	}
}

func TestTenancy_Rule5_AllDead(t *testing.T) {
	groups := []types.ConsumerGroupInfo{
		grp("Dead", 0, 100, 100),
		grp("Dead", 0, 50, 100),
	}
	got, _, _ := scoreTenancy(groups, 100, true, true)
	if got != 100 {
		t.Errorf("got %d want 100", got)
	}
}

func TestTenancy_Rule6_NoGroups(t *testing.T) {
	got, _, ok := scoreTenancy(nil, 100, true, true)
	if got != 100 || !ok {
		t.Errorf("got %d ok=%v want 100 true", got, ok)
	}
}

func TestTenancy_AuthFail_UnknownMissingSignal(t *testing.T) {
	got, ev, ok := scoreTenancy(nil, 100, false, true)
	if got != neutralScore || ev != types.EvidenceUnknown || ok {
		t.Errorf("describe auth fail: got %d %v %v", got, ev, ok)
	}
	got, ev, ok = scoreTenancy(nil, 100, true, false)
	if got != neutralScore || ev != types.EvidenceUnknown || ok {
		t.Errorf("fetch auth fail: got %d %v %v", got, ev, ok)
	}
}
