#!/usr/bin/env python3
"""Assert that a kafka-attic JSON snapshot from the e2e harness matches
the expected verdicts / flags for the seeded topic mix.

Reads JSON on stdin, takes the backend name as argv[1].
Exits 0 on success, 1 on first failed assertion (with a diff to stderr).
"""
import json
import sys


def find_topic(snap, name):
    for t in snap.get("topics", []):
        if t["name"] == name:
            return t
    return None


def check(label, ok, detail=""):
    mark = "✓" if ok else "✗"
    line = f"    {mark} {label}"
    if detail:
        line += f"  ({detail})"
    print(line)
    return ok


def main():
    backend = sys.argv[1] if len(sys.argv) > 1 else "?"
    snap = json.load(sys.stdin)
    failed = False

    topics = {t["name"]: t for t in snap.get("topics", [])}
    print(f"  scanned topics: {len(snap.get('topics', []))} ({backend})")
    expected = ["active-orders", "stale-events", "empty-topic", "oversized-events", "compacted-state"]
    for name in expected:
        if name not in topics:
            failed |= not check(f"topic {name} present", False, "missing from scan")

    if failed:
        sys.exit(1)

    # active-orders: records present + active consumer group → Active or Inspect, never LIKELY_UNUSED
    t = topics["active-orders"]
    verdict = t.get("attic", {}).get("verdict")
    failed |= not check("active-orders is not LIKELY_UNUSED", verdict != "LIKELY_UNUSED", f"verdict={verdict}")

    # empty-topic: earliest == latest == 0, no groups → APPEARS_NEVER_USED flag
    t = topics["empty-topic"]
    flags = t.get("flags", [])
    failed |= not check("empty-topic has APPEARS_NEVER_USED flag", "APPEARS_NEVER_USED" in flags, f"flags={flags}")

    # compacted-state: cleanup.policy=compact → COMPACTED flag, verdict capped at INSPECT
    t = topics["compacted-state"]
    flags = t.get("flags", [])
    verdict = t.get("attic", {}).get("verdict")
    failed |= not check("compacted-state has COMPACTED flag", "COMPACTED" in flags, f"flags={flags}")
    # verdict cap: should NOT be LIKELY_UNUSED nor CANDIDATE (COMPACTED caps to INSPECT)
    failed |= not check("compacted-state verdict <= INSPECT", verdict in ("INSPECT", "ACTIVE", None), f"verdict={verdict}")

    # stale-events: records present, no consumer → Consumption=0 caps verdict; must not be LIKELY_UNUSED
    t = topics["stale-events"]
    verdict = t.get("attic", {}).get("verdict")
    failed |= not check("stale-events is not LIKELY_UNUSED", verdict != "LIKELY_UNUSED", f"verdict={verdict}")

    # Sanity: every topic has a score in [0,100]
    for name, t in topics.items():
        if name in expected:
            raw = t.get("attic", {}).get("raw_score")
            if raw is None:
                ok = True  # signal-missing path may legitimately omit
            else:
                ok = 0 <= raw <= 100
            failed |= not check(f"{name} raw_score in [0,100]", ok, f"raw_score={raw}")

    # Sanity: the snapshot version is the expected attic spec version
    spec_v = snap.get("attic_spec_version") or snap.get("scan", {}).get("config_snapshot", {}).get("attic_spec_version")
    failed |= not check("attic_spec_version present", spec_v is not None, f"value={spec_v}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
