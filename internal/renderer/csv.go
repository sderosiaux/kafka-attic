package renderer

import (
	"encoding/csv"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

// CSVOptions controls CSV rendering.
type CSVOptions struct {
	// Redact applies SHA-256 redaction to topic names when true.
	Redact bool
}

// csvColumns is the stable column order for CSV output. Adding columns is
// only allowed at the tail (additive minor schema change, like JSON).
var csvColumns = []string{
	"name",
	"partitions",
	"replication_factor",
	"cleanup_policy",
	"retention_ms",
	"remote_storage_enabled",
	"message_timestamp_type",
	"last_produce_ts",
	"earliest_offset_sum",
	"latest_offset_sum",
	"storage_bytes",
	"storage_source",
	"storage_evidence",
	"score_activity",
	"score_tenancy",
	"score_tonnage",
	"score_intent",
	"score_consumption",
	"raw_score",
	"verdict",
	"verdict_capped_by",
	"flags",
	"owner",
	"owner_source",
	"signals_missing",
}

// RenderCSV writes a CSV row per topic to w with a stable header row.
func RenderCSV(w io.Writer, s *types.Snapshot, opts CSVOptions) error {
	cw := csv.NewWriter(w)
	err := cw.Write(csvColumns)
	if err != nil {
		return err
	}
	for _, t := range s.Topics {
		name := t.Name
		if opts.Redact {
			name = sha256Hex(name)
		}
		row := []string{
			name,
			strconv.Itoa(t.Partitions),
			strconv.Itoa(t.ReplicationFactor),
			t.CleanupPolicy,
			strconv.FormatInt(t.RetentionMs, 10),
			strconv.FormatBool(t.RemoteStorageEnabled),
			t.MessageTimestampType,
			isoOrEmpty(t.LastProduceTS),
			strconv.FormatInt(t.EarliestOffsetSum, 10),
			strconv.FormatInt(t.LatestOffsetSum, 10),
			storageBytesString(t.Storage.Bytes),
			t.Storage.Source,
			string(t.Storage.Evidence),
			subScoreString(t, types.SubSignalActivity),
			subScoreString(t, types.SubSignalTenancy),
			subScoreString(t, types.SubSignalTonnage),
			subScoreString(t, types.SubSignalIntent),
			subScoreString(t, types.SubSignalConsumption),
			rawScoreString(t),
			string(t.Attic.Verdict),
			derefString(t.Attic.VerdictCappedBy),
			flagsCSV(t.Flags),
			ownerValue(t.Owner, opts.Redact),
			ownerSource(t.Owner),
			signalsMissingCSV(t.SignalsMissing, opts.Redact),
		}
		err := cw.Write(row)
		if err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func storageBytesString(b *int64) string {
	if b == nil {
		return ""
	}
	return strconv.FormatInt(*b, 10)
}

func subScoreString(t types.Topic, sig types.SubSignal) string {
	sub, ok := t.Attic.SubScores[sig]
	if !ok {
		return ""
	}
	if sub.Skipped {
		return ""
	}
	return strconv.Itoa(sub.Score)
}

func rawScoreString(t types.Topic) string {
	if len(t.Attic.SubScores) == 0 {
		return ""
	}
	return strconv.FormatFloat(t.Attic.RawScore, 'f', -1, 64)
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func flagsCSV(flags []types.Flag) string {
	if len(flags) == 0 {
		return ""
	}
	parts := make([]string, len(flags))
	for i, f := range flags {
		parts[i] = string(f)
	}
	return strings.Join(parts, "|")
}

func ownerValue(o *types.OwnerInfo, redact bool) string {
	if o == nil {
		return ""
	}
	if redact {
		return sha256Hex(o.Value)
	}
	return o.Value
}

func ownerSource(o *types.OwnerInfo) string {
	if o == nil {
		return ""
	}
	return o.Source
}

func signalsMissingCSV(sigs []types.SubSignal, _ bool) string {
	if len(sigs) == 0 {
		return ""
	}
	parts := make([]string, len(sigs))
	for i, s := range sigs {
		parts[i] = string(s)
	}
	return strings.Join(parts, "|")
}

func isoOrEmpty(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}
