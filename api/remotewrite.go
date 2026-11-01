package api

import (
	"math"
	"sort"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/eshaan2418/chronoshard/labels"
	"github.com/eshaan2418/chronoshard/storage"
)

// TimeSeries is one series of a remote-write request.
type TimeSeries struct {
	Labels  labels.Labels
	Samples []storage.Sample
}

// DecodeWriteRequest parses a snappy-compressed prometheus.WriteRequest
// (remote write 1.0) without generated code:
//
//	WriteRequest { repeated TimeSeries timeseries = 1; }
//	TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
//	Label        { string name = 1; string value = 2; }
//	Sample       { double value = 1; int64 timestamp = 2; }
func DecodeWriteRequest(compressed []byte) ([]TimeSeries, error) {
	b, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}
	var out []TimeSeries
	err = fields(b, func(num protowire.Number, typ protowire.Type, v []byte) error {
		if num != 1 || typ != protowire.BytesType {
			return nil
		}
		var ts TimeSeries
		m := map[string]string{}
		err := fields(v, func(n protowire.Number, t protowire.Type, v []byte) error {
			switch {
			case n == 1 && t == protowire.BytesType:
				var name, value string
				if err := fields(v, func(n protowire.Number, t protowire.Type, v []byte) error {
					if t == protowire.BytesType && n == 1 {
						name = string(v)
					} else if t == protowire.BytesType && n == 2 {
						value = string(v)
					}
					return nil
				}); err != nil {
					return err
				}
				m[name] = value
			case n == 2 && t == protowire.BytesType:
				var s storage.Sample
				if err := fieldsRaw(v, func(n protowire.Number, t protowire.Type, raw uint64) {
					if n == 1 && t == protowire.Fixed64Type {
						s.V = math.Float64frombits(raw)
					} else if n == 2 && t == protowire.VarintType {
						s.T = int64(raw)
					}
				}); err != nil {
					return err
				}
				ts.Samples = append(ts.Samples, s)
			}
			return nil
		})
		if err != nil {
			return err
		}
		ts.Labels = labels.FromMap(m)
		sort.Slice(ts.Samples, func(i, j int) bool { return ts.Samples[i].T < ts.Samples[j].T })
		out = append(out, ts)
		return nil
	})
	return out, err
}

func fields(b []byte, f func(protowire.Number, protowire.Type, []byte) error) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch typ {
		case protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			if err := f(num, typ, v); err != nil {
				return err
			}
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			b = b[m:]
		}
	}
	return nil
}

func fieldsRaw(b []byte, f func(protowire.Number, protowire.Type, uint64)) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			f(num, typ, v)
			b = b[m:]
		case protowire.Fixed64Type:
			v, m := protowire.ConsumeFixed64(b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			f(num, typ, v)
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			b = b[m:]
		}
	}
	return nil
}

// EncodeWriteRequest builds a snappy-compressed WriteRequest (used by tests,
// the load generator and the Prometheus comparison script).
func EncodeWriteRequest(series []TimeSeries) []byte {
	var b []byte
	for _, ts := range series {
		var tsb []byte
		for _, l := range ts.Labels {
			var lb []byte
			lb = protowire.AppendTag(lb, 1, protowire.BytesType)
			lb = protowire.AppendString(lb, l.Name)
			lb = protowire.AppendTag(lb, 2, protowire.BytesType)
			lb = protowire.AppendString(lb, l.Value)
			tsb = protowire.AppendTag(tsb, 1, protowire.BytesType)
			tsb = protowire.AppendBytes(tsb, lb)
		}
		for _, s := range ts.Samples {
			var sb []byte
			sb = protowire.AppendTag(sb, 1, protowire.Fixed64Type)
			sb = protowire.AppendFixed64(sb, math.Float64bits(s.V))
			sb = protowire.AppendTag(sb, 2, protowire.VarintType)
			sb = protowire.AppendVarint(sb, uint64(s.T))
			tsb = protowire.AppendTag(tsb, 2, protowire.BytesType)
			tsb = protowire.AppendBytes(tsb, sb)
		}
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, tsb)
	}
	return snappy.Encode(nil, b)
}
