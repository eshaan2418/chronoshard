// Package labels implements label sets and PromQL label matchers.
package labels

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strings"
)

// MetricName is the reserved label holding a series' metric name.
const MetricName = "__name__"

type Label struct {
	Name, Value string
}

// Labels is a set of labels sorted by name.
type Labels []Label

// FromMap builds a sorted label set.
func FromMap(m map[string]string) Labels {
	ls := make(Labels, 0, len(m))
	for k, v := range m {
		ls = append(ls, Label{k, v})
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].Name < ls[j].Name })
	return ls
}

// FromStrings builds a sorted label set from name, value pairs.
func FromStrings(kv ...string) Labels {
	if len(kv)%2 != 0 {
		panic("labels: odd number of arguments")
	}
	m := make(map[string]string, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return FromMap(m)
}

// Get returns the value of name, or "" if absent.
func (ls Labels) Get(name string) string {
	for _, l := range ls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Hash is stable for equal label sets.
func (ls Labels) Hash() uint64 {
	h := fnv.New64a()
	for _, l := range ls {
		h.Write([]byte(l.Name))
		h.Write([]byte{0xff})
		h.Write([]byte(l.Value))
		h.Write([]byte{0xff})
	}
	return h.Sum64()
}

func (ls Labels) Equal(o Labels) bool {
	if len(ls) != len(o) {
		return false
	}
	for i := range ls {
		if ls[i] != o[i] {
			return false
		}
	}
	return true
}

// Compare orders label sets the way Prometheus does: label by label, name
// then value; a set that is a prefix of another sorts first.
func Compare(a, b Labels) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := strings.Compare(a[i].Name, b[i].Name); c != 0 {
			return c
		}
		if c := strings.Compare(a[i].Value, b[i].Value); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

// Without returns a copy without the named labels.
func (ls Labels) Without(names ...string) Labels {
	out := make(Labels, 0, len(ls))
outer:
	for _, l := range ls {
		for _, n := range names {
			if l.Name == n {
				continue outer
			}
		}
		out = append(out, l)
	}
	return out
}

// Keep returns a copy with only the named labels.
func (ls Labels) Keep(names ...string) Labels {
	out := make(Labels, 0, len(names))
	for _, l := range ls {
		for _, n := range names {
			if l.Name == n {
				out = append(out, l)
				break
			}
		}
	}
	return out
}

// String renders like Prometheus: name{a="b", c="d"}.
func (ls Labels) String() string {
	var b strings.Builder
	name := ls.Get(MetricName)
	b.WriteString(name)
	b.WriteByte('{')
	first := true
	for _, l := range ls {
		if l.Name == MetricName {
			continue
		}
		if !first {
			b.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&b, "%s=%q", l.Name, l.Value)
	}
	b.WriteByte('}')
	return b.String()
}

func (ls Labels) Map() map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

type MatchType int

const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (t MatchType) String() string { return [...]string{"=", "!=", "=~", "!~"}[t] }

// Matcher matches one label's value. As in Prometheus, an absent label has
// the value "", and regular expressions are fully anchored.
type Matcher struct {
	Type  MatchType
	Name  string
	Value string
	re    *regexp.Regexp
}

func NewMatcher(t MatchType, name, value string) (*Matcher, error) {
	m := &Matcher{Type: t, Name: name, Value: value}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := regexp.Compile("^(?s:" + value + ")$")
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

func MustMatcher(t MatchType, name, value string) *Matcher {
	m, err := NewMatcher(t, name, value)
	if err != nil {
		panic(err)
	}
	return m
}

func (m *Matcher) Matches(v string) bool {
	switch m.Type {
	case MatchEqual:
		return v == m.Value
	case MatchNotEqual:
		return v != m.Value
	case MatchRegexp:
		return m.re.MatchString(v)
	default:
		return !m.re.MatchString(v)
	}
}

func (m *Matcher) String() string { return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value) }
