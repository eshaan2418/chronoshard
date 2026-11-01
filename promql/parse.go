// Package promql implements a subset of PromQL: selectors with all four
// matcher types, range vectors, offset, rate/irate/increase/delta and the
// *_over_time family, aggregations (sum, avg, min, max, count, topk,
// bottomk) with by/without, scalar and vector arithmetic and comparisons
// with one-to-one matching (optionally on/ignoring), and abs/ceil/floor/
// round/clamp_min/clamp_max/time/vector/scalar.
package promql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/eshaan2418/chronoshard/labels"
)

// Expr is a parsed PromQL expression.
type Expr interface{ String() string }

type NumberLiteral struct{ Val float64 }

type VectorSelector struct {
	Name     string
	Matchers []*labels.Matcher
	Offset   time.Duration
}

type MatrixSelector struct {
	VS    *VectorSelector
	Range time.Duration
}

type Call struct {
	Func string
	Args []Expr
}

type Aggregate struct {
	Op      string // sum avg min max count topk bottomk
	Param   Expr   // k for topk/bottomk
	Expr    Expr
	By      bool
	Without bool
	Labels  []string
}

type Binary struct {
	Op         string
	LHS, RHS   Expr
	ReturnBool bool
	On         bool
	Ignoring   bool
	Matching   []string
}

type Paren struct{ Expr Expr }

type Unary struct {
	Op   string
	Expr Expr
}

func (n *NumberLiteral) String() string { return strconv.FormatFloat(n.Val, 'g', -1, 64) }
func (v *VectorSelector) String() string {
	var ms []string
	for _, m := range v.Matchers {
		if m.Name != labels.MetricName {
			ms = append(ms, m.String())
		}
	}
	s := v.Name + "{" + strings.Join(ms, ",") + "}"
	if v.Offset != 0 {
		s += " offset " + v.Offset.String()
	}
	return s
}
func (m *MatrixSelector) String() string { return fmt.Sprintf("%s[%s]", m.VS, m.Range) }
func (c *Call) String() string {
	var a []string
	for _, x := range c.Args {
		a = append(a, x.String())
	}
	return c.Func + "(" + strings.Join(a, ", ") + ")"
}
func (a *Aggregate) String() string {
	s := a.Op
	if a.By {
		s += " by (" + strings.Join(a.Labels, ",") + ")"
	} else if a.Without {
		s += " without (" + strings.Join(a.Labels, ",") + ")"
	}
	if a.Param != nil {
		return s + "(" + a.Param.String() + ", " + a.Expr.String() + ")"
	}
	return s + "(" + a.Expr.String() + ")"
}
func (b *Binary) String() string {
	return "(" + b.LHS.String() + " " + b.Op + " " + b.RHS.String() + ")"
}
func (p *Paren) String() string { return "(" + p.Expr.String() + ")" }
func (u *Unary) String() string { return u.Op + u.Expr.String() }

// ---------------------------------------------------------------------------
// Lexer

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tNumber
	tString
	tDuration
	tOp
	tLParen
	tRParen
	tLBrace
	tRBrace
	tLBracket
	tRBracket
	tComma
)

type token struct {
	kind tokKind
	val  string
	pos  int
}

func lex(s string) ([]token, error) {
	var out []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
			continue
		case c == '#':
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		case c == '(':
			out = append(out, token{tLParen, "(", i})
		case c == ')':
			out = append(out, token{tRParen, ")", i})
		case c == '{':
			out = append(out, token{tLBrace, "{", i})
		case c == '}':
			out = append(out, token{tRBrace, "}", i})
		case c == '[':
			// Range durations are lexed as one token: [5m]
			j := strings.IndexByte(s[i:], ']')
			if j < 0 {
				return nil, fmt.Errorf("unclosed '[' at %d", i)
			}
			out = append(out, token{tLBracket, "[", i}, token{tDuration, strings.TrimSpace(s[i+1 : i+j]), i + 1}, token{tRBracket, "]", i + j})
			i += j + 1
			continue
		case c == ',':
			out = append(out, token{tComma, ",", i})
		case c == '"' || c == '\'' || c == '`':
			j := i + 1
			var b strings.Builder
			for ; j < len(s) && s[j] != c; j++ {
				if s[j] == '\\' && c != '`' && j+1 < len(s) {
					j++
					switch s[j] {
					case 'n':
						b.WriteByte('\n')
					case 't':
						b.WriteByte('\t')
					default:
						b.WriteByte(s[j])
					}
					continue
				}
				b.WriteByte(s[j])
			}
			if j >= len(s) {
				return nil, fmt.Errorf("unterminated string at %d", i)
			}
			out = append(out, token{tString, b.String(), i})
			i = j + 1
			continue
		case strings.ContainsRune("+-*/%^=!<>", rune(c)):
			two := ""
			if i+1 < len(s) {
				two = s[i : i+2]
			}
			switch two {
			case "==", "!=", "<=", ">=", "=~", "!~":
				out = append(out, token{tOp, two, i})
				i += 2
				continue
			}
			if c == '!' {
				return nil, fmt.Errorf("unexpected '!' at %d", i)
			}
			out = append(out, token{tOp, string(c), i})
		case c >= '0' && c <= '9' || c == '.':
			j := i
			for j < len(s) && (unicode.IsDigit(rune(s[j])) || s[j] == '.' || s[j] == 'e' || s[j] == 'E' ||
				((s[j] == '+' || s[j] == '-') && j > i && (s[j-1] == 'e' || s[j-1] == 'E')) || s[j] == 'x' || s[j] == 'X' ||
				(j > i+1 && (s[i+1] == 'x' || s[i+1] == 'X') && strings.ContainsRune("abcdefABCDEF", rune(s[j])))) {
				j++
			}
			// Durations after offset, e.g. "5m", are glued to the number.
			k := j
			for k < len(s) && strings.ContainsRune("smhdwy", rune(s[k])) {
				k++
			}
			if k > j {
				out = append(out, token{tDuration, s[i:k], i})
				i = k
				continue
			}
			out = append(out, token{tNumber, s[i:j], i})
			i = j
			continue
		case c == '_' || c == ':' || unicode.IsLetter(rune(c)):
			j := i
			for j < len(s) && (s[j] == '_' || s[j] == ':' || unicode.IsLetter(rune(s[j])) || unicode.IsDigit(rune(s[j]))) {
				j++
			}
			out = append(out, token{tIdent, s[i:j], i})
			i = j
			continue
		default:
			return nil, fmt.Errorf("unexpected character %q at %d", c, i)
		}
		i++
	}
	return append(out, token{tEOF, "", len(s)}), nil
}

// ParseDuration accepts Prometheus durations like 5m, 1h30m, 2d, 1w.
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	var total time.Duration
	units := map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour,
		'w': 7 * 24 * time.Hour, 'y': 365 * 24 * time.Hour}
	for s != "" {
		j := 0
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j == 0 || j == len(s) {
			return 0, fmt.Errorf("bad duration %q", s)
		}
		n, _ := strconv.ParseInt(s[:j], 10, 64)
		u := time.Duration(0)
		rest := s[j:]
		if strings.HasPrefix(rest, "ms") {
			u, rest = time.Millisecond, rest[2:]
		} else if d, ok := units[rest[0]]; ok {
			u, rest = d, rest[1:]
		} else {
			return 0, fmt.Errorf("bad duration unit in %q", s)
		}
		total += time.Duration(n) * u
		s = rest
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// Parser (precedence climbing; ^ is right-associative)

var precedence = map[string]int{
	"or": 1, "and": 2, "unless": 2,
	"==": 3, "!=": 3, "<": 3, "<=": 3, ">": 3, ">=": 3,
	"+": 4, "-": 4,
	"*": 5, "/": 5, "%": 5,
	"^": 6,
}

var rangeFunctions = map[string]bool{
	"rate": true, "irate": true, "increase": true, "delta": true, "avg_over_time": true, "sum_over_time": true,
	"min_over_time": true, "max_over_time": true, "count_over_time": true, "last_over_time": true,
}

var aggregations = map[string]bool{"sum": true, "avg": true, "min": true, "max": true, "count": true, "topk": true, "bottomk": true}

type parser struct {
	toks []token
	p    int
}

// Parse parses a PromQL expression.
func Parse(q string) (Expr, error) {
	toks, err := lex(q)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	e, err := p.expr(0)
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tEOF {
		return nil, fmt.Errorf("unexpected %q at %d", p.peek().val, p.peek().pos)
	}
	return e, nil
}

func (p *parser) peek() token { return p.toks[p.p] }
func (p *parser) next() token {
	t := p.toks[p.p]
	if p.p < len(p.toks)-1 {
		p.p++
	}
	return t
}

func (p *parser) binOp() (string, bool) {
	t := p.peek()
	if t.kind == tOp && t.val != "=" && t.val != "=~" && t.val != "!~" {
		return t.val, true
	}
	if t.kind == tIdent && (t.val == "and" || t.val == "or" || t.val == "unless") {
		return t.val, true
	}
	return "", false
}

func (p *parser) expr(minPrec int) (Expr, error) {
	lhs, err := p.unary()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.binOp()
		if !ok || precedence[op] < minPrec || precedence[op] == 0 {
			return lhs, nil
		}
		p.next()
		b := &Binary{Op: op, LHS: lhs}
		if p.peek().kind == tIdent && p.peek().val == "bool" {
			p.next()
			b.ReturnBool = true
		}
		if t := p.peek(); t.kind == tIdent && (t.val == "on" || t.val == "ignoring") {
			p.next()
			b.On, b.Ignoring = t.val == "on", t.val == "ignoring"
			if b.Matching, err = p.labelList(); err != nil {
				return nil, err
			}
		}
		next := precedence[op] + 1
		if op == "^" {
			next = precedence[op]
		}
		if b.RHS, err = p.expr(next); err != nil {
			return nil, err
		}
		lhs = b
	}
}

func (p *parser) unary() (Expr, error) {
	if t := p.peek(); t.kind == tOp && (t.val == "-" || t.val == "+") {
		p.next()
		e, err := p.expr(precedence["^"]) // unary binds looser than ^ only
		if err != nil {
			return nil, err
		}
		if n, ok := e.(*NumberLiteral); ok && t.val == "-" {
			return &NumberLiteral{-n.Val}, nil
		}
		return &Unary{Op: t.val, Expr: e}, nil
	}
	return p.postfix()
}

func (p *parser) postfix() (Expr, error) {
	e, err := p.primary()
	if err != nil {
		return nil, err
	}
	for {
		switch t := p.peek(); {
		case t.kind == tLBracket:
			vs, ok := e.(*VectorSelector)
			if !ok {
				return nil, fmt.Errorf("range can only follow a selector, at %d", t.pos)
			}
			p.next()
			d, err := ParseDuration(p.next().val)
			if err != nil {
				return nil, err
			}
			p.next() // ]
			e = &MatrixSelector{VS: vs, Range: d}
		case t.kind == tIdent && t.val == "offset":
			p.next()
			d, err := ParseDuration(p.next().val)
			if err != nil {
				return nil, err
			}
			switch x := e.(type) {
			case *VectorSelector:
				x.Offset = d
			case *MatrixSelector:
				x.VS.Offset = d
			default:
				return nil, fmt.Errorf("offset must follow a selector")
			}
		default:
			return e, nil
		}
	}
}

func (p *parser) primary() (Expr, error) {
	t := p.next()
	switch t.kind {
	case tNumber:
		v, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			if iv, err2 := strconv.ParseInt(t.val, 0, 64); err2 == nil {
				return &NumberLiteral{float64(iv)}, nil
			}
			return nil, fmt.Errorf("bad number %q", t.val)
		}
		return &NumberLiteral{v}, nil
	case tLParen:
		e, err := p.expr(0)
		if err != nil {
			return nil, err
		}
		if p.next().kind != tRParen {
			return nil, fmt.Errorf("expected ')'")
		}
		return &Paren{e}, nil
	case tLBrace:
		p.p--
		return p.selector("")
	case tIdent:
		switch t.val {
		case "Inf", "inf":
			return &NumberLiteral{posInf}, nil
		case "NaN", "nan":
			return &NumberLiteral{nan}, nil
		}
		if aggregations[t.val] {
			return p.aggregate(t.val)
		}
		if p.peek().kind == tLParen {
			return p.call(t.val)
		}
		return p.selector(t.val)
	}
	return nil, fmt.Errorf("unexpected %q at %d", t.val, t.pos)
}

func (p *parser) labelList() ([]string, error) {
	if p.next().kind != tLParen {
		return nil, fmt.Errorf("expected '(' before label list")
	}
	var out []string
	for p.peek().kind != tRParen {
		t := p.next()
		if t.kind != tIdent {
			return nil, fmt.Errorf("expected label name, got %q", t.val)
		}
		out = append(out, t.val)
		if p.peek().kind == tComma {
			p.next()
		}
	}
	p.next()
	return out, nil
}

func (p *parser) aggregate(op string) (Expr, error) {
	a := &Aggregate{Op: op}
	parseMod := func() error {
		t := p.peek()
		if t.kind == tIdent && (t.val == "by" || t.val == "without") {
			p.next()
			a.By, a.Without = t.val == "by", t.val == "without"
			var err error
			a.Labels, err = p.labelList()
			return err
		}
		return nil
	}
	if err := parseMod(); err != nil {
		return nil, err
	}
	if p.next().kind != tLParen {
		return nil, fmt.Errorf("expected '(' after %s", op)
	}
	first, err := p.expr(0)
	if err != nil {
		return nil, err
	}
	if op == "topk" || op == "bottomk" {
		if p.next().kind != tComma {
			return nil, fmt.Errorf("%s needs a parameter", op)
		}
		a.Param = first
		if a.Expr, err = p.expr(0); err != nil {
			return nil, err
		}
	} else {
		a.Expr = first
	}
	if p.next().kind != tRParen {
		return nil, fmt.Errorf("expected ')' to close %s", op)
	}
	if !a.By && !a.Without {
		if err := parseMod(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (p *parser) call(name string) (Expr, error) {
	if _, ok := functions[name]; !ok {
		return nil, fmt.Errorf("unknown function %q", name)
	}
	p.next() // (
	c := &Call{Func: name}
	for p.peek().kind != tRParen {
		e, err := p.expr(0)
		if err != nil {
			return nil, err
		}
		c.Args = append(c.Args, e)
		if p.peek().kind == tComma {
			p.next()
		} else if p.peek().kind != tRParen {
			return nil, fmt.Errorf("expected ',' or ')' in call to %s", name)
		}
	}
	p.next()
	if want := functions[name].args; want >= 0 && len(c.Args) != want {
		return nil, fmt.Errorf("%s expects %d argument(s), got %d", name, want, len(c.Args))
	}
	if rangeFunctions[name] {
		if _, ok := c.Args[0].(*MatrixSelector); !ok {
			return nil, fmt.Errorf("%s expects a range vector argument, e.g. %s(x[5m])", name, name)
		}
	}
	return c, nil
}

func (p *parser) selector(name string) (Expr, error) {
	vs := &VectorSelector{Name: name}
	if name != "" {
		vs.Matchers = append(vs.Matchers, labels.MustMatcher(labels.MatchEqual, labels.MetricName, name))
	}
	if p.peek().kind == tLBrace {
		p.next()
		for p.peek().kind != tRBrace {
			lt := p.next()
			if lt.kind != tIdent {
				return nil, fmt.Errorf("expected label name, got %q", lt.val)
			}
			ot := p.next()
			var mt labels.MatchType
			switch ot.val {
			case "=":
				mt = labels.MatchEqual
			case "!=":
				mt = labels.MatchNotEqual
			case "=~":
				mt = labels.MatchRegexp
			case "!~":
				mt = labels.MatchNotRegexp
			default:
				return nil, fmt.Errorf("bad matcher operator %q", ot.val)
			}
			vt := p.next()
			if vt.kind != tString {
				return nil, fmt.Errorf("expected string after %s%s", lt.val, ot.val)
			}
			m, err := labels.NewMatcher(mt, lt.val, vt.val)
			if err != nil {
				return nil, err
			}
			vs.Matchers = append(vs.Matchers, m)
			if p.peek().kind == tComma {
				p.next()
			}
		}
		p.next()
	}
	if len(vs.Matchers) == 0 {
		return nil, fmt.Errorf("vector selector must contain at least one matcher")
	}
	empty := true
	for _, m := range vs.Matchers {
		if !m.Matches("") {
			empty = false
		}
	}
	if empty {
		return nil, fmt.Errorf("vector selector must contain at least one non-empty matcher")
	}
	return vs, nil
}
