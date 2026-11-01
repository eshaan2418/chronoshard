package scrape

import (
	"math"
	"strings"
	"testing"

	"github.com/eshaan2418/chronoshard/labels"
)

func TestParseText(t *testing.T) {
	in := `# HELP http_requests_total Total requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="get",code="400",path="/a\"b"} 3
go_goroutines 42
weird_inf +Inf
rpc_duration_seconds{quantile="0.5"} 4773 # {trace_id="abc"} 1.0
`
	ss, err := ParseText(strings.NewReader(in), 99, labels.FromStrings("job", "x"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 5 {
		t.Fatalf("%d series", len(ss))
	}
	if ss[0].Samples[0].T != 1395066363000 || ss[0].Labels.Get("job") != "x" || ss[0].Labels.Get("code") != "200" {
		t.Fatalf("%+v", ss[0])
	}
	if ss[1].Labels.Get("path") != `/a"b` || ss[1].Samples[0].T != 99 {
		t.Fatalf("%+v", ss[1])
	}
	if !math.IsInf(ss[3].Samples[0].V, 1) || ss[4].Samples[0].V != 4773 {
		t.Fatalf("%+v %+v", ss[3], ss[4])
	}
	if _, err := ParseText(strings.NewReader("bad{x=\"1\" 2\n"), 0, nil); err == nil {
		t.Fatal("malformed line accepted")
	}
}
