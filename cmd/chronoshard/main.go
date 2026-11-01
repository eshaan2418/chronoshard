// Command chronoshard runs the TSDB: Prometheus HTTP query API, remote write
// ingestion, optional scraping, WAL + snapshots, and retention.
//
//	chronoshard -data ./data -listen :9201 -scrape node=http://localhost:9100/metrics@15s
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/eshaan2418/chronoshard/api"
	"github.com/eshaan2418/chronoshard/scrape"
	"github.com/eshaan2418/chronoshard/storage"
)

type scrapeFlags []scrape.Target

func (s *scrapeFlags) String() string { return fmt.Sprint(*s) }
func (s *scrapeFlags) Set(v string) error {
	// job=url@interval
	job, rest, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("want job=url@interval, got %q", v)
	}
	url, iv, ok := strings.Cut(rest, "@")
	d := 15 * time.Second
	if ok {
		var err error
		if d, err = time.ParseDuration(iv); err != nil {
			return err
		}
	}
	*s = append(*s, scrape.Target{Job: job, URL: url, Interval: d})
	return nil
}

func main() {
	data := flag.String("data", "data", "data directory")
	listen := flag.String("listen", ":9201", "HTTP listen address")
	retention := flag.Duration("retention", 15*24*time.Hour, "drop data older than this")
	checkpoint := flag.Duration("checkpoint-interval", 10*time.Minute, "how often to snapshot and truncate the WAL")
	var targets scrapeFlags
	flag.Var(&targets, "scrape", "scrape target job=url@interval (repeatable)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	t0 := time.Now()
	head, err := storage.Open(*data)
	if err != nil {
		log.Error("open failed", "err", err)
		os.Exit(1)
	}
	st := head.Stats()
	log.Info("opened", "series", st.Series, "samples", st.Samples, "took", time.Since(t0).Round(time.Millisecond))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	for _, t := range targets {
		go scrape.Run(ctx, t, head, log)
	}
	go func() {
		tick := time.NewTicker(*checkpoint)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				n := head.Truncate(time.Now().Add(-*retention).UnixMilli())
				if err := head.Checkpoint(*data); err != nil {
					log.Error("checkpoint failed", "err", err)
				} else {
					log.Info("checkpoint", "dropped_series", n)
				}
			}
		}
	}()
	srv := &http.Server{Addr: *listen, Handler: api.New(head), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		srv.Shutdown(shutdown)
	}()
	log.Info("listening", "addr", *listen)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("serve", "err", err)
	}
	if err := head.Checkpoint(*data); err != nil {
		log.Error("final checkpoint failed", "err", err)
	}
	if err := head.Close(); err != nil {
		log.Error("close failed", "err", err)
	}
}
