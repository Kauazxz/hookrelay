// Command hookrelay is a tiny webhook gateway: it verifies, de-duplicates and
// reliably forwards webhooks.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kauazxz/hookrelay/internal/config"
	"github.com/Kauazxz/hookrelay/internal/dispatch"
	"github.com/Kauazxz/hookrelay/internal/server"
	"github.com/Kauazxz/hookrelay/internal/store"
	"github.com/Kauazxz/hookrelay/internal/webhook"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "hookrelay:", err)
		os.Exit(1)
	}
}

func run() error {
	defPath := os.Getenv("HOOKRELAY_CONFIG")
	if defPath == "" {
		defPath = "config.json"
	}
	cfgPath := flag.String("config", defPath, "path to the JSON config file (env HOOKRELAY_CONFIG)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath, os.Getenv)
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return fmt.Errorf("log_level: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	st, err := openStore(cfg.Store)
	if err != nil {
		return err
	}
	defer st.Close()

	sources := make(map[string]server.Source, len(cfg.Sources))
	targets := make(map[string]string, len(cfg.Sources))
	for name, s := range cfg.Sources {
		sources[name] = server.Source{
			Verifier: webhook.Verifier{
				Secret:          []byte(s.Secret),
				Header:          s.SignatureHeader,
				Prefix:          s.SignaturePrefix,
				TimestampHeader: s.TimestampHeader,
				Tolerance:       s.Tolerance.D(),
			},
			Keys: webhook.KeySpec{Header: s.IdempotencyHeader, Field: s.IdempotencyField},
		}
		targets[name] = s.TargetURL
	}

	d := cfg.Dispatcher
	disp := dispatch.New(st, targets, dispatch.Options{
		Workers:        d.Workers,
		PollInterval:   d.PollInterval.D(),
		RequestTimeout: d.RequestTimeout.D(),
		MaxAttempts:    d.MaxAttempts,
		BackoffBase:    d.BackoffBase.D(),
		BackoffMax:     d.BackoffMax.D(),
	}, log)
	if cfg.AdminToken == "" {
		log.Warn("HOOKRELAY_ADMIN_TOKEN is not set: admin endpoints will reject every request")
	}
	api := server.New(st, sources, server.Options{AdminToken: cfg.AdminToken, MaxBodyBytes: cfg.MaxBodyBytes}, disp.Notify, log)
	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	// The dispatcher gets its own context so it is stopped only after the HTTP
	// server has finished accepting webhooks.
	dispCtx, stopDisp := context.WithCancel(context.Background())
	defer stopDisp()
	dispDone := make(chan struct{})
	go func() { disp.Run(dispCtx); close(dispDone) }()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.Addr, "store", cfg.Store.Type, "sources", len(sources))
		errc <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		stopDisp()
		<-dispDone
		return err
	case <-sigCtx.Done():
	}

	log.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), d.RequestTimeout.D()+15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	stopDisp()
	select {
	case <-dispDone:
		log.Info("dispatcher drained")
	case <-ctx.Done():
		log.Warn("dispatcher did not drain in time; unfinished events retry after their lease")
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func openStore(c config.Store) (store.Store, error) {
	if c.Type == "file" {
		return store.OpenFile(c.Path)
	}
	return store.NewMemory(), nil
}
