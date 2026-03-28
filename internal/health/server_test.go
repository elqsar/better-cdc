package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"better-cdc/internal/publisher"
)

func TestNewHandler_HealthEndpoint(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	NewHandler(Options{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "ok" {
		t.Fatalf("unexpected body %q", body)
	}
}

func TestNewHandler_ReadyEndpointFailsChecks(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	handler := NewHandler(Options{
		Readiness: []Check{
			{
				Name: "postgres",
				Func: func(context.Context) error { return errors.New("down") },
			},
		},
	})
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected failure details in body")
	}
}

func TestNewHandler_ReadyEndpointFailsForNoopPublisher(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	pub := publisher.NewNoopPublisher()

	handler := NewHandler(Options{
		Readiness: []Check{
			{
				Name: "publisher",
				Func: func(ctx context.Context) error {
					return pub.Ready(ctx)
				},
			},
		},
	})
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected noop publisher failure details in body")
	}
}

func TestNewHandler_PprofDisabledByDefault(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()

	NewHandler(Options{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestNewHandler_PprofEnabled(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()

	NewHandler(Options{EnablePprof: true}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestStart_ReturnsErrorWhenAddrInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = Start(ctx, Options{Addr: ln.Addr().String()})
	if err == nil {
		t.Fatal("expected bind error, got nil")
	}
}

func TestStart_ServesHealthEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := Start(ctx, Options{Addr: addr}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + addr + "/health")
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("health endpoint at %s did not become ready", addr)
}
