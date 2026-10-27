package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestVerify(t *testing.T) {
	secret := []byte("s3cret")
	body := []byte(`{"id":"evt_1"}`)
	now := time.Unix(1_800_000_000, 0)
	sig := func(ts string, b []byte) string { return "sha256=" + hex.EncodeToString(Sign(secret, ts, b)) }
	ts := strconv.FormatInt(now.Unix(), 10)
	old := strconv.FormatInt(now.Add(-10*time.Minute).Unix(), 10)

	plain := Verifier{Secret: secret, Header: "X-Signature", Prefix: "sha256="}
	stamped := Verifier{Secret: secret, Header: "X-Signature", Prefix: "sha256=",
		TimestampHeader: "X-Timestamp", Tolerance: 5 * time.Minute, Now: func() time.Time { return now }}

	cases := []struct {
		name    string
		v       Verifier
		headers map[string]string
		want    error
	}{
		{"valid", plain, map[string]string{"X-Signature": sig("", body)}, nil},
		{"missing header", plain, nil, ErrMissingSignature},
		{"wrong secret", plain, map[string]string{"X-Signature": "sha256=" + hex.EncodeToString(Sign([]byte("nope"), "", body))}, ErrBadSignature},
		{"tampered body", plain, map[string]string{"X-Signature": sig("", []byte(`{"id":"evt_2"}`))}, ErrBadSignature},
		{"missing prefix", plain, map[string]string{"X-Signature": hex.EncodeToString(Sign(secret, "", body))}, ErrBadSignature},
		{"not hex", plain, map[string]string{"X-Signature": "sha256=zz"}, ErrBadSignature},
		{"valid with timestamp", stamped, map[string]string{"X-Signature": sig(ts, body), "X-Timestamp": ts}, nil},
		{"expired", stamped, map[string]string{"X-Signature": sig(old, body), "X-Timestamp": old}, ErrExpired},
		{"timestamp swapped", stamped, map[string]string{"X-Signature": sig(old, body), "X-Timestamp": ts}, ErrBadSignature},
		{"missing timestamp", stamped, map[string]string{"X-Signature": sig(ts, body)}, ErrMissingTimestamp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			if err := tc.v.Verify(h, body); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestExtractKey(t *testing.T) {
	body := []byte(`{"data":{"id":"pay_123","n":12345678901234567890}}`)
	sum := sha256.Sum256(body)
	hash := "sha256:" + hex.EncodeToString(sum[:])
	hdr := http.Header{"Idempotency-Key": {"hdr-1"}}
	cases := []struct {
		name string
		spec KeySpec
		h    http.Header
		want string
	}{
		{"header wins", KeySpec{Header: "Idempotency-Key", Field: "data.id"}, hdr, "hdr-1"},
		{"json field", KeySpec{Header: "Idempotency-Key", Field: "data.id"}, http.Header{}, "pay_123"},
		{"big number stays exact", KeySpec{Field: "data.n"}, nil, "12345678901234567890"},
		{"missing field falls back to hash", KeySpec{Field: "data.nope"}, nil, hash},
		{"nothing configured", KeySpec{}, hdr, hash},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.spec.Extract(tc.h, body); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
