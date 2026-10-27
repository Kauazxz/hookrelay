// Package webhook holds the pure, transport-agnostic pieces of ingestion:
// HMAC signature verification and idempotency-key extraction.
package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	ErrMissingSignature = errors.New("missing signature header")
	ErrBadSignature     = errors.New("signature mismatch")
	ErrMissingTimestamp = errors.New("missing timestamp header")
	ErrBadTimestamp     = errors.New("malformed timestamp")
	ErrExpired          = errors.New("timestamp outside tolerance")
)

// Verifier checks an HMAC-SHA256 signature sent as lowercase or uppercase hex.
//
// Without TimestampHeader the signed payload is the raw body. With it, the
// payload is "<timestamp>.<body>" (the Stripe/Slack scheme), so an attacker
// cannot re-send an old body with a fresh timestamp, and Tolerance rejects
// captured requests replayed later.
type Verifier struct {
	Secret          []byte
	Header          string        // e.g. "X-Signature"
	Prefix          string        // optional, e.g. "sha256="
	TimestampHeader string        // optional, unix seconds
	Tolerance       time.Duration // 0 disables the age check
	Now             func() time.Time
}

// Verify returns nil when the request is authentic (and fresh, if configured).
func (v Verifier) Verify(h http.Header, body []byte) error {
	raw := strings.TrimSpace(h.Get(v.Header))
	if raw == "" {
		return ErrMissingSignature
	}
	hexSig, ok := strings.CutPrefix(raw, v.Prefix)
	if !ok {
		return ErrBadSignature
	}
	got, err := hex.DecodeString(hexSig)
	if err != nil {
		return ErrBadSignature
	}

	var ts string
	if v.TimestampHeader != "" {
		if ts = strings.TrimSpace(h.Get(v.TimestampHeader)); ts == "" {
			return ErrMissingTimestamp
		}
	}
	// hmac.Equal is constant-time, so response timing does not leak how many
	// leading bytes of a forged signature were right.
	if !hmac.Equal(got, Sign(v.Secret, ts, body)) {
		return ErrBadSignature
	}
	if ts == "" || v.Tolerance <= 0 {
		return nil
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	now := time.Now
	if v.Now != nil {
		now = v.Now
	}
	if age := now().Sub(time.Unix(sec, 0)); age > v.Tolerance || age < -v.Tolerance {
		return ErrExpired
	}
	return nil
}

// Sign computes the raw MAC. timestamp may be empty for body-only signing.
func Sign(secret []byte, timestamp string, body []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	if timestamp != "" {
		mac.Write([]byte(timestamp + "."))
	}
	mac.Write(body)
	return mac.Sum(nil)
}

// KeySpec says where a source puts its idempotency key. Header wins over
// Field; if neither yields a value the key is the SHA-256 of the body, which
// still collapses byte-identical re-deliveries.
type KeySpec struct {
	Header string // e.g. "Idempotency-Key"
	Field  string // dot path into a JSON body, e.g. "data.id"
}

// Extract returns the idempotency key for a request.
func (k KeySpec) Extract(h http.Header, body []byte) string {
	if k.Header != "" {
		if v := strings.TrimSpace(h.Get(k.Header)); v != "" {
			return v
		}
	}
	if k.Field != "" {
		if v, ok := jsonField(body, k.Field); ok {
			return v
		}
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// jsonField walks a dot path and returns a string or numeric leaf.
func jsonField(body []byte, path string) (string, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber() // keep large IDs exact instead of rounding through float64
	var cur any
	if err := dec.Decode(&cur); err != nil {
		return "", false
	}
	for _, part := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		if cur, ok = obj[part]; !ok {
			return "", false
		}
	}
	switch v := cur.(type) {
	case string:
		return v, v != ""
	case json.Number:
		return v.String(), true
	}
	return "", false
}
