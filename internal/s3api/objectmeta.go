package s3api

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"telegram-s3/internal/metadata"
)

// metadataHeaders are the system headers persisted in the object_metadata side
// table (lower-cased). Content-Type stays on the objects row (unchanged) and
// is handled separately.
var metadataHeaders = []string{
	"Content-Disposition", "Content-Encoding", "Cache-Control", "Expires",
}

// checksumHeaders are the AWS flexible-checksum headers (P8.4). They are
// captured at PUT/UploadPart and echoed on GET/HEAD; the body is **not**
// verified server-side (matches the chunked trailer's existing behavior in
// chunked.go:49). The algorithm hint travels with them.
var checksumHeaders = []string{
	"x-amz-checksum-crc32", "x-amz-checksum-crc32c",
	"x-amz-checksum-sha1", "x-amz-checksum-sha256",
	"x-amz-checksum-algorithm",
}

// captureObjectMetadata extracts the persisted system headers plus every
// x-amz-meta-* header (name lower-cased, value verbatim) from a PUT/Copy/MPU
// request. Returns nil when there is nothing to store (legacy-equivalent).
func captureObjectMetadata(h http.Header) map[string]string {
	md := map[string]string{}
	for _, name := range metadataHeaders {
		if v := h.Get(name); v != "" {
			md[strings.ToLower(name)] = v
		}
	}
	for _, name := range checksumHeaders {
		if v := h.Get(name); v != "" {
			md[name] = v
		}
	}
	for name, vals := range h {
		if len(vals) == 0 {
			continue
		}
		if ln := strings.ToLower(name); strings.HasPrefix(ln, "x-amz-meta-") {
			md[ln] = vals[0]
		}
	}
	if len(md) == 0 {
		return nil
	}
	return md
}

// applyObjectHeaders sets the response headers shared by GET and HEAD: the
// content type, validators, cache directive, and the echoed side-table
// metadata. GET response-* query overrides (allowed on the public-read path)
// take precedence over both the stored value and the defaults.
func applyObjectHeaders(w http.ResponseWriter, q url.Values, obj metadata.Object, md map[string]string) {
	ct := obj.ContentType
	if v := q.Get("response-content-type"); v != "" {
		ct = v
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("ETag", quoteETag(obj.ETag))
	w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))

	// Default keeps the public-cache design (HANDOFF: reads are public,
	// immutable); a stored or requested Cache-Control overrides it.
	cc := "public, max-age=31536000, immutable"
	if v, ok := md["cache-control"]; ok {
		cc = v
	}
	if v := q.Get("response-cache-control"); v != "" {
		cc = v
	}
	w.Header().Set("Cache-Control", cc)

	echo := func(stored, header, override string) {
		val := md[stored]
		if o := q.Get(override); o != "" {
			val = o
		}
		if val != "" {
			w.Header().Set(header, val)
		}
	}
	echo("content-disposition", "Content-Disposition", "response-content-disposition")
	echo("content-encoding", "Content-Encoding", "response-content-encoding")
	echo("expires", "Expires", "response-expires")
	for k, v := range md {
		if strings.HasPrefix(k, "x-amz-meta-") {
			w.Header().Set(k, v)
		}
	}
	// Echo P8.4 checksum headers (algorithm hint + per-algo digest). Stored
	// values are already lower-cased; net/http normalizes the wire form.
	for k, v := range md {
		if strings.HasPrefix(k, "x-amz-checksum-") {
			w.Header().Set(k, v)
		}
	}
}

// etagMatches implements RFC 7232 list matching for If-Match / If-None-Match:
// `*` matches any existing object; otherwise any (optionally weak) quoted
// entity-tag equal to the object's ETag matches.
func etagMatches(headerVal, etag string) bool {
	for _, tok := range strings.Split(headerVal, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "*" {
			return true
		}
		tok = strings.TrimPrefix(tok, "W/")
		if strings.Trim(tok, `"`) == etag {
			return true
		}
	}
	return false
}

// conditionalStatus evaluates the precedence chain (RFC 7232 / S3):
// If-Match → If-Unmodified-Since → If-None-Match → If-Modified-Since.
// It returns 412, 304, or 0 (no precondition triggered → proceed).
func conditionalStatus(r *http.Request, obj metadata.Object) int {
	lastMod := obj.UpdatedAt.UTC().Truncate(time.Second)

	if im := r.Header.Get("If-Match"); im != "" {
		if !etagMatches(im, obj.ETag) {
			return http.StatusPreconditionFailed
		}
	} else if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && lastMod.After(t) {
			return http.StatusPreconditionFailed
		}
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etagMatches(inm, obj.ETag) {
			return http.StatusNotModified
		}
	} else if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !lastMod.After(t) {
			return http.StatusNotModified
		}
	}
	return 0
}

// writeConditional emits the precondition response: a bodyless 304 (with
// validators, per RFC 7232) or the S3 PreconditionFailed error.
func (h *Handler) writeConditional(w http.ResponseWriter, status int, obj metadata.Object) {
	if status == http.StatusNotModified {
		w.Header().Set("ETag", quoteETag(obj.ETag))
		w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusNotModified)
		return
	}
	h.writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the preconditions you specified did not hold.")
}
