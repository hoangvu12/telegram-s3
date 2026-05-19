package s3api

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
)

func TestBucketSubresourceLocationVersioning(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a")

	rec := signedGet(r, "/send?location")
	if rec.Code != http.StatusOK {
		t.Fatalf("?location: %d %s", rec.Code, rec.Body)
	}
	if b := rec.Body.String(); !strings.Contains(b, "<LocationConstraint") || !strings.Contains(b, `xmlns="`+s3XMLNS+`"`) {
		t.Fatalf("?location body = %s", b)
	}

	rec = signedGet(r, "/send?versioning")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<VersioningConfiguration") {
		t.Fatalf("?versioning: %d %s", rec.Code, rec.Body)
	}

	rec = signedGet(r, "/send?acl")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<Permission>FULL_CONTROL</Permission>") {
		t.Fatalf("?acl: %d %s", rec.Code, rec.Body)
	}
}

func TestBucketSubresource404s(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a")

	cases := map[string]string{
		"tagging":     "NoSuchTagSet",
		"cors":        "NoSuchCORSConfiguration",
		"policy":      "NoSuchBucketPolicy",
		"lifecycle":   "NoSuchLifecycleConfiguration",
		"website":     "NoSuchWebsiteConfiguration",
		"encryption":  "ServerSideEncryptionConfigurationNotFoundError",
		"replication": "ReplicationConfigurationNotFoundError",
	}
	for sub, code := range cases {
		t.Run(sub, func(t *testing.T) {
			rec := signedGet(r, "/send?"+sub)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("?%s: status %d, want 404 (body %s)", sub, rec.Code, rec.Body)
			}
			var er errorResponse
			if err := xml.Unmarshal(rec.Body.Bytes(), &er); err != nil || er.Code != code {
				t.Fatalf("?%s: error code = %q (parse %v), want %q", sub, er.Code, err, code)
			}
		})
	}
}

func TestBucketSubresourceNoSuchBucket(t *testing.T) {
	r := newMPRig(t)
	rec := signedGet(r, "/nope?location")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
	var er errorResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &er); err != nil || er.Code != "NoSuchBucket" {
		t.Fatalf("error code = %q (parse %v), want NoSuchBucket", er.Code, err)
	}
}

func TestBucketSubresourcePutDeleteNoop(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a")

	if rec := r.do(http.MethodPut, "/send?cors", []byte(`<CORSConfiguration/>`)); rec.Code != http.StatusOK {
		t.Fatalf("PUT ?cors no-op: %d %s, want 200", rec.Code, rec.Body)
	}
	if rec := r.do(http.MethodDelete, "/send?tagging", nil); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE ?tagging no-op: %d %s, want 204", rec.Code, rec.Body)
	}
	// The bucket and its object are untouched by the no-op subresource writes.
	if rec := signedGet(r, "/send"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<Key>a</Key>") {
		t.Fatalf("bucket damaged by no-op subresource write: %d %s", rec.Code, rec.Body)
	}
}

func TestListMultipartUploads(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	var init initiateMultipartUploadResult
	xml.Unmarshal(r.do(http.MethodPost, "/send/big.bin?uploads", nil).Body.Bytes(), &init)
	if init.UploadID == "" {
		t.Fatal("no upload id")
	}

	rec := signedGet(r, "/send?uploads")
	if rec.Code != http.StatusOK {
		t.Fatalf("?uploads: %d %s", rec.Code, rec.Body)
	}
	var res listMultipartUploadsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("parse: %v body=%s", err, rec.Body)
	}
	if res.Bucket != "send" || res.MaxUploads != 1000 || res.IsTruncated {
		t.Fatalf("envelope = %+v", res)
	}
	if len(res.Uploads) != 1 || res.Uploads[0].Key != "big.bin" || res.Uploads[0].UploadID != init.UploadID {
		t.Fatalf("uploads = %+v", res.Uploads)
	}
}

// Phase-6 listing must still work — the subresource arm only fires when a
// recognized config subresource key is present, not for list params.
func TestSubresourceListingRegression(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "p/a", "p/b", "top")

	plain := listV1(t, r, "/send")
	eq(t, "plain list", keysOf(plain.Contents), []string{"p/a", "p/b", "top"})

	v2 := listV2(t, r, "/send?list-type=2")
	eq(t, "v2 list", keysOf(v2.Contents), []string{"p/a", "p/b", "top"})

	rolled := listV2(t, r, "/send?list-type=2&delimiter=/")
	eq(t, "rollup keys", keysOf(rolled.Contents), []string{"top"})
	eq(t, "rollup prefixes", prefixesOf(rolled.CommonPrefixes), []string{"p/"})
}
