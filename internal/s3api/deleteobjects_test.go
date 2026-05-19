package s3api

import (
	"encoding/xml"
	"net/http"
	"testing"
)

func TestDeleteObjectsBulk(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "k1", "k2", "k3", "k4")

	body := `<Delete><Object><Key>k1</Key></Object><Object><Key>k3</Key></Object></Delete>`
	rec := r.do(http.MethodPost, "/send?delete", []byte(body))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete objects: %d %s", rec.Code, rec.Body)
	}
	var res deleteResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("parse: %v body=%s", err, rec.Body)
	}
	if len(res.Deleted) != 2 || len(res.Errors) != 0 {
		t.Fatalf("result = %+v", res)
	}
	for _, k := range []string{"k1", "k3"} {
		if rec := getWith(r, "/send/"+k, nil); rec.Code != http.StatusNotFound {
			t.Fatalf("%s should be 404 after bulk delete, got %d", k, rec.Code)
		}
	}
	for _, k := range []string{"k2", "k4"} {
		if rec := getWith(r, "/send/"+k, nil); rec.Code != http.StatusOK {
			t.Fatalf("%s should remain, got %d", k, rec.Code)
		}
	}
}

func TestDeleteObjectsQuiet(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a", "b")

	body := `<Delete><Quiet>true</Quiet><Object><Key>a</Key></Object></Delete>`
	rec := r.do(http.MethodPost, "/send?delete", []byte(body))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	var res deleteResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(res.Deleted) != 0 {
		t.Fatalf("Quiet must omit <Deleted>, got %+v", res.Deleted)
	}
	if rec := getWith(r, "/send/a", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("a should be deleted despite Quiet, got %d", rec.Code)
	}
}

// A missing key is idempotently "deleted", not an error (matches S3).
func TestDeleteObjectsMissingKeyIsDeleted(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "exists")

	body := `<Delete><Object><Key>exists</Key></Object><Object><Key>ghost</Key></Object></Delete>`
	rec := r.do(http.MethodPost, "/send?delete", []byte(body))
	var res deleteResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("parse: %v body=%s", err, rec.Body)
	}
	if len(res.Errors) != 0 {
		t.Fatalf("missing key must not be an Error: %+v", res.Errors)
	}
	if len(res.Deleted) != 2 {
		t.Fatalf("both keys reported Deleted (idempotent), got %+v", res.Deleted)
	}
}

func TestDeleteObjectsMalformed(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	if rec := r.do(http.MethodPost, "/send?delete", []byte(`not xml`)); rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed body: %d, want 400", rec.Code)
	}
}
