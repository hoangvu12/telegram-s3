package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
)

// bucketSubresourceKeys are the bucket-config query subresources S3 clients
// probe during connect/setup. "delete" is deliberately NOT here — that is the
// bulk DeleteObjects POST (P7.2), a data operation, not a config subresource.
var bucketSubresourceKeys = []string{
	"location", "versioning", "acl", "cors", "tagging", "policy",
	"lifecycle", "website", "encryption", "object-lock", "notification",
	"logging", "replication", "accelerate", "requestPayment", "uploads",
	"ownershipControls", "publicAccessBlock", "analytics", "metrics",
	"inventory", "intelligent-tiering",
}

func isBucketSubresource(q url.Values) bool {
	for _, k := range bucketSubresourceKeys {
		if _, ok := q[k]; ok {
			return true
		}
	}
	return false
}

// bucketSubresource answers the bucket-config probes. The gateway stores no
// bucket-level configuration, so reads return the canned "default"/"absent"
// response S3 would give an unconfigured bucket and writes are accepted as a
// no-op. ?uploads is the one real one (ListMultipartUploads from the store).
func (h *Handler) bucketSubresource(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string, q url.Values) {
	exists, err := h.store.BucketExists(ctx, bucket)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if !exists {
		h.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
		return
	}

	// PUT/DELETE of a subresource (put-bucket-cors, delete-bucket-tagging, ...)
	// is accepted as a no-op so clients that try to configure a bucket during
	// setup don't get a 501. Nothing is persisted.
	switch r.Method {
	case http.MethodPut:
		w.WriteHeader(http.StatusOK)
		return
	case http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
		return
	}

	canned := func(root string) {
		h.writeXMLString(w, http.StatusOK, fmt.Sprintf(`<%s xmlns=%q></%s>`, root, s3XMLNS, root))
	}
	switch {
	case q.Has("uploads"):
		h.listMultipartUploads(ctx, w, bucket)
	case q.Has("location"):
		canned("LocationConstraint") // empty == us-east-1
	case q.Has("versioning"):
		canned("VersioningConfiguration") // unversioned
	case q.Has("accelerate"):
		canned("AccelerateConfiguration")
	case q.Has("notification"):
		canned("NotificationConfiguration")
	case q.Has("logging"):
		canned("BucketLoggingStatus")
	case q.Has("requestPayment"):
		h.writeXMLString(w, http.StatusOK, fmt.Sprintf(
			`<RequestPaymentConfiguration xmlns=%q><Payer>BucketOwner</Payer></RequestPaymentConfiguration>`, s3XMLNS))
	case q.Has("acl"):
		id := xmlEscape(h.cfg.AccessKeyID)
		h.writeXMLString(w, http.StatusOK, fmt.Sprintf(
			`<AccessControlPolicy xmlns=%q><Owner><ID>%s</ID><DisplayName>%s</DisplayName></Owner>`+
				`<AccessControlList><Grant>`+
				`<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">`+
				`<ID>%s</ID><DisplayName>%s</DisplayName></Grantee>`+
				`<Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>`,
			s3XMLNS, id, id, id, id))
	case q.Has("cors"):
		h.writeError(w, http.StatusNotFound, "NoSuchCORSConfiguration", "The CORS configuration does not exist.")
	case q.Has("tagging"):
		h.writeError(w, http.StatusNotFound, "NoSuchTagSet", "There is no tag set associated with the bucket.")
	case q.Has("policy"):
		h.writeError(w, http.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist.")
	case q.Has("lifecycle"):
		h.writeError(w, http.StatusNotFound, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist.")
	case q.Has("website"):
		h.writeError(w, http.StatusNotFound, "NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration.")
	case q.Has("encryption"):
		h.writeError(w, http.StatusNotFound, "ServerSideEncryptionConfigurationNotFoundError", "The server side encryption configuration was not found.")
	case q.Has("object-lock"):
		h.writeError(w, http.StatusNotFound, "ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket.")
	case q.Has("replication"):
		h.writeError(w, http.StatusNotFound, "ReplicationConfigurationNotFoundError", "The replication configuration was not found.")
	case q.Has("ownershipControls"):
		h.writeError(w, http.StatusNotFound, "OwnershipControlsNotFoundError", "The bucket ownership controls were not found.")
	case q.Has("publicAccessBlock"):
		h.writeError(w, http.StatusNotFound, "NoSuchPublicAccessBlockConfiguration", "The public access block configuration was not found.")
	default: // analytics | metrics | inventory | intelligent-tiering
		h.writeError(w, http.StatusNotFound, "NoSuchConfiguration", "The specified configuration does not exist.")
	}
}

func (h *Handler) listMultipartUploads(ctx context.Context, w http.ResponseWriter, bucket string) {
	uploads, err := h.store.ListMultipartUploads(ctx, bucket)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	res := listMultipartUploadsResult{XMLNS: s3XMLNS, Bucket: bucket, MaxUploads: 1000, IsTruncated: false}
	for _, u := range uploads {
		res.Uploads = append(res.Uploads, uploadEntry{
			Key:       u.Key,
			UploadID:  u.UploadID,
			Initiated: u.CreatedAt.UTC().Format(awsListTimeFormat),
		})
	}
	h.writeXML(w, http.StatusOK, res)
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name      `xml:"ListMultipartUploadsResult"`
	XMLNS              string        `xml:"xmlns,attr"`
	Bucket             string        `xml:"Bucket"`
	KeyMarker          string        `xml:"KeyMarker"`
	UploadIDMarker     string        `xml:"UploadIdMarker"`
	NextKeyMarker      string        `xml:"NextKeyMarker"`
	NextUploadIDMarker string        `xml:"NextUploadIdMarker"`
	MaxUploads         int           `xml:"MaxUploads"`
	IsTruncated        bool          `xml:"IsTruncated"`
	Uploads            []uploadEntry `xml:"Upload"`
}

type uploadEntry struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}
