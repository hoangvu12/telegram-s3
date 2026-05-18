package s3api

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"telegram-s3/internal/config"
	"telegram-s3/internal/metadata"
	"telegram-s3/internal/storage"
)

type Handler struct {
	cfg     config.Config
	store   *metadata.Store
	backend storage.Backend
	logger  *slog.Logger
}

func NewHandler(cfg config.Config, store *metadata.Store, backend storage.Backend, logger *slog.Logger) *Handler {
	return &Handler{cfg: cfg, store: store, backend: backend, logger: logger}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !h.authorized(r) {
		h.writeError(w, http.StatusForbidden, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
		return
	}

	bucket, key := parsePath(r.URL.Path)
	ctx := r.Context()

	switch {
	case r.Method == http.MethodGet && bucket == "":
		h.listBuckets(ctx, w)
	case r.Method == http.MethodPut && bucket != "" && key == "":
		h.createBucket(ctx, w, bucket)
	case r.Method == http.MethodHead && bucket != "" && key == "":
		h.headBucket(ctx, w, bucket)
	case r.Method == http.MethodDelete && bucket != "" && key == "":
		h.deleteBucket(ctx, w, bucket)
	case r.Method == http.MethodPut && bucket != "" && key != "":
		h.putObject(ctx, w, r, bucket, key)
	case r.Method == http.MethodGet && bucket != "" && key != "":
		h.getObject(ctx, w, r, bucket, key)
	case r.Method == http.MethodHead && bucket != "" && key != "":
		h.headObject(ctx, w, bucket, key)
	case r.Method == http.MethodDelete && bucket != "" && key != "":
		h.deleteObject(ctx, w, bucket, key)
	case r.Method == http.MethodGet && bucket != "":
		h.listObjects(ctx, w, r, bucket)
	default:
		h.writeError(w, http.StatusNotImplemented, "NotImplemented", "This S3 operation is not implemented yet.")
	}
}

func (h *Handler) createBucket(ctx context.Context, w http.ResponseWriter, bucket string) {
	if err := h.store.CreateBucket(ctx, bucket); err != nil {
		if strings.Contains(err.Error(), "constraint") {
			h.writeError(w, http.StatusConflict, "BucketAlreadyOwnedByYou", "Bucket already exists.")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteBucket(ctx context.Context, w http.ResponseWriter, bucket string) {
	objects, err := h.store.ListObjects(ctx, bucket, "", 1)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if len(objects) > 0 {
		h.writeError(w, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty.")
		return
	}
	if err := h.store.DeleteBucket(ctx, bucket); err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) headBucket(ctx context.Context, w http.ResponseWriter, bucket string) {
	exists, err := h.store.BucketExists(ctx, bucket)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) listBuckets(ctx context.Context, w http.ResponseWriter) {
	buckets, err := h.store.ListBuckets(ctx)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	response := listAllMyBucketsResult{Owner: owner{ID: h.cfg.AccessKeyID, DisplayName: h.cfg.AccessKeyID}}
	for _, b := range buckets {
		response.Buckets.Bucket = append(response.Buckets.Bucket, bucketResult{Name: b.Name, CreationDate: b.CreatedAt.Format(time.RFC3339)})
	}
	h.writeXML(w, http.StatusOK, response)
}

func (h *Handler) putObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	exists, err := h.store.BucketExists(ctx, bucket)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if !exists {
		h.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	hasher := md5.New()
	reader := io.TeeReader(r.Body, hasher)
	uploaded, err := h.backend.Upload(ctx, key, contentType, reader)
	if err != nil {
		h.writeError(w, http.StatusBadGateway, "TelegramUploadFailed", err.Error())
		return
	}
	etag := hex.EncodeToString(hasher.Sum(nil))
	if err := h.store.PutObject(ctx, metadata.Object{
		Bucket:            bucket,
		Key:               key,
		Size:              r.ContentLength,
		ETag:              etag,
		ContentType:       contentType,
		TelegramFileID:    uploaded.FileID,
		TelegramMessageID: uploaded.MessageID,
	}); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.Header().Set("ETag", quoteETag(etag))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	if r.Header.Get("Range") != "" {
		h.writeError(w, http.StatusNotImplemented, "NotImplemented", "Range reads are not implemented yet.")
		return
	}
	obj, err := h.store.GetObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	body, err := h.backend.Download(ctx, obj.TelegramFileID)
	if err != nil {
		h.writeError(w, http.StatusBadGateway, "TelegramDownloadFailed", err.Error())
		return
	}
	defer body.Close()

	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("ETag", quoteETag(obj.ETag))
	w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, body)
}

func (h *Handler) headObject(ctx context.Context, w http.ResponseWriter, bucket, key string) {
	obj, err := h.store.GetObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("ETag", quoteETag(obj.ETag))
	w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteObject(ctx context.Context, w http.ResponseWriter, bucket, key string) {
	if err := h.store.DeleteObject(ctx, bucket, key); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listObjects(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	maxKeys, _ := strconv.Atoi(r.URL.Query().Get("max-keys"))
	objects, err := h.store.ListObjects(ctx, bucket, prefix, maxKeys)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	result := listBucketResult{Name: bucket, Prefix: prefix, MaxKeys: maxKeysOrDefault(maxKeys), IsTruncated: false}
	for _, obj := range objects {
		result.Contents = append(result.Contents, objectResult{Key: obj.Key, LastModified: obj.UpdatedAt.Format(time.RFC3339), ETag: quoteETag(obj.ETag), Size: obj.Size, StorageClass: "STANDARD"})
	}
	h.writeXML(w, http.StatusOK, result)
}

func (h *Handler) authorized(r *http.Request) bool {
	if h.cfg.SecretAccessKey == "" || h.cfg.AccessKeyID == "" {
		return false
	}
	if sig := r.URL.Query().Get("X-Amz-Signature"); sig != "" {
		return h.authorizedPresigned(r, sig)
	}
	auth := r.Header.Get("Authorization")
	if auth == "" || !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		return false
	}

	fields := parseAuthFields(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "))
	credential := fields["Credential"]
	signedHeaders := fields["SignedHeaders"]
	signature := fields["Signature"]
	if credential == "" || signedHeaders == "" || signature == "" {
		return false
	}
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[0] != h.cfg.AccessKeyID {
		return false
	}
	date, region, service := parts[1], parts[2], parts[3]
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return false
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	canonicalRequest := canonicalRequest(r.Method, r.URL.EscapedPath(), canonicalQuery(r.URL.Query(), ""), canonicalHeaders(r, signedHeaders), signedHeaders, payloadHash)
	stringToSign := stringToSign(amzDate, date, region, service, canonicalRequest)
	expected := sign(h.cfg.SecretAccessKey, date, region, service, stringToSign)
	return hmac.Equal([]byte(expected), []byte(signature))
}

func (h *Handler) authorizedPresigned(r *http.Request, provided string) bool {
	q := r.URL.Query()
	credential := q.Get("X-Amz-Credential")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	amzDate := q.Get("X-Amz-Date")
	expires := q.Get("X-Amz-Expires")
	if credential == "" || signedHeaders == "" || amzDate == "" || expires == "" {
		return false
	}
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[0] != h.cfg.AccessKeyID {
		return false
	}
	date, region, service := parts[1], parts[2], parts[3]
	payloadHash := "UNSIGNED-PAYLOAD"
	canonicalRequest := canonicalRequest(r.Method, r.URL.EscapedPath(), canonicalQuery(q, "X-Amz-Signature"), canonicalHeaders(r, signedHeaders), signedHeaders, payloadHash)
	stringToSign := stringToSign(amzDate, date, region, service, canonicalRequest)
	expected := sign(h.cfg.SecretAccessKey, date, region, service, stringToSign)
	return hmac.Equal([]byte(expected), []byte(provided))
}

func parsePath(path string) (string, string) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", ""
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	key, _ := url.PathUnescape(parts[1])
	return parts[0], key
}

func parseAuthFields(value string) map[string]string {
	fields := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			fields[kv[0]] = kv[1]
		}
	}
	return fields
}

func canonicalRequest(method, path, query, headers, signedHeaders, payloadHash string) string {
	if path == "" {
		path = "/"
	}
	return strings.Join([]string{method, path, query, headers, signedHeaders, payloadHash}, "\n")
}

func canonicalQuery(values url.Values, skip string) string {
	query := values.Encode()
	if skip == "" || query == "" {
		return query
	}
	filtered := url.Values{}
	for key, vals := range values {
		if key == skip {
			continue
		}
		for _, val := range vals {
			filtered.Add(key, val)
		}
	}
	return filtered.Encode()
}

func canonicalHeaders(r *http.Request, signedHeaders string) string {
	var builder strings.Builder
	for _, name := range strings.Split(signedHeaders, ";") {
		lower := strings.ToLower(name)
		value := ""
		if lower == "host" {
			value = r.Host
		} else {
			value = r.Header.Get(name)
		}
		builder.WriteString(lower)
		builder.WriteByte(':')
		builder.WriteString(strings.Join(strings.Fields(value), " "))
		builder.WriteByte('\n')
	}
	return builder.String()
}

func stringToSign(amzDate, date, region, service, canonicalRequest string) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	return fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s/%s/%s/aws4_request\n%s", amzDate, date, region, service, hex.EncodeToString(sum[:]))
}

func sign(secret, date, region, service, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func quoteETag(etag string) string { return `"` + etag + `"` }

func maxKeysOrDefault(maxKeys int) int {
	if maxKeys <= 0 || maxKeys > 1000 {
		return 1000
	}
	return maxKeys
}

func (h *Handler) writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(value)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, message string) {
	h.writeXML(w, status, errorResponse{Code: code, Message: message})
}

var _ hash.Hash = md5.New()

type errorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   owner    `xml:"Owner"`
	Buckets buckets  `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type buckets struct {
	Bucket []bucketResult `xml:"Bucket"`
}

type bucketResult struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketResult struct {
	XMLName     xml.Name       `xml:"ListBucketResult"`
	Name        string         `xml:"Name"`
	Prefix      string         `xml:"Prefix"`
	MaxKeys     int            `xml:"MaxKeys"`
	IsTruncated bool           `xml:"IsTruncated"`
	Contents    []objectResult `xml:"Contents"`
}

type objectResult struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}
