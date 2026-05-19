package s3api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"telegram-s3/internal/metadata"
)

// parseCopySource decodes the x-amz-copy-source header. AWS accepts both
// `/{bucket}/{key}` and `{bucket}/{key}`, URL-encoded, optionally with a
// `?versionId=…` suffix (we are unversioned, so strip and ignore it). The
// first path segment is the bucket; everything after is the (encoded) key, so
// a key containing `/` survives. bucket/key are unescaped after splitting so a
// `%2F` in the key is not mistaken for a separator.
func parseCopySource(r *http.Request) (bucket, key string, ok bool) {
	raw := r.Header.Get("X-Amz-Copy-Source")
	if raw == "" {
		return "", "", false
	}
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i]
	}
	raw = strings.TrimPrefix(raw, "/")
	slash := strings.IndexByte(raw, '/')
	if slash <= 0 || slash == len(raw)-1 {
		return "", "", false
	}
	b, berr := url.PathUnescape(raw[:slash])
	k, kerr := url.PathUnescape(raw[slash+1:])
	if berr != nil {
		b = raw[:slash]
	}
	if kerr != nil {
		k = raw[slash+1:]
	}
	if b == "" || k == "" {
		return "", "", false
	}
	return b, k, true
}

// copyObject implements server-side CopyObject (PUT + x-amz-copy-source). The
// source is streamed through MD5 into a fresh set of Telegram messages; the
// destination's superseded chunks are reaped after the new object is durably
// recorded (same overwrite pattern as uploadPart). x-amz-metadata-directive is
// effectively COPY here (Content-Type carried); richer metadata lands in P7.5.
func (h *Handler) copyObject(ctx context.Context, w http.ResponseWriter, r *http.Request, dstBucket, dstKey string) {
	srcBucket, srcKey, ok := parseCopySource(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "InvalidArgument", "Invalid x-amz-copy-source.")
		return
	}
	src, srcChunks, ok := h.loadSource(ctx, w, srcBucket, srcKey)
	if !ok {
		return
	}

	// x-amz-metadata-directive: REPLACE takes the request's headers; COPY (the
	// default) carries the source's content-type + side-table metadata.
	contentType := src.ContentType
	var meta map[string]string
	if strings.EqualFold(r.Header.Get("X-Amz-Metadata-Directive"), "REPLACE") {
		if ct := r.Header.Get("Content-Type"); ct != "" {
			contentType = ct
		}
		meta = captureObjectMetadata(r.Header)
	} else {
		meta, _ = h.store.GetObjectMetadata(ctx, srcBucket, srcKey)
	}

	reader, err := h.openObject(ctx, src, srcChunks, nil)
	if err != nil {
		h.writeError(w, http.StatusBadGateway, "TelegramDownloadFailed", err.Error())
		return
	}
	defer reader.Close()

	hasher := md5.New()
	counter := &countingReader{r: io.TeeReader(reader, hasher)}
	chunks, err := h.backend.Upload(ctx, dstKey, contentType, counter)
	if err != nil {
		h.writeError(w, http.StatusBadGateway, "TelegramUploadFailed", err.Error())
		return
	}
	etag := hex.EncodeToString(hasher.Sum(nil))

	obj := metadata.Object{Bucket: dstBucket, Key: dstKey, Size: counter.n, ETag: etag, ContentType: contentType, Metadata: meta}
	if len(chunks) > 0 {
		obj.TelegramFileID = chunks[0].FileID
		obj.TelegramMessageID = chunks[0].MessageID
	}
	old, _ := h.store.GetObjectChunks(ctx, dstBucket, dstKey)
	if err := h.store.PutObject(ctx, obj, toMetaChunks(chunks)); err != nil {
		h.deleteChunks(ctx, chunks)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	for _, c := range old {
		if derr := h.backend.Delete(ctx, c.MessageID); derr != nil && h.logger != nil {
			h.logger.Warn("reap superseded object chunk on copy", "message_id", c.MessageID, "error", derr)
		}
	}

	h.writeXML(w, http.StatusOK, copyObjectResult{
		XMLNS:        s3XMLNS,
		LastModified: time.Now().UTC().Format(awsListTimeFormat),
		ETag:         quoteETag(etag),
	})
}

// uploadPartCopy implements UploadPartCopy: a multipart part whose bytes are a
// (possibly ranged) copy of an existing object. Unlike a normal UploadPart the
// ETag is returned in an XML body, not the ETag header.
func (h *Handler) uploadPartCopy(ctx context.Context, w http.ResponseWriter, r *http.Request, u metadata.MultipartUpload, partNumber int) {
	srcBucket, srcKey, ok := parseCopySource(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "InvalidArgument", "Invalid x-amz-copy-source.")
		return
	}
	src, srcChunks, ok := h.loadSource(ctx, w, srcBucket, srcKey)
	if !ok {
		return
	}

	var rng *httpRange
	if cr := r.Header.Get("X-Amz-Copy-Source-Range"); cr != "" {
		resolved, satisfiable, isRange := parseByteRange(cr, src.Size)
		if !isRange || !satisfiable {
			h.writeError(w, http.StatusBadRequest, "InvalidRange", "The x-amz-copy-source-range is not satisfiable.")
			return
		}
		rng = &resolved
	}

	reader, err := h.openObject(ctx, src, srcChunks, rng)
	if err != nil {
		h.writeError(w, http.StatusBadGateway, "TelegramDownloadFailed", err.Error())
		return
	}
	defer reader.Close()

	hasher := md5.New()
	counter := &countingReader{r: io.TeeReader(reader, hasher)}
	chunks, err := h.backend.Upload(ctx, u.Key, u.ContentType, counter)
	if err != nil {
		h.writeError(w, http.StatusBadGateway, "TelegramUploadFailed", err.Error())
		return
	}
	partETag := hex.EncodeToString(hasher.Sum(nil))

	oldChunks, _ := h.store.GetMultipartPartChunks(ctx, u.UploadID, partNumber)
	if err := h.store.PutMultipartPart(ctx, u.UploadID, partNumber, partETag, counter.n, toMetaChunks(chunks)); err != nil {
		h.deleteChunks(ctx, chunks)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	for _, c := range oldChunks {
		if derr := h.backend.Delete(ctx, c.MessageID); derr != nil && h.logger != nil {
			h.logger.Warn("reap superseded part chunk on copy", "message_id", c.MessageID, "error", derr)
		}
	}

	h.writeXML(w, http.StatusOK, copyPartResult{
		XMLNS:        s3XMLNS,
		ETag:         quoteETag(partETag),
		LastModified: time.Now().UTC().Format(awsListTimeFormat),
	})
}

// loadSource fetches the copy source object + its chunk map, writing the S3
// error (404 NoSuchKey / 500) and returning ok=false on failure.
func (h *Handler) loadSource(ctx context.Context, w http.ResponseWriter, srcBucket, srcKey string) (metadata.Object, []metadata.Chunk, bool) {
	src, err := h.store.GetObject(ctx, srcBucket, srcKey)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
			return metadata.Object{}, nil, false
		}
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return metadata.Object{}, nil, false
	}
	srcChunks, err := h.store.GetObjectChunks(ctx, srcBucket, srcKey)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return metadata.Object{}, nil, false
	}
	return src, srcChunks, true
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	XMLNS        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	XMLNS        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}
