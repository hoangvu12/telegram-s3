package s3api

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"telegram-s3/internal/metadata"
)

// minPartSize is the AWS S3 minimum size for any multipart part **except the
// last** (the last part can be any size). CompleteMultipartUpload rejects
// undersized non-last parts with EntityTooSmall (8.2 closes §6.3).
const minPartSize = 5 * 1024 * 1024

func newUploadID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// newRequestID is the value for the x-amz-request-id response header and the
// <RequestId> error element (same random-hex style as newUploadID).
func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (h *Handler) createMultipartUpload(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
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
	uploadID := newUploadID()
	if err := h.store.CreateMultipartUpload(ctx, uploadID, bucket, key, contentType); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	// Carry the create-time metadata headers (P7.5) — Complete does not resend
	// them, so they are stashed against the upload and folded onto the object
	// in FinalizeMultipartUpload.
	if md := captureObjectMetadata(r.Header); md != nil {
		if err := h.store.PutMultipartUploadMetadata(ctx, uploadID, md); err != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}
	h.writeXML(w, http.StatusOK, initiateMultipartUploadResult{Bucket: bucket, Key: key, UploadID: uploadID})
}

// resolveUpload loads the upload and asserts it belongs to bucket/key. On any
// problem it writes the S3 error and returns ok=false.
func (h *Handler) resolveUpload(ctx context.Context, w http.ResponseWriter, bucket, key, uploadID string) (metadata.MultipartUpload, bool) {
	u, err := h.store.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.")
			return metadata.MultipartUpload{}, false
		}
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return metadata.MultipartUpload{}, false
	}
	if u.Bucket != bucket || u.Key != key {
		h.writeError(w, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.")
		return metadata.MultipartUpload{}, false
	}
	return u, true
}

func (h *Handler) uploadPart(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, uploadID, partNumberStr string) {
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		h.writeError(w, http.StatusBadRequest, "InvalidArgument", "Part number must be an integer between 1 and 10000.")
		return
	}
	u, ok := h.resolveUpload(ctx, w, bucket, key, uploadID)
	if !ok {
		return
	}

	// UploadPartCopy (P7.4): the part bytes come from an existing object
	// (optionally a sub-range) instead of the request body.
	if r.Header.Get("X-Amz-Copy-Source") != "" {
		h.uploadPartCopy(ctx, w, r, u, partNumber)
		return
	}

	body, hasher := decodeUpload(r)
	chunks, err := h.backend.Upload(ctx, key, u.ContentType, body)
	if err != nil {
		// Same 400 mapping as putObject for malformed aws-chunked (8.3).
		if errors.Is(err, ErrMalformedChunked) {
			h.deleteChunks(ctx, chunks)
			h.writeError(w, http.StatusBadRequest, "IncompleteBody", "The request body does not match the declared chunk framing.")
			return
		}
		h.writeError(w, http.StatusBadGateway, "TelegramUploadFailed", err.Error())
		return
	}
	size, ok := h.validateDecodedSize(w, r, body.n)
	if !ok {
		h.deleteChunks(ctx, chunks)
		return
	}
	partETag := hex.EncodeToString(hasher.Sum(nil))

	// Re-uploading an existing part replaces it; reap the superseded
	// Telegram messages once the new part is durably recorded.
	old, _ := h.store.GetMultipartPartChunks(ctx, uploadID, partNumber)
	if err := h.store.PutMultipartPart(ctx, uploadID, partNumber, partETag, size, toMetaChunks(chunks)); err != nil {
		h.deleteChunks(ctx, chunks)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if refs := chunkRefs(old); len(refs) > 0 {
		if derr := h.backend.DeleteBatch(ctx, refs); derr != nil && h.logger != nil {
			h.logger.Warn("reap superseded part chunks failed", "count", len(refs), "error", derr)
		}
	}

	w.Header().Set("ETag", quoteETag(partETag))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) completeMultipartUpload(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	u, ok := h.resolveUpload(ctx, w, bucket, key, uploadID)
	if !ok {
		return
	}

	var req completeMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Parts) == 0 {
		h.writeError(w, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate.")
		return
	}

	stored, err := h.store.ListMultipartParts(ctx, uploadID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	storedByNum := make(map[int]metadata.MultipartPart, len(stored))
	for _, p := range stored {
		storedByNum[p.PartNumber] = p
	}

	var (
		finalChunks []metadata.Chunk
		md5Concat   []byte
		totalSize   int64
		seq         int
		prevPart    int
	)
	for i, cp := range req.Parts {
		if cp.PartNumber <= prevPart { // strictly ascending, no dups
			h.writeError(w, http.StatusBadRequest, "InvalidPartOrder", "The list of parts was not in ascending order.")
			return
		}
		prevPart = cp.PartNumber
		sp, present := storedByNum[cp.PartNumber]
		if !present || !strings.EqualFold(strings.Trim(cp.ETag, `"`), sp.ETag) {
			h.writeError(w, http.StatusBadRequest, "InvalidPart", "One or more of the specified parts could not be found or the ETag did not match.")
			return
		}
		// AWS rule: only the LAST part may be smaller than 5 MiB.
		if i != len(req.Parts)-1 && sp.Size < minPartSize {
			h.writeError(w, http.StatusBadRequest, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed size.")
			return
		}
		raw, derr := hex.DecodeString(sp.ETag)
		if derr != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", "corrupt part etag")
			return
		}
		md5Concat = append(md5Concat, raw...)

		partChunks, cerr := h.store.GetMultipartPartChunks(ctx, uploadID, cp.PartNumber)
		if cerr != nil {
			h.writeError(w, http.StatusInternalServerError, "InternalError", cerr.Error())
			return
		}
		for _, c := range partChunks {
			finalChunks = append(finalChunks, metadata.Chunk{
				Seq: seq, FileID: c.FileID, MessageID: c.MessageID, Size: c.Size, Offset: totalSize,
				Transport: c.Transport, BotIndex: c.BotIndex,
			})
			seq++
			totalSize += c.Size
		}
	}

	sum := md5.Sum(md5Concat)
	objectETag := fmt.Sprintf("%s-%d", hex.EncodeToString(sum[:]), len(req.Parts))

	obj := metadata.Object{Bucket: bucket, Key: key, Size: totalSize, ETag: objectETag, ContentType: u.ContentType}
	obj.Metadata, _ = h.store.GetMultipartUploadMetadata(ctx, uploadID)
	if len(finalChunks) > 0 {
		obj.TelegramFileID = finalChunks[0].FileID
		obj.TelegramMessageID = finalChunks[0].MessageID
	}

	// Same overwrite-reap pattern as putObject (8.5): capture the prior
	// version BEFORE the finalize txn replaces it in object_chunks, reap
	// AFTER the txn commits.
	oldChunks, _ := h.store.GetObjectChunks(ctx, bucket, key)
	prev, _ := h.store.GetObject(ctx, bucket, key)

	if err := h.store.FinalizeMultipartUpload(ctx, obj, finalChunks, uploadID); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	h.reapSupersededChunks(ctx, oldChunks, prev)

	h.writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Location: "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     quoteETag(objectETag),
	})
}

func (h *Handler) abortMultipartUpload(ctx context.Context, w http.ResponseWriter, bucket, key, uploadID string) {
	if _, ok := h.resolveUpload(ctx, w, bucket, key, uploadID); !ok {
		return
	}
	if err := h.abortUploadInternal(ctx, uploadID); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// abortUploadInternal reaps the Telegram messages tied to a multipart upload
// and removes its bookkeeping. Shared by the HTTP abort handler and the P8.6
// janitor (which has no bucket/key context, only the upload_id).
func (h *Handler) abortUploadInternal(ctx context.Context, uploadID string) error {
	all, err := h.store.AllMultipartChunks(ctx, uploadID)
	if err != nil {
		return err
	}
	if refs := chunkRefs(all); len(refs) > 0 {
		if derr := h.backend.DeleteBatch(ctx, refs); derr != nil && h.logger != nil {
			h.logger.Warn("abort: delete part chunks failed", "count", len(refs), "error", derr)
		}
	}
	return h.store.DeleteMultipartUpload(ctx, uploadID)
}

func (h *Handler) listParts(ctx context.Context, w http.ResponseWriter, bucket, key, uploadID string) {
	if _, ok := h.resolveUpload(ctx, w, bucket, key, uploadID); !ok {
		return
	}
	parts, err := h.store.ListMultipartParts(ctx, uploadID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	result := listPartsResult{Bucket: bucket, Key: key, UploadID: uploadID, StorageClass: "STANDARD"}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, p := range parts {
		result.Parts = append(result.Parts, listPart{
			PartNumber: p.PartNumber, ETag: quoteETag(p.ETag), Size: p.Size, LastModified: now,
		})
	}
	h.writeXML(w, http.StatusOK, result)
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUpload struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []completePart `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listPartsResult struct {
	XMLName      xml.Name   `xml:"ListPartsResult"`
	Bucket       string     `xml:"Bucket"`
	Key          string     `xml:"Key"`
	UploadID     string     `xml:"UploadId"`
	StorageClass string     `xml:"StorageClass"`
	Parts        []listPart `xml:"Part"`
}

type listPart struct {
	PartNumber   int    `xml:"PartNumber"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}
