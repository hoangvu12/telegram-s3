package s3api

import (
	"context"
	"encoding/xml"
	"net/http"
)

// deleteObjects implements the bulk DeleteObjects operation
// (POST /{bucket}?delete) used by `aws s3 rm --recursive`, rclone `purge`,
// and restic `prune`. Each key is deleted via the same idempotent core as the
// single-object DELETE, so a missing key reports as Deleted (not Error). The
// response is always 200 with a per-key result list.
func (h *Handler) deleteObjects(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	var req deleteObjectsRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Objects) == 0 {
		h.writeError(w, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate.")
		return
	}
	if len(req.Objects) > 1000 {
		h.writeError(w, http.StatusBadRequest, "MalformedXML", "The request contains more than the maximum of 1000 keys.")
		return
	}

	res := deleteResult{XMLNS: s3XMLNS}
	for _, o := range req.Objects {
		if err := h.deleteOneObject(ctx, bucket, o.Key); err != nil {
			res.Errors = append(res.Errors, deleteErrorEntry{
				Key: o.Key, Code: "InternalError", Message: err.Error(),
			})
			continue
		}
		if !req.Quiet {
			res.Deleted = append(res.Deleted, deletedEntry{Key: o.Key})
		}
	}
	h.writeXML(w, http.StatusOK, res)
}

type deleteObjectsRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name           `xml:"DeleteResult"`
	XMLNS   string             `xml:"xmlns,attr"`
	Deleted []deletedEntry     `xml:"Deleted"`
	Errors  []deleteErrorEntry `xml:"Error"`
}

type deletedEntry struct {
	Key string `xml:"Key"`
}

type deleteErrorEntry struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}
