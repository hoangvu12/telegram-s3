package metadata

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	db *sql.DB
}

type Bucket struct {
	Name      string
	CreatedAt time.Time
}

type Object struct {
	Bucket            string
	Key               string
	Size              int64
	ETag              string
	ContentType       string
	TelegramFileID    string // legacy single-message objects only; chunked objects use object_chunks
	TelegramMessageID int64  // (kept for backward compat with pre-Phase-3 rows)
	CreatedAt         time.Time
	UpdatedAt         time.Time
	// Metadata is the optional side-table content (content-disposition,
	// content-encoding, cache-control, expires, x-amz-meta-*) — write-only
	// input to PutObject/FinalizeMultipartUpload. Reads use GetObjectMetadata
	// (a separate query; GetObject does not populate this).
	Metadata map[string]string
}

// Chunk mirrors storage.Chunk for persistence (the layers stay decoupled; the
// handler converts between them, as it already does for Object).
type Chunk struct {
	Seq       int
	FileID    string
	MessageID int64
	Size      int64
	Offset    int64
}

func Open(path string) (*Store, error) {
	// WAL + a busy timeout make the single-file DB resilient to the extra
	// write pressure from chunk maps / multipart (S3-COMPAT-PLAN.md §6.4).
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	store := &Store{db: db}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	// Additive only: the existing buckets/objects schema is untouched so live
	// production rows (legacy single-message objects) keep working.
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS buckets (
  name TEXT PRIMARY KEY,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  size INTEGER NOT NULL,
  etag TEXT NOT NULL,
  content_type TEXT NOT NULL,
  telegram_file_id TEXT NOT NULL,
  telegram_message_id INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deleted_at TEXT,
  PRIMARY KEY (bucket, key),
  FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects(bucket, key);

CREATE TABLE IF NOT EXISTS object_chunks (
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  part_seq INTEGER NOT NULL,
  telegram_file_id TEXT NOT NULL,
  telegram_message_id INTEGER NOT NULL,
  size INTEGER NOT NULL,
  offset INTEGER NOT NULL,
  PRIMARY KEY (bucket, key, part_seq)
);

CREATE INDEX IF NOT EXISTS idx_object_chunks_key ON object_chunks(bucket, key, part_seq);

CREATE TABLE IF NOT EXISTS multipart_uploads (
  upload_id TEXT PRIMARY KEY,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  content_type TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS multipart_parts (
  upload_id TEXT NOT NULL,
  part_number INTEGER NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  PRIMARY KEY (upload_id, part_number)
);

CREATE TABLE IF NOT EXISTS multipart_part_chunks (
  upload_id TEXT NOT NULL,
  part_number INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  telegram_file_id TEXT NOT NULL,
  telegram_message_id INTEGER NOT NULL,
  size INTEGER NOT NULL,
  PRIMARY KEY (upload_id, part_number, seq)
);

CREATE TABLE IF NOT EXISTS object_metadata (
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (bucket, key, name)
);

CREATE TABLE IF NOT EXISTS multipart_upload_metadata (
  upload_id TEXT NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (upload_id, name)
);
`)
	return err
}

func (s *Store) CreateBucket(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO buckets(name, created_at) VALUES(?, ?)`, name, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) BucketExists(ctx context.Context, name string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, name).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, created_at FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buckets []Bucket
	for rows.Next() {
		var b Bucket
		var created string
		if err := rows.Scan(&b.Name, &created); err != nil {
			return nil, err
		}
		b.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		buckets = append(buckets, b)
	}
	return buckets, rows.Err()
}

// PutObject writes the object row and replaces its chunk map atomically. An
// overwrite drops the prior chunk rows; the caller is responsible for deleting
// the superseded Telegram messages (it holds the backend).
func (s *Store) PutObject(ctx context.Context, obj Object, chunks []Chunk) error {
	now := time.Now().UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
INSERT INTO objects(bucket, key, size, etag, content_type, telegram_file_id, telegram_message_id, created_at, updated_at, deleted_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
ON CONFLICT(bucket, key) DO UPDATE SET
  size = excluded.size,
  etag = excluded.etag,
  content_type = excluded.content_type,
  telegram_file_id = excluded.telegram_file_id,
  telegram_message_id = excluded.telegram_message_id,
  updated_at = excluded.updated_at,
  deleted_at = NULL
`, obj.Bucket, obj.Key, obj.Size, obj.ETag, obj.ContentType, obj.TelegramFileID, obj.TelegramMessageID, obj.CreatedAt.Format(time.RFC3339Nano), obj.UpdatedAt.Format(time.RFC3339Nano)); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key = ?`, obj.Bucket, obj.Key); err != nil {
		return err
	}
	for _, c := range chunks {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO object_chunks(bucket, key, part_seq, telegram_file_id, telegram_message_id, size, offset)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, obj.Bucket, obj.Key, c.Seq, c.FileID, c.MessageID, c.Size, c.Offset); err != nil {
			return err
		}
	}
	if err := replaceObjectMetadataTx(ctx, tx, obj.Bucket, obj.Key, obj.Metadata); err != nil {
		return err
	}
	return tx.Commit()
}

// replaceObjectMetadata side-table mirrors the object_chunks replace pattern:
// the rows are wiped and re-inserted inside the object's own transaction, so a
// legacy object simply has no rows (→ defaults) and an overwrite never leaves
// stale metadata.
func replaceObjectMetadataTx(ctx context.Context, tx *sql.Tx, bucket, key string, kv map[string]string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_metadata WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	for name, value := range kv {
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_metadata(bucket, key, name, value) VALUES(?, ?, ?, ?)`, bucket, key, name, value); err != nil {
			return err
		}
	}
	return nil
}

// GetObjectMetadata returns the object's side-table metadata (empty for legacy
// rows / objects stored without any).
func (s *Store) GetObjectMetadata(ctx context.Context, bucket, key string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, value FROM object_metadata WHERE bucket = ? AND key = ?`, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	md := map[string]string{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		md[name] = value
	}
	return md, rows.Err()
}

func (s *Store) GetObject(ctx context.Context, bucket, key string) (Object, error) {
	var obj Object
	var created, updated string
	err := s.db.QueryRowContext(ctx, `
SELECT bucket, key, size, etag, content_type, telegram_file_id, telegram_message_id, created_at, updated_at
FROM objects
WHERE bucket = ? AND key = ? AND deleted_at IS NULL
`, bucket, key).Scan(&obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &obj.TelegramFileID, &obj.TelegramMessageID, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, err
	}
	obj.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	obj.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return obj, nil
}

// GetObjectChunks returns the ordered chunk map, or an empty slice for legacy
// single-message objects (which predate Phase 3 and use Object.TelegramFileID).
func (s *Store) GetObjectChunks(ctx context.Context, bucket, key string) ([]Chunk, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT part_seq, telegram_file_id, telegram_message_id, size, offset
FROM object_chunks
WHERE bucket = ? AND key = ?
ORDER BY part_seq
`, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.Seq, &c.FileID, &c.MessageID, &c.Size, &c.Offset); err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}

// DeleteObject hard-deletes the object and its chunk map. Telegram message
// removal is the caller's responsibility (done before this, while the chunk
// list is still readable).
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_metadata WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	return tx.Commit()
}

// ListParams drives a single ListObjects / ListObjectsV2 page.
type ListParams struct {
	Bucket    string
	Prefix    string
	Delimiter string
	After     string // exclusive lower bound (marker / start-after / decoded token)
	MaxKeys   int
}

// ListPage is one page of a (optionally delimiter-rolled) listing.
type ListPage struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
	// NextAfter is the underlying key to resume strictly-after; meaningful only
	// when IsTruncated. The handler turns it into NextMarker /
	// NextContinuationToken. It is the last key actually *consumed* (whether it
	// became a Content row or was rolled into a CommonPrefix), so the next page
	// reproduces an as-yet-unreturned prefix without duplicating data.
	NextAfter string
}

// ListObjectsPage scans keys in byte order and applies S3 delimiter rollup +
// real pagination. Prefix is a half-open range scan (not LIKE — keys may
// contain % / _), and rows are streamed lazily so a huge bucket stops as soon
// as the page is full.
func (s *Store) ListObjectsPage(ctx context.Context, p ListParams) (ListPage, error) {
	maxKeys := p.MaxKeys
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	query := `SELECT bucket, key, size, etag, content_type, telegram_file_id, telegram_message_id, created_at, updated_at
FROM objects WHERE bucket = ? AND deleted_at IS NULL`
	args := []any{p.Bucket}
	if p.After != "" {
		query += ` AND key > ?`
		args = append(args, p.After)
	}
	if p.Prefix != "" {
		query += ` AND key >= ?`
		args = append(args, p.Prefix)
		if hi := prefixUpperBound(p.Prefix); hi != "" {
			query += ` AND key < ?`
			args = append(args, hi)
		}
	}
	query += ` ORDER BY key`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ListPage{}, err
	}
	defer rows.Close()

	var page ListPage
	seen := map[string]bool{}
	count := 0
	for rows.Next() {
		var obj Object
		var created, updated string
		if err := rows.Scan(&obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &obj.TelegramFileID, &obj.TelegramMessageID, &created, &updated); err != nil {
			return ListPage{}, err
		}
		obj.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		obj.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)

		if p.Delimiter != "" {
			rel := obj.Key[len(p.Prefix):] // SQL guarantees the key starts with prefix
			if idx := strings.Index(rel, p.Delimiter); idx >= 0 {
				cp := p.Prefix + rel[:idx+len(p.Delimiter)]
				if seen[cp] {
					page.NextAfter = obj.Key // consumed, but no new result
					continue
				}
				if count == maxKeys { // a new result beyond the page → more remains
					page.IsTruncated = true
					return page, rows.Err()
				}
				seen[cp] = true
				page.CommonPrefixes = append(page.CommonPrefixes, cp)
				count++
				page.NextAfter = obj.Key
				continue
			}
		}
		if count == maxKeys {
			page.IsTruncated = true
			return page, rows.Err()
		}
		page.Objects = append(page.Objects, obj)
		count++
		page.NextAfter = obj.Key
	}
	if err := rows.Err(); err != nil {
		return ListPage{}, err
	}
	page.NextAfter = "" // exhausted the stream → no resume point
	return page, nil
}

// prefixUpperBound returns the smallest string strictly greater than every
// string starting with prefix (for a half-open [prefix, hi) byte-range scan).
// "" means unbounded above (empty prefix, or an all-0xFF prefix). SQLite's
// default TEXT collation is BINARY, so byte order == S3's UTF-8 key order.
func prefixUpperBound(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			out := append([]byte(nil), b[:i]...)
			return string(append(out, b[i]+1))
		}
	}
	return ""
}

// --- Multipart upload (Phase 4) --------------------------------------------

type MultipartUpload struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   time.Time
}

type MultipartPart struct {
	PartNumber int
	ETag       string // hex MD5 of the part bytes (unquoted)
	Size       int64
}

func (s *Store) CreateMultipartUpload(ctx context.Context, uploadID, bucket, key, contentType string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO multipart_uploads(upload_id, bucket, key, content_type, created_at) VALUES(?, ?, ?, ?, ?)`,
		uploadID, bucket, key, contentType, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) GetMultipartUpload(ctx context.Context, uploadID string) (MultipartUpload, error) {
	var u MultipartUpload
	var created string
	err := s.db.QueryRowContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE upload_id = ?`, uploadID).
		Scan(&u.UploadID, &u.Bucket, &u.Key, &u.ContentType, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return MultipartUpload{}, ErrNotFound
	}
	if err != nil {
		return MultipartUpload{}, err
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	return u, nil
}

// ListMultipartUploads returns every in-progress upload in a bucket, ordered
// by key then upload_id (the order S3's ListMultipartUploads reports). The
// gateway does not paginate this — abandoned uploads are bounded in practice
// and there is no lifecycle janitor yet (see progress §6.2).
func (s *Store) ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartUpload, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE bucket = ? ORDER BY key, upload_id`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var uploads []MultipartUpload
	for rows.Next() {
		var u MultipartUpload
		var created string
		if err := rows.Scan(&u.UploadID, &u.Bucket, &u.Key, &u.ContentType, &created); err != nil {
			return nil, err
		}
		u.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		uploads = append(uploads, u)
	}
	return uploads, rows.Err()
}

// SetMultipartCreatedAt overrides a multipart upload's created_at timestamp.
// Production never edits created_at after CreateMultipartUpload; this exists
// only so the P8.6 janitor tests can stage a "stale" upload without sleeping
// for the real TTL.
func (s *Store) SetMultipartCreatedAt(ctx context.Context, uploadID string, t time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE multipart_uploads SET created_at = ? WHERE upload_id = ?`, t.UTC().Format(time.RFC3339Nano), uploadID)
	return err
}

// StaleMultipartUploads returns every multipart upload whose created_at is
// strictly before the cutoff. Used by the P8.6 janitor; uploads within the
// TTL window are left alone (they may be live in-progress writes).
func (s *Store) StaleMultipartUploads(ctx context.Context, before time.Time) ([]MultipartUpload, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE created_at < ? ORDER BY created_at`, before.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var uploads []MultipartUpload
	for rows.Next() {
		var u MultipartUpload
		var created string
		if err := rows.Scan(&u.UploadID, &u.Bucket, &u.Key, &u.ContentType, &created); err != nil {
			return nil, err
		}
		u.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		uploads = append(uploads, u)
	}
	return uploads, rows.Err()
}

func (s *Store) GetMultipartPartChunks(ctx context.Context, uploadID string, partNumber int) ([]Chunk, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq, telegram_file_id, telegram_message_id, size FROM multipart_part_chunks WHERE upload_id = ? AND part_number = ? ORDER BY seq`, uploadID, partNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chunks []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.Seq, &c.FileID, &c.MessageID, &c.Size); err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}

// PutMultipartPart replaces a part and its chunk list atomically (re-uploading
// the same part number is allowed by S3; the caller reaps the old chunks'
// Telegram messages, having read them first).
func (s *Store) PutMultipartPart(ctx context.Context, uploadID string, partNumber int, etag string, size int64, chunks []Chunk) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT INTO multipart_parts(upload_id, part_number, etag, size) VALUES(?, ?, ?, ?)
ON CONFLICT(upload_id, part_number) DO UPDATE SET etag = excluded.etag, size = excluded.size`, uploadID, partNumber, etag, size); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_part_chunks WHERE upload_id = ? AND part_number = ?`, uploadID, partNumber); err != nil {
		return err
	}
	for _, c := range chunks {
		if _, err := tx.ExecContext(ctx, `INSERT INTO multipart_part_chunks(upload_id, part_number, seq, telegram_file_id, telegram_message_id, size) VALUES(?, ?, ?, ?, ?, ?)`,
			uploadID, partNumber, c.Seq, c.FileID, c.MessageID, c.Size); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListMultipartParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT part_number, etag, size FROM multipart_parts WHERE upload_id = ? ORDER BY part_number`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []MultipartPart
	for rows.Next() {
		var p MultipartPart
		if err := rows.Scan(&p.PartNumber, &p.ETag, &p.Size); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

// AllMultipartChunks returns every chunk across all parts (for abort cleanup).
func (s *Store) AllMultipartChunks(ctx context.Context, uploadID string) ([]Chunk, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT telegram_file_id, telegram_message_id FROM multipart_part_chunks WHERE upload_id = ? ORDER BY part_number, seq`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chunks []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.FileID, &c.MessageID); err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}

// FinalizeMultipartUpload atomically materializes the assembled object (row +
// chunk map) and drops all multipart bookkeeping for the upload.
func (s *Store) FinalizeMultipartUpload(ctx context.Context, obj Object, chunks []Chunk, uploadID string) error {
	now := time.Now().UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
INSERT INTO objects(bucket, key, size, etag, content_type, telegram_file_id, telegram_message_id, created_at, updated_at, deleted_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
ON CONFLICT(bucket, key) DO UPDATE SET
  size = excluded.size, etag = excluded.etag, content_type = excluded.content_type,
  telegram_file_id = excluded.telegram_file_id, telegram_message_id = excluded.telegram_message_id,
  updated_at = excluded.updated_at, deleted_at = NULL
`, obj.Bucket, obj.Key, obj.Size, obj.ETag, obj.ContentType, obj.TelegramFileID, obj.TelegramMessageID, obj.CreatedAt.Format(time.RFC3339Nano), obj.UpdatedAt.Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key = ?`, obj.Bucket, obj.Key); err != nil {
		return err
	}
	for _, c := range chunks {
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_chunks(bucket, key, part_seq, telegram_file_id, telegram_message_id, size, offset) VALUES(?, ?, ?, ?, ?, ?, ?)`,
			obj.Bucket, obj.Key, c.Seq, c.FileID, c.MessageID, c.Size, c.Offset); err != nil {
			return err
		}
	}
	if err := replaceObjectMetadataTx(ctx, tx, obj.Bucket, obj.Key, obj.Metadata); err != nil {
		return err
	}
	if err := deleteMultipartTx(ctx, tx, uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

// PutMultipartUploadMetadata stores the headers captured at
// CreateMultipartUpload so FinalizeMultipartUpload can carry them onto the
// completed object (the complete request does not resend them).
func (s *Store) PutMultipartUploadMetadata(ctx context.Context, uploadID string, kv map[string]string) error {
	for name, value := range kv {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO multipart_upload_metadata(upload_id, name, value) VALUES(?, ?, ?)`, uploadID, name, value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetMultipartUploadMetadata(ctx context.Context, uploadID string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, value FROM multipart_upload_metadata WHERE upload_id = ?`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	md := map[string]string{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		md[name] = value
	}
	return md, rows.Err()
}

// DeleteMultipartUpload removes all bookkeeping for an upload (abort path,
// after the caller has reaped the Telegram messages).
func (s *Store) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := deleteMultipartTx(ctx, tx, uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

func deleteMultipartTx(ctx context.Context, tx *sql.Tx, uploadID string) error {
	for _, q := range []string{
		`DELETE FROM multipart_part_chunks WHERE upload_id = ?`,
		`DELETE FROM multipart_parts WHERE upload_id = ?`,
		`DELETE FROM multipart_upload_metadata WHERE upload_id = ?`,
		`DELETE FROM multipart_uploads WHERE upload_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, uploadID); err != nil {
			return err
		}
	}
	return nil
}
