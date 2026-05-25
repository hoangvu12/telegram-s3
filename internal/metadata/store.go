package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("not found")

// Store holds two *sql.DB handles to the same WAL-mode SQLite file. modernc.org/sqlite
// guidance (and the SQLite WAL model in general): a single pool with N connections
// does NOT give parallel writes — SQLite serializes the writer regardless — and tends
// to add SQLITE_BUSY contention. The right pattern is one writer connection plus a
// pool of reader connections, since WAL allows many concurrent readers and one writer
// to coexist freely.
//
// write is the sole writer (MaxOpenConns=1, BeginTx + Exec target it).
// read is the reader pool (MaxOpenConns=N, QueryContext/QueryRowContext target it).
type Store struct {
	write *sql.DB
	read  *sql.DB
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
//
// Transport and BotIndex are persisted on the chunk row (Phase 3 schema
// additions). Pre-Phase-3 rows backfilled at migration time read as
// Transport="bot", BotIndex=0. The dispatcher treats Transport == "" as
// "bot" so the absent-column case (e.g. a fresh test DB hand-rolled
// without the column) still routes through BotStorage.
type Chunk struct {
	Seq       int
	FileID    string
	MessageID int64
	Size      int64
	Offset    int64
	Transport string
	BotIndex  int
}

// Open opens the metadata store with the default reader-pool size (8). Use
// OpenWithOptions when callers want to override via config.
func Open(path string) (*Store, error) {
	return OpenWithOptions(path, 8)
}

// OpenWithOptions opens the metadata store with an explicit reader-pool size.
// readerConns <= 0 falls back to 8. The writer pool is always 1 (SQLite's
// single-writer model). Both handles share the same WAL file and busy_timeout.
func OpenWithOptions(path string, readerConns int) (*Store, error) {
	if readerConns <= 0 {
		readerConns = 8
	}
	// WAL + a busy timeout make the single-file DB resilient to the extra
	// write pressure from chunk maps / multipart (S3-COMPAT-PLAN.md §6.4).
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"

	write, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	write.SetMaxOpenConns(1)

	read, err := sql.Open("sqlite", dsn)
	if err != nil {
		write.Close()
		return nil, err
	}
	read.SetMaxOpenConns(readerConns)
	read.SetMaxIdleConns(readerConns)

	store := &Store{write: write, read: read}
	if err := store.migrate(); err != nil {
		write.Close()
		read.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	rerr := s.read.Close()
	werr := s.write.Close()
	if werr != nil {
		return werr
	}
	return rerr
}

func (s *Store) migrate() error {
	// Additive only: the existing buckets/objects schema is untouched so live
	// production rows (legacy single-message objects) keep working.
	if _, err := s.write.Exec(`
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

-- Phase 4: gotd MTProto session blobs, one row per bot. Partial writes
-- would force the bot to re-auth (auth.importBotAuthorization) and trip
-- flood control, so the session.Storage adapter upserts inside a tx.
CREATE TABLE IF NOT EXISTS tg_sessions (
  key TEXT PRIMARY KEY,
  value BLOB NOT NULL,
  updated_at TEXT NOT NULL
);

-- Phase 4: grace-delete buffer for the migration sweeper. After
-- pass-1 atomically swaps a bot chunk to mtproto, the old (message_id,
-- bot_index) lands here; pass-2 reaps it once cfg.BotDeleteGrace has
-- passed. See PHASES.md decision #14 for why the bot delete cannot
-- be done in the same tx as the row swap.
CREATE TABLE IF NOT EXISTS bot_chunks_pending_delete (
  message_id INTEGER NOT NULL,
  bot_index INTEGER NOT NULL,
  swapped_at TEXT NOT NULL,
  PRIMARY KEY (message_id, bot_index)
);
CREATE INDEX IF NOT EXISTS idx_bot_chunks_pending_delete_swapped_at ON bot_chunks_pending_delete(swapped_at);
`); err != nil {
		return err
	}

	// Phase 3 additive columns. SQLite doesn't support `ADD COLUMN IF NOT
	// EXISTS`, so ensureColumn probes pragma_table_info first and only
	// issues the ALTER when missing. NOT NULL DEFAULT is safe with ALTER
	// TABLE since SQLite materializes the default for existing rows.
	for _, c := range []struct {
		table, col, typ, def string
	}{
		{"object_chunks", "transport", "TEXT NOT NULL", "'bot'"},
		{"object_chunks", "bot_index", "INTEGER NOT NULL", "0"},
		{"multipart_part_chunks", "transport", "TEXT NOT NULL", "'bot'"},
		{"multipart_part_chunks", "bot_index", "INTEGER NOT NULL", "0"},
		{"objects", "transport", "TEXT NOT NULL", "'bot'"},
		{"objects", "bot_index", "INTEGER NOT NULL", "0"},
	} {
		if err := s.ensureColumn(c.table, c.col, c.typ, c.def); err != nil {
			return err
		}
	}

	// One-shot backfill: legacy single-message objects (size > 0, no row in
	// object_chunks) collapse to a one-row chunk so every read path can
	// consume object_chunks uniformly — the three pre-Phase-3 fallback
	// branches in handler.go are deleted in this phase. Idempotent: the
	// EXISTS clause skips already-backfilled rows on subsequent boots.
	return s.backfillLegacyChunks()
}

// ensureColumn issues ALTER TABLE ADD COLUMN if the column is missing.
// SQLite has no `ADD COLUMN IF NOT EXISTS`, so we probe pragma_table_info
// first; the helper is the single place ALTER lives so future additive
// migrations follow the same pattern.
func (s *Store) ensureColumn(table, col, typ, defaultExpr string) error {
	rows, err := s.write.Query(`SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if name == col {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Identifiers come from the caller (this file), not user input — so the
	// fmt.Sprintf is safe. SQL injection would require attacker control of
	// `table`/`col`, which is impossible.
	stmt := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s DEFAULT %s`, table, col, typ, defaultExpr)
	_, err = s.write.Exec(stmt)
	return err
}

// backfillLegacyChunks turns every pre-Phase-3 single-message object into
// a one-row object_chunks entry so the read/delete code paths can drop
// their legacy fallbacks. Empty objects (size == 0) never had a Telegram
// message and stay chunkless. Re-running is a no-op: the NOT EXISTS clause
// filters out rows that already have chunks (the normal Phase-2+ shape).
func (s *Store) backfillLegacyChunks() error {
	tx, err := s.write.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`
SELECT bucket, key, size, telegram_file_id, telegram_message_id
FROM objects
WHERE size > 0
  AND telegram_message_id != 0
  AND deleted_at IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM object_chunks oc
    WHERE oc.bucket = objects.bucket AND oc.key = objects.key
  )
`)
	if err != nil {
		return err
	}
	type legacyRow struct {
		bucket, key, fileID string
		size, messageID     int64
	}
	var legacies []legacyRow
	for rows.Next() {
		var r legacyRow
		if err := rows.Scan(&r.bucket, &r.key, &r.size, &r.fileID, &r.messageID); err != nil {
			rows.Close()
			return err
		}
		legacies = append(legacies, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, r := range legacies {
		if _, err := tx.Exec(`
INSERT INTO object_chunks(bucket, key, part_seq, telegram_file_id, telegram_message_id, size, offset, transport, bot_index)
VALUES(?, ?, 0, ?, ?, ?, 0, 'bot', 0)
`, r.bucket, r.key, r.fileID, r.messageID, r.size); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) CreateBucket(ctx context.Context, name string) error {
	_, err := s.write.ExecContext(ctx, `INSERT INTO buckets(name, created_at) VALUES(?, ?)`, name, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	result, err := s.write.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
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
	err := s.read.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, name).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT name, created_at FROM buckets ORDER BY name`)
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

	tx, err := s.write.BeginTx(ctx, nil)
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
		transport := c.Transport
		if transport == "" {
			transport = "bot"
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO object_chunks(bucket, key, part_seq, telegram_file_id, telegram_message_id, size, offset, transport, bot_index)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
`, obj.Bucket, obj.Key, c.Seq, c.FileID, c.MessageID, c.Size, c.Offset, transport, c.BotIndex); err != nil {
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
	rows, err := s.read.QueryContext(ctx, `SELECT name, value FROM object_metadata WHERE bucket = ? AND key = ?`, bucket, key)
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
	err := s.read.QueryRowContext(ctx, `
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

// GetObjectChunks returns the ordered chunk map. Post-Phase-3 every
// non-empty object has at least one row here (legacy single-message
// objects are backfilled at migration time); empty objects return nil.
func (s *Store) GetObjectChunks(ctx context.Context, bucket, key string) ([]Chunk, error) {
	rows, err := s.read.QueryContext(ctx, `
SELECT part_seq, telegram_file_id, telegram_message_id, size, offset, transport, bot_index
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
		if err := rows.Scan(&c.Seq, &c.FileID, &c.MessageID, &c.Size, &c.Offset, &c.Transport, &c.BotIndex); err != nil {
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
	tx, err := s.write.BeginTx(ctx, nil)
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

	rows, err := s.read.QueryContext(ctx, query, args...)
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
	_, err := s.write.ExecContext(ctx, `INSERT INTO multipart_uploads(upload_id, bucket, key, content_type, created_at) VALUES(?, ?, ?, ?, ?)`,
		uploadID, bucket, key, contentType, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) GetMultipartUpload(ctx context.Context, uploadID string) (MultipartUpload, error) {
	var u MultipartUpload
	var created string
	err := s.read.QueryRowContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE upload_id = ?`, uploadID).
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
	rows, err := s.read.QueryContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE bucket = ? ORDER BY key, upload_id`, bucket)
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
	_, err := s.write.ExecContext(ctx, `UPDATE multipart_uploads SET created_at = ? WHERE upload_id = ?`, t.UTC().Format(time.RFC3339Nano), uploadID)
	return err
}

// StaleMultipartUploads returns every multipart upload whose created_at is
// strictly before the cutoff. Used by the P8.6 janitor; uploads within the
// TTL window are left alone (they may be live in-progress writes).
func (s *Store) StaleMultipartUploads(ctx context.Context, before time.Time) ([]MultipartUpload, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE created_at < ? ORDER BY created_at`, before.UTC().Format(time.RFC3339Nano))
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
	rows, err := s.read.QueryContext(ctx, `SELECT seq, telegram_file_id, telegram_message_id, size, transport, bot_index FROM multipart_part_chunks WHERE upload_id = ? AND part_number = ? ORDER BY seq`, uploadID, partNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chunks []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.Seq, &c.FileID, &c.MessageID, &c.Size, &c.Transport, &c.BotIndex); err != nil {
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
	tx, err := s.write.BeginTx(ctx, nil)
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
		transport := c.Transport
		if transport == "" {
			transport = "bot"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO multipart_part_chunks(upload_id, part_number, seq, telegram_file_id, telegram_message_id, size, transport, bot_index) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			uploadID, partNumber, c.Seq, c.FileID, c.MessageID, c.Size, transport, c.BotIndex); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListMultipartParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT part_number, etag, size FROM multipart_parts WHERE upload_id = ? ORDER BY part_number`, uploadID)
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
// Transport and BotIndex are included so the dispatcher routes each delete
// to the bot that owns the message — under Bot HTTP API a message can only
// be deleted by the bot that sent it (modulo channel-admin permissions).
func (s *Store) AllMultipartChunks(ctx context.Context, uploadID string) ([]Chunk, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT telegram_file_id, telegram_message_id, transport, bot_index FROM multipart_part_chunks WHERE upload_id = ? ORDER BY part_number, seq`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chunks []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.FileID, &c.MessageID, &c.Transport, &c.BotIndex); err != nil {
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

	tx, err := s.write.BeginTx(ctx, nil)
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
		transport := c.Transport
		if transport == "" {
			transport = "bot"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_chunks(bucket, key, part_seq, telegram_file_id, telegram_message_id, size, offset, transport, bot_index) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			obj.Bucket, obj.Key, c.Seq, c.FileID, c.MessageID, c.Size, c.Offset, transport, c.BotIndex); err != nil {
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
		if _, err := s.write.ExecContext(ctx, `INSERT INTO multipart_upload_metadata(upload_id, name, value) VALUES(?, ?, ?)`, uploadID, name, value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetMultipartUploadMetadata(ctx context.Context, uploadID string) (map[string]string, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT name, value FROM multipart_upload_metadata WHERE upload_id = ?`, uploadID)
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
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := deleteMultipartTx(ctx, tx, uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

// --- Phase 4: MTProto session storage --------------------------------------

// LoadSession returns the raw session blob for the given key, or
// ErrNotFound if no session has been stored yet. The bot's first boot
// after deploy goes through ErrNotFound, which gotd's session.Loader
// translates into a fresh auth.importBotAuthorization call.
func (s *Store) LoadSession(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	err := s.read.QueryRowContext(ctx, `SELECT value FROM tg_sessions WHERE key = ?`, key).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return data, err
}

// StoreSession upserts the session blob for the given key. The write
// goes through the single-conn writer pool so two bots saving the same
// key cannot tear (gotd's session is a self-consistent blob and a
// partial overwrite would force a re-auth + flood-control penalty).
func (s *Store) StoreSession(ctx context.Context, key string, data []byte) error {
	_, err := s.write.ExecContext(ctx, `
INSERT INTO tg_sessions(key, value, updated_at) VALUES(?, ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
`, key, data, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// --- Phase 4: migration sweeper helpers ------------------------------------

// BotChunkLoc identifies one Bot-API chunk that the sweeper considers
// for migration to MTProto. The sweeper downloads via BotStorage using
// (FileID, MessageID, BotIndex) and then re-uploads, swapping the row
// via SwapBotChunkToMtproto.
type BotChunkLoc struct {
	Bucket    string
	Key       string
	PartSeq   int
	Offset    int64
	Size      int64
	FileID    string
	MessageID int64
	BotIndex  int
}

// ListBotChunksOldestFirst returns up to `limit` object_chunks rows
// with transport='bot', sorted by the parent object's updated_at
// ascending — so the oldest data drains first. The join is cheap
// because (bucket, key) is the object_chunks primary key prefix.
func (s *Store) ListBotChunksOldestFirst(ctx context.Context, limit int) ([]BotChunkLoc, error) {
	rows, err := s.read.QueryContext(ctx, `
SELECT oc.bucket, oc.key, oc.part_seq, oc.offset, oc.size,
       oc.telegram_file_id, oc.telegram_message_id, oc.bot_index
FROM object_chunks oc
JOIN objects o ON o.bucket = oc.bucket AND o.key = oc.key
WHERE oc.transport = 'bot' AND o.deleted_at IS NULL
ORDER BY o.updated_at ASC, oc.bucket ASC, oc.key ASC, oc.part_seq ASC
LIMIT ?
`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BotChunkLoc
	for rows.Next() {
		var c BotChunkLoc
		if err := rows.Scan(&c.Bucket, &c.Key, &c.PartSeq, &c.Offset, &c.Size, &c.FileID, &c.MessageID, &c.BotIndex); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// SwapBotChunkToMtproto is the load-bearing pass-1 atomic op of the
// sweeper. In one transaction it (a) updates the chunk row to point at
// the freshly-uploaded MTProto message, and (b) records the old bot
// (message_id, bot_index) in bot_chunks_pending_delete so pass-2 can
// reap it after the grace window. The two writes MUST share a tx — a
// crash between them either leaves the bot message readable
// indefinitely (if only the insert ran) or breaks reads (if only the
// update ran). Both are correctness violations.
//
// The pre-update guard `WHERE part_seq=? AND transport='bot'` makes
// double-application a no-op: a second invocation finds the row
// already mtproto and silently affects 0 rows (the duplicate insert
// is then a unique-key conflict, which we ignore — see ON CONFLICT).
func (s *Store) SwapBotChunkToMtproto(ctx context.Context, bucket, key string, partSeq int, oldMessageID int64, oldBotIndex int, newMessageID int64, newBotIndex int, swappedAt time.Time) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
UPDATE object_chunks
   SET transport = 'mtproto',
       telegram_message_id = ?,
       bot_index = ?,
       telegram_file_id = ''
 WHERE bucket = ? AND key = ? AND part_seq = ? AND transport = 'bot'
`, newMessageID, newBotIndex, bucket, key, partSeq)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// Another pass-1 already migrated this row, or the parent object was
		// deleted underneath us. Either way pass-2 has nothing new to do; we
		// also do NOT insert into pending_delete because the old bot message
		// was already enqueued (or already reaped).
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO bot_chunks_pending_delete(message_id, bot_index, swapped_at)
VALUES(?, ?, ?)
ON CONFLICT(message_id, bot_index) DO UPDATE SET swapped_at = excluded.swapped_at
`, oldMessageID, oldBotIndex, swappedAt.UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return tx.Commit()
}

// PendingDelete is one row from bot_chunks_pending_delete — the
// sweeper's pass-2 reaps these after swapped_at + cfg.BotDeleteGrace.
type PendingDelete struct {
	MessageID int64
	BotIndex  int
	SwappedAt time.Time
}

// PendingDeletesOlderThan returns up to `limit` pending-delete rows
// whose swapped_at is strictly before `before`. Ordered oldest-first
// so a tight rate cap drains the worst backlog first.
func (s *Store) PendingDeletesOlderThan(ctx context.Context, before time.Time, limit int) ([]PendingDelete, error) {
	rows, err := s.read.QueryContext(ctx, `
SELECT message_id, bot_index, swapped_at
FROM bot_chunks_pending_delete
WHERE swapped_at < ?
ORDER BY swapped_at ASC
LIMIT ?
`, before.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingDelete
	for rows.Next() {
		var p PendingDelete
		var swapped string
		if err := rows.Scan(&p.MessageID, &p.BotIndex, &swapped); err != nil {
			return nil, err
		}
		p.SwappedAt, _ = time.Parse(time.RFC3339Nano, swapped)
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeletePendingDelete removes one pending-delete row after the
// sweeper's pass-2 has successfully deleted the bot message. A
// failure here is harmless — the row stays and the next pass-2 tries
// the bot delete again (Telegram's deleteMessage is idempotent on a
// message that's already gone).
func (s *Store) DeletePendingDelete(ctx context.Context, messageID int64, botIndex int) error {
	_, err := s.write.ExecContext(ctx, `
DELETE FROM bot_chunks_pending_delete WHERE message_id = ? AND bot_index = ?
`, messageID, botIndex)
	return err
}

// CountBotChunks returns the number of object_chunks rows still on
// the Bot HTTP API transport. Exposed for ops/observability: when this
// hits zero the sweeper has drained, and the operator can flip
// TELEGRAM_TRANSPORT to "mtproto" and eventually delete the legacy bot.
func (s *Store) CountBotChunks(ctx context.Context) (int64, error) {
	var n int64
	err := s.read.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_chunks WHERE transport = 'bot'`).Scan(&n)
	return n, err
}

// BotMigrationSnapshot is a point-in-time picture of the Phase 4
// sweeper's remaining work. The sweeper logs this at startup and once
// per tick so operators can read drain progress directly from the
// service logs — important because the production deploy's SQLite DB
// lives inside an Easypanel container that the easypanel-skill tRPC
// API can't reach (terminal is a separate WebSocket).
type BotMigrationSnapshot struct {
	// BotChunksRemaining is the count of object_chunks rows still
	// addressed via the Bot HTTP API. Drops by ~MigrationRate/day.
	// Item-5-ready when this hits 0.
	BotChunksRemaining int64
	// PendingDeletes is the size of the bot_chunks_pending_delete
	// queue — pass-1 enqueues here, pass-2 reaps after the grace
	// window. Hovers near MigrationRate × grace / 24h in steady state.
	PendingDeletes int64
	// LatestSwap is the newest swapped_at across pending_delete rows.
	// The IsZero check on the value tells the caller whether the
	// table was empty (no swaps yet) vs. had data — we don't surface
	// sql.NullTime to keep the struct boring to log.
	LatestSwap time.Time
}

// BotMigrationSnapshot reads the three drain-progress counts in a
// single read-pool round trip (one COUNT on object_chunks, one
// combined COUNT + MAX on bot_chunks_pending_delete). Safe to call on
// every sweeper tick — at the default MigrationRate=100/day this
// fires ~96 times/day, well under any meaningful SQLite contention.
func (s *Store) BotMigrationSnapshot(ctx context.Context) (BotMigrationSnapshot, error) {
	var snap BotMigrationSnapshot
	if err := s.read.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM object_chunks WHERE transport = 'bot'`,
	).Scan(&snap.BotChunksRemaining); err != nil {
		return snap, fmt.Errorf("count bot chunks: %w", err)
	}
	// MAX(swapped_at) is NULL on an empty table; sql.NullString catches
	// that. The column is stored as an RFC3339Nano string (see
	// SwapBotChunkToMtproto), so we Scan into a string and parse — the
	// driver doesn't auto-convert text to time.Time. Same pattern as
	// PendingDeletesOlderThan. Combining COUNT + MAX in one row keeps
	// the snapshot to two read-pool round trips total.
	var latest sql.NullString
	if err := s.read.QueryRowContext(ctx,
		`SELECT COUNT(*), MAX(swapped_at) FROM bot_chunks_pending_delete`,
	).Scan(&snap.PendingDeletes, &latest); err != nil {
		return snap, fmt.Errorf("count pending deletes: %w", err)
	}
	if latest.Valid {
		// Parse failures leave LatestSwap zero — that's the same fallback
		// the logger uses for "no rows yet", so an unparseable timestamp
		// degrades to "we have rows but no readable max" rather than a
		// confusing year-0 line in the log. Production rows are always
		// written via SwapBotChunkToMtproto's RFC3339Nano formatter, so
		// the parse should not fail.
		snap.LatestSwap, _ = time.Parse(time.RFC3339Nano, latest.String)
	}
	return snap, nil
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
