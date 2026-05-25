package metadata

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// seedPrePhase3DB creates a SQLite file containing the pre-Phase-3 schema
// (no transport / bot_index columns) and a single legacy single-message
// row in objects with no matching object_chunks entry. Mirrors what an
// in-production database looked like before this migration ships.
func seedPrePhase3DB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	defer db.Close()
	// Old schema: no transport/bot_index columns anywhere.
	if _, err := db.Exec(`
CREATE TABLE buckets (name TEXT PRIMARY KEY, created_at TEXT NOT NULL);
CREATE TABLE objects (
  bucket TEXT NOT NULL, key TEXT NOT NULL, size INTEGER NOT NULL, etag TEXT NOT NULL,
  content_type TEXT NOT NULL, telegram_file_id TEXT NOT NULL, telegram_message_id INTEGER NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT,
  PRIMARY KEY (bucket, key)
);
CREATE TABLE object_chunks (
  bucket TEXT NOT NULL, key TEXT NOT NULL, part_seq INTEGER NOT NULL,
  telegram_file_id TEXT NOT NULL, telegram_message_id INTEGER NOT NULL,
  size INTEGER NOT NULL, offset INTEGER NOT NULL,
  PRIMARY KEY (bucket, key, part_seq)
);
CREATE TABLE multipart_uploads (upload_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, key TEXT NOT NULL, content_type TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE multipart_parts (upload_id TEXT NOT NULL, part_number INTEGER NOT NULL, etag TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY (upload_id, part_number));
CREATE TABLE multipart_part_chunks (upload_id TEXT NOT NULL, part_number INTEGER NOT NULL, seq INTEGER NOT NULL, telegram_file_id TEXT NOT NULL, telegram_message_id INTEGER NOT NULL, size INTEGER NOT NULL, PRIMARY KEY (upload_id, part_number, seq));
CREATE TABLE object_metadata (bucket TEXT NOT NULL, key TEXT NOT NULL, name TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (bucket, key, name));
CREATE TABLE multipart_upload_metadata (upload_id TEXT NOT NULL, name TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (upload_id, name));
`); err != nil {
		t.Fatalf("create pre-Phase-3 schema: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO buckets(name, created_at) VALUES('send', ?)`, ts); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	// Three rows exercise the backfill's NOT EXISTS clause:
	//   * legacy.bin: size>0, no chunks -> must be backfilled.
	//   * empty.bin:  size=0          -> must NOT be backfilled (empty).
	//   * chunked.bin: size>0, already has a chunk -> must NOT be touched.
	if _, err := db.Exec(`INSERT INTO objects(bucket,key,size,etag,content_type,telegram_file_id,telegram_message_id,created_at,updated_at,deleted_at) VALUES
('send','legacy.bin',  42,'le','application/octet-stream','legacyfid', 777, ?, ?, NULL),
('send','empty.bin',    0,'ee','application/octet-stream','',           0,   ?, ?, NULL),
('send','chunked.bin', 10,'cc','application/octet-stream','c0',        100, ?, ?, NULL)
`, ts, ts, ts, ts, ts, ts); err != nil {
		t.Fatalf("seed objects: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO object_chunks(bucket,key,part_seq,telegram_file_id,telegram_message_id,size,offset) VALUES('send','chunked.bin',0,'c0',100,10,0)`); err != nil {
		t.Fatalf("seed chunked: %v", err)
	}
}

// TestMigrationAddsColumnsAndBackfillsLegacy exercises the Phase 3 migration
// end-to-end against a hand-crafted pre-Phase-3 SQLite file. After Open,
// the new columns must exist and the legacy single-message row must have
// gained a one-row object_chunks entry with transport='bot', bot_index=0.
// A second Open must be a no-op (no duplicate chunk inserted).
func TestMigrationAddsColumnsAndBackfillsLegacy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pre.db")
	seedPrePhase3DB(t, dbPath)

	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open (first migrate): %v", err)
	}

	// New columns exist on each affected table.
	for _, table := range []string{"object_chunks", "multipart_part_chunks", "objects"} {
		assertColumn(t, s, table, "transport")
		assertColumn(t, s, table, "bot_index")
	}

	// Legacy row backfilled: one chunk, seq=0, transport='bot', bot_index=0.
	chunks, err := s.GetObjectChunks(context.Background(), "send", "legacy.bin")
	if err != nil {
		t.Fatalf("get backfilled chunks: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("backfill produced %d chunks, want 1: %+v", len(chunks), chunks)
	}
	got := chunks[0]
	want := Chunk{Seq: 0, FileID: "legacyfid", MessageID: 777, Size: 42, Offset: 0, Transport: "bot", BotIndex: 0}
	if got != want {
		t.Fatalf("backfilled chunk = %+v, want %+v", got, want)
	}

	// Empty object stays chunkless — it never had a Telegram message.
	if c, _ := s.GetObjectChunks(context.Background(), "send", "empty.bin"); len(c) != 0 {
		t.Fatalf("empty object got backfilled (size=0): %+v", c)
	}

	// Pre-existing chunked row is untouched (still one chunk, default-filled
	// columns are now 'bot' / 0).
	c, _ := s.GetObjectChunks(context.Background(), "send", "chunked.bin")
	if len(c) != 1 || c[0].FileID != "c0" || c[0].Transport != "bot" || c[0].BotIndex != 0 {
		t.Fatalf("chunked row mangled: %+v", c)
	}
	s.Close()

	// Second Open is a no-op: ensureColumn skips existing columns, and the
	// backfill's NOT EXISTS clause skips the (now-chunked) legacy row.
	s2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open (second migrate): %v", err)
	}
	defer s2.Close()
	c2, _ := s2.GetObjectChunks(context.Background(), "send", "legacy.bin")
	if len(c2) != 1 {
		t.Fatalf("second migrate duplicated chunks: %+v", c2)
	}
}

// assertColumn fails the test if `col` is missing from `table`. We probe
// the read-side pool through pragma_table_info; modernc.org/sqlite's
// pragma function returns the same view as the writer.
func assertColumn(t *testing.T, s *Store, table, col string) {
	t.Helper()
	rows, err := s.read.Query(`SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		t.Fatalf("pragma_table_info(%s): %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if name == col {
			return
		}
	}
	t.Fatalf("column %s.%s missing after migrate", table, col)
}
