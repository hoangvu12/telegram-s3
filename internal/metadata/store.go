package metadata

import (
	"context"
	"database/sql"
	"errors"
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
	TelegramFileID    string
	TelegramMessageID int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
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

func (s *Store) PutObject(ctx context.Context, obj Object) error {
	now := time.Now().UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now

	_, err := s.db.ExecContext(ctx, `
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
`, obj.Bucket, obj.Key, obj.Size, obj.ETag, obj.ContentType, obj.TelegramFileID, obj.TelegramMessageID, obj.CreatedAt.Format(time.RFC3339Nano), obj.UpdatedAt.Format(time.RFC3339Nano))
	return err
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

func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE objects SET deleted_at = ? WHERE bucket = ? AND key = ?`, time.Now().UTC().Format(time.RFC3339Nano), bucket, key)
	return err
}

func (s *Store) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]Object, error) {
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT bucket, key, size, etag, content_type, telegram_file_id, telegram_message_id, created_at, updated_at
FROM objects
WHERE bucket = ? AND key LIKE ? AND deleted_at IS NULL
ORDER BY key
LIMIT ?
`, bucket, prefix+"%", maxKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []Object
	for rows.Next() {
		var obj Object
		var created, updated string
		if err := rows.Scan(&obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &obj.TelegramFileID, &obj.TelegramMessageID, &created, &updated); err != nil {
			return nil, err
		}
		obj.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		obj.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}
