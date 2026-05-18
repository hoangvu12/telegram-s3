# telegram-s3

Experimental S3-compatible gateway backed by Telegram file storage.

This is for personal/small-scale use. Telegram is not a real object storage provider and does not offer S3 durability, availability, or API guarantees.

## Current Scope

- Go HTTP server
- SQLite metadata
- Telegram Bot API storage backend
- Path-style S3 endpoints
- Static AWS SigV4 credentials
- Basic operations: buckets, put/get/head/delete object, list objects

Not implemented yet: multipart uploads, range reads, object tags, ACLs, versioning, lifecycle policies, MTProto large-file worker, encryption, cache.

## Configuration

Copy `.env.example` values into your environment.

```env
LISTEN_ADDR=:9000
DATABASE_PATH=telegram-s3.db
S3_ACCESS_KEY_ID=dev-access-key
S3_SECRET_ACCESS_KEY=dev-secret-key
TELEGRAM_BOT_TOKEN=123456:your-bot-token
TELEGRAM_CHAT_ID=-1001234567890
```

The bot must be able to send documents to `TELEGRAM_CHAT_ID`.

## Run

```bash
go run ./cmd/telegram-s3
```

## Test With AWS CLI

```bash
aws configure set aws_access_key_id dev-access-key
aws configure set aws_secret_access_key dev-secret-key
aws configure set region us-east-1

aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp ./README.md s3://test/README.md
aws --endpoint-url http://localhost:9000 s3 ls s3://test
aws --endpoint-url http://localhost:9000 s3 cp s3://test/README.md ./downloaded-README.md
```
