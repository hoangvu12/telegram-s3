#!/usr/bin/env bash
# Live acceptance for the Telegram-S3 gateway (real default clients).
# Self-contained; see S3-COMPAT-ACCEPTANCE.md. Safe: refuses bucket "send",
# touches only its own throwaway bucket, deletes exactly what it creates.
set -uo pipefail

S3_ENDPOINT="${S3_ENDPOINT:-https://s3.nguyenvu.dev}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-telegram-s3-local}"
S3_SECRET_KEY="${S3_SECRET_KEY:-}"
TEST_BUCKET="${TEST_BUCKET:-phase7-accept}"
ACCEPT_BIG_MIB="${ACCEPT_BIG_MIB:-20}"
ACCEPT_LIST_N="${ACCEPT_LIST_N:-2500}"
RUN_RCLONE="${RUN_RCLONE:-1}"
RUN_RESTIC="${RUN_RESTIC:-0}"

# Secrets live in .env (git-ignored, never committed). If S3_SECRET_KEY is not
# already exported, source .env and map the gateway's own var names.
if [ -z "$S3_SECRET_KEY" ] && [ -f .env ]; then
  set -a; . ./.env; set +a
  S3_SECRET_KEY="${S3_SECRET_KEY:-${S3_SECRET_ACCESS_KEY:-}}"
  S3_ACCESS_KEY="${S3_ACCESS_KEY:-${S3_ACCESS_KEY_ID:-$S3_ACCESS_KEY}}"
fi

[ -n "$S3_SECRET_KEY" ] || { echo "FATAL: set S3_SECRET_ACCESS_KEY in .env (or export S3_SECRET_KEY)"; exit 2; }
if [ "$TEST_BUCKET" = "send" ]; then echo "FATAL: refusing to run against the 'send' bucket (Gokapi data)"; exit 2; fi
command -v aws >/dev/null || { echo "FATAL: aws CLI v2 not found"; exit 2; }

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
export AWS_CONFIG_FILE="$WORK/awscfg" AWS_SHARED_CREDENTIALS_FILE="$WORK/awscreds"
export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" AWS_DEFAULT_REGION=us-east-1
# path-style only (Gokapi-equivalent). Do NOT touch checksum/payload-signing
# config: the aws-cli defaults (aws-chunked streaming) are exactly the point.
aws configure set default.s3.addressing_style path
B="s3://$TEST_BUCKET"
awsx() { aws --endpoint-url "$S3_ENDPOINT" --no-cli-pager "$@"; }

PASS=0 FAIL=0 SKIP=0
pass() { echo "PASS  $1"; PASS=$((PASS+1)); }
fail() { echo "FAIL  $1 -- ${2:-}"; FAIL=$((FAIL+1)); }
skip() { echo "SKIP  $1 -- ${2:-}"; SKIP=$((SKIP+1)); }
code() { curl -s -m 30 -o /dev/null -w '%{http_code}' "$@"; }

echo "== endpoint=$S3_ENDPOINT bucket=$TEST_BUCKET big=${ACCEPT_BIG_MIB}MiB listN=$ACCEPT_LIST_N =="

# --- bucket lifecycle ------------------------------------------------------
out="$(awsx s3api create-bucket --bucket "$TEST_BUCKET" 2>&1)"
if [ $? -eq 0 ] || echo "$out" | grep -q "BucketAlreadyOwnedByYou"; then pass "create-bucket"; else fail "create-bucket" "$out"; fi
awsx s3api head-bucket --bucket "$TEST_BUCKET" >/dev/null 2>&1 && pass "head-bucket" || fail "head-bucket"

# --- small round-trip (default checksums = aws-chunked, P1) ----------------
echo -n "hello-acceptance" > "$WORK/a.txt"
awsx s3 cp "$WORK/a.txt" "$B/a.txt" >/dev/null 2>&1 && pass "put small" || fail "put small"
awsx s3 cp "$B/a.txt" "$WORK/a.out" >/dev/null 2>&1 && cmp -s "$WORK/a.txt" "$WORK/a.out" \
  && pass "get small byte-identical" || fail "get small byte-identical"

# --- P7.5 metadata persist + echo + response-* override --------------------
awsx s3api put-object --bucket "$TEST_BUCKET" --key meta.bin --body "$WORK/a.txt" \
  --content-type "text/plain" --content-disposition 'attachment; filename="m.txt"' \
  --metadata foo=bar >/dev/null 2>&1 && pass "put-object w/ metadata" || fail "put-object w/ metadata"
hd="$(awsx s3api head-object --bucket "$TEST_BUCKET" --key meta.bin 2>/dev/null)"
echo "$hd" | grep -q '"foo": "bar"' && echo "$hd" | grep -qi 'attachment' \
  && pass "head-object echoes metadata" || fail "head-object echoes metadata" "$hd"
ov="$(code "$S3_ENDPOINT/$TEST_BUCKET/meta.bin?response-content-type=application/json")"
[ "$ov" = "200" ] && pass "response-content-type override 200" || fail "response-* override" "http $ov"

# --- P7.5 conditional requests (public read path) --------------------------
etag="$(awsx s3api head-object --bucket "$TEST_BUCKET" --key a.txt --query ETag --output text 2>/dev/null)"
c="$(code -H "If-None-Match: $etag" "$S3_ENDPOINT/$TEST_BUCKET/a.txt")"
[ "$c" = "304" ] && pass "If-None-Match -> 304" || fail "If-None-Match -> 304" "http $c"
c="$(code -H 'If-Match: "deadbeef"' "$S3_ENDPOINT/$TEST_BUCKET/a.txt")"
[ "$c" = "412" ] && pass "If-Match mismatch -> 412" || fail "If-Match -> 412" "http $c"

# --- P5 Range GET ----------------------------------------------------------
c="$(code -r 0-3 "$S3_ENDPOINT/$TEST_BUCKET/a.txt")"
[ "$c" = "206" ] && pass "Range bytes=0-3 -> 206" || fail "Range -> 206" "http $c"

# --- P7.4 CopyObject (server-side) -----------------------------------------
awsx s3api copy-object --bucket "$TEST_BUCKET" --key a-copy.txt \
  --copy-source "$TEST_BUCKET/a.txt" >/dev/null 2>&1 && pass "copy-object" || fail "copy-object"
awsx s3 cp "$B/a-copy.txt" "$WORK/a-copy.out" >/dev/null 2>&1 && cmp -s "$WORK/a.txt" "$WORK/a-copy.out" \
  && pass "copied object byte-identical" || fail "copied object byte-identical"

# --- P4 multipart + P3 multi-chunk + P5 ranged reassembly ------------------
head -c $((ACCEPT_BIG_MIB*1024*1024)) /dev/urandom > "$WORK/big.bin"
awsx s3 cp "$WORK/big.bin" "$B/big.bin" >/dev/null 2>&1 && pass "multipart upload ${ACCEPT_BIG_MIB}MiB" || fail "multipart upload"
awsx s3 cp "$B/big.bin" "$WORK/big.out" >/dev/null 2>&1 && cmp -s "$WORK/big.bin" "$WORK/big.out" \
  && pass "multi-chunk download byte-identical" || fail "multi-chunk download byte-identical"

# --- P7.1 subresource probes ----------------------------------------------
awsx s3api get-bucket-location --bucket "$TEST_BUCKET" >/dev/null 2>&1 && pass "get-bucket-location" || fail "get-bucket-location"
awsx s3api get-bucket-versioning --bucket "$TEST_BUCKET" >/dev/null 2>&1 && pass "get-bucket-versioning" || fail "get-bucket-versioning"
te="$(awsx s3api get-bucket-tagging --bucket "$TEST_BUCKET" 2>&1)"
echo "$te" | grep -q "NoSuchTagSet" && pass "get-bucket-tagging -> NoSuchTagSet" || fail "get-bucket-tagging" "$te"

# --- P6 listing/pagination at scale ---------------------------------------
mkdir -p "$WORK/scale"
for i in $(seq 1 "$ACCEPT_LIST_N"); do printf 'k' > "$WORK/scale/obj-$(printf '%05d' "$i").txt"; done
echo "  uploading $ACCEPT_LIST_N keys (each is one Telegram message)..."
awsx s3 cp "$WORK/scale" "$B/scale/" --recursive >/dev/null 2>&1 && pass "bulk upload $ACCEPT_LIST_N keys" || fail "bulk upload"
# --page-size forces the gateway's server-side continuation (real IsTruncated
# + NextContinuationToken paging); the CLI follows every page and aggregates,
# so length(Contents) == N proves P6 pagination end-to-end at scale.
n="$(awsx s3api list-objects-v2 --bucket "$TEST_BUCKET" --prefix scale/ --page-size 1000 \
      --query 'length(Contents)' --output text 2>/dev/null)"
[ "$n" = "$ACCEPT_LIST_N" ] && pass "paginated list counted $n == $ACCEPT_LIST_N" || fail "paginated list" "counted $n want $ACCEPT_LIST_N"

# --- optional: rclone ------------------------------------------------------
if [ "$RUN_RCLONE" = "1" ] && command -v rclone >/dev/null; then
  RC=":s3,provider=Other,access_key_id=$S3_ACCESS_KEY,secret_access_key=$S3_SECRET_KEY,endpoint=$S3_ENDPOINT,force_path_style=true:"
  rclone lsd "$RC" >/dev/null 2>&1 && pass "rclone lsd" || fail "rclone lsd"
  rclone copy "$WORK/a.txt" "${RC}${TEST_BUCKET}/rclone/" >/dev/null 2>&1 \
    && rclone cat "${RC}${TEST_BUCKET}/rclone/a.txt" 2>/dev/null | grep -q hello-acceptance \
    && pass "rclone copy+cat" || fail "rclone copy+cat"
  rclone purge "${RC}${TEST_BUCKET}/rclone" >/dev/null 2>&1 && pass "rclone purge" || fail "rclone purge"
else skip "rclone" "RUN_RCLONE!=1 or rclone not installed"; fi

# --- optional: restic ------------------------------------------------------
if [ "$RUN_RESTIC" = "1" ] && command -v restic >/dev/null; then
  export RESTIC_REPOSITORY="s3:$S3_ENDPOINT/$TEST_BUCKET/restic" RESTIC_PASSWORD=acceptance
  restic init >/dev/null 2>&1 && restic backup "$WORK/a.txt" >/dev/null 2>&1 \
    && restic snapshots >/dev/null 2>&1 && restic forget --keep-last 1 --prune >/dev/null 2>&1 \
    && pass "restic init/backup/snapshots/prune" || fail "restic round-trip"
else skip "restic" "RUN_RESTIC!=1 or restic not installed"; fi

# --- P7.2 bulk DeleteObjects + teardown -----------------------------------
awsx s3 rm "$B" --recursive >/dev/null 2>&1 && pass "bulk delete (s3 rm --recursive)" || fail "bulk delete"
awsx s3api delete-bucket --bucket "$TEST_BUCKET" >/dev/null 2>&1 && pass "delete-bucket" || fail "delete-bucket"

echo "============================================================"
echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped"
echo "Gokapi Level-3 E2E is manual — see S3-COMPAT-ACCEPTANCE.md §F"
[ "$FAIL" -eq 0 ]
