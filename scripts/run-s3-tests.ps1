#!/usr/bin/env pwsh
# Run Ceph s3-tests against the telegram-s3 gateway with the unsupported
# feature markers filtered out. See S3-COMPAT-ACCEPTANCE.md for context.
#
# The marker expression excludes every category the gateway intentionally
# omits (versioning, object lock, ACLs/bucket policies, lifecycle, SSE,
# tagging, IAM/STS, website, s3select, logging, replication, storage classes,
# delete markers, append-object, aws2 sigs, and Ceph-only behaviors that fail
# on AWS itself).
#
# Usage:
#   .\scripts\run-s3-tests.ps1                       # full filtered run
#   .\scripts\run-s3-tests.ps1 -K 'list or copy'     # subset by name
#   .\scripts\run-s3-tests.ps1 -Maxfail 20           # stop after N failures
#
# Env:
#   S3TESTS_DIR  — path to the ceph/s3-tests checkout
#                  (default: ..\..\s3-tests relative to the repo)

[CmdletBinding()]
param(
    [string]$K = '',
    [int]$Maxfail = 0,
    [string]$ReportFile = ''
)

$repo = Split-Path -Parent $PSScriptRoot
$s3testsDir = if ($env:S3TESTS_DIR) { $env:S3TESTS_DIR } else { Join-Path (Split-Path -Parent $repo) 's3-tests' }
$conf = Join-Path $s3testsDir 's3tests.conf'
$python = Join-Path $s3testsDir '.venv\Scripts\python.exe'

if (-not (Test-Path $python))     { throw "venv python not found at $python" }
if (-not (Test-Path $conf))       { throw "s3tests.conf not found at $conf" }

$markerExpr = @(
    'versioning', 'delete_marker', 'object_lock',
    'bucket_policy', 'bucket_encryption', 'sse_s3', 'encryption',
    'bucket_logging', 'bucket_logging_cleanup', 'fails_without_logging_rollover',
    'lifecycle', 'lifecycle_expiration', 'lifecycle_transition',
    'tagging',
    'iam_account', 'iam_cross_account', 'iam_role', 'iam_tenant', 'iam_user',
    'role_policy', 'session_policy', 'user_policy', 'abac_test',
    'group', 'group_policy', 'object_ownership',
    'test_of_sts', 'webidentity_test',
    'token_claims_trust_policy_test', 'token_principal_tag_role_policy_test',
    'token_request_tag_trust_policy_test', 'token_resource_tags_test',
    'token_role_tags_test', 'token_tag_keys_test',
    's3select', 's3website', 's3website_routing_rules', 's3website_redirect_location',
    'sns', 'appendobject',
    'cloud_transition', 'cloud_restore', 'storage_class', 'target_by_bucket',
    'auth_aws2', 'fails_on_aws'
) -join ' or '
$markerExpr = "not ($markerExpr)"

$env:S3TEST_CONF = $conf
$args = @(
    '-m', 'pytest',
    (Join-Path $s3testsDir 's3tests\functional\test_s3.py'),
    '-m', $markerExpr,
    '-q', '--tb=line', '-rN'
)
if ($K)       { $args += @('-k', $K) }
if ($Maxfail) { $args += @("--maxfail=$Maxfail") }
if ($ReportFile) { $args += @("--junit-xml=$ReportFile") }

& $python @args
