#!/usr/bin/env bash

# Generic background push script for the portable encoded-bundle workflow.
# Copy this file to .git/hooks/push_bundle_background.sh in the target repository.

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HOOK_OUTPUT_DIR="$PROJECT_ROOT/.post-commit-output"
LOG_FILE="$HOOK_OUTPUT_DIR/bundle-push.log"
PROJECT_NAME="${BUNDLE_PROJECT_NAME:-$(basename "$PROJECT_ROOT")}"
DEFAULT_BASENAME="${PROJECT_NAME}_latest"

detect_python() {
    if [ -n "${BUNDLE_PYTHON:-}" ]; then
        printf '%s\n' "$BUNDLE_PYTHON"
        return 0
    fi
    if [ -x "$PROJECT_ROOT/.venv/bin/python3" ]; then
        printf '%s\n' "$PROJECT_ROOT/.venv/bin/python3"
        return 0
    fi
    if [ -x "$PROJECT_ROOT/.venv/bin/python" ]; then
        printf '%s\n' "$PROJECT_ROOT/.venv/bin/python"
        return 0
    fi
    if command -v python3 >/dev/null 2>&1; then
        command -v python3
        return 0
    fi
    if command -v python >/dev/null 2>&1; then
        command -v python
        return 0
    fi
    printf '\n'
}

mkdir -p "$HOOK_OUTPUT_DIR"
exec > "$LOG_FILE" 2>&1

# Serialize concurrent pushes — if another push is running, skip this one
exec 9>"$HOOK_OUTPUT_DIR/push.lock"
flock -n 9 || { printf '[%s] Push already in progress, skipping\n' "$(date)"; exit 0; }

printf '[%s] Starting background bundle push...\n' "$(date)"

CURRENT_BRANCH="$(cat "$HOOK_OUTPUT_DIR/current_branch.txt" 2>/dev/null || echo main)"
COMMIT_MSG="$(cat "$HOOK_OUTPUT_DIR/commit_msg.txt" 2>/dev/null || echo unknown)"
CURRENT_COMMIT="$(cat "$HOOK_OUTPUT_DIR/current_commit.txt" 2>/dev/null || echo unknown)"
BUNDLE_BASENAME="$(cat "$HOOK_OUTPUT_DIR/bundle_basename.txt" 2>/dev/null || echo "$DEFAULT_BASENAME")"
ENCODED_RELEASE_BRANCH="$(cat "$HOOK_OUTPUT_DIR/encoded_release_branch.txt" 2>/dev/null || echo encoded-releases)"
SHORT_COMMIT="${CURRENT_COMMIT:0:7}"
PYTHON_BIN="$(detect_python)"

BUNDLE="$PROJECT_ROOT/encoded_releases/${BUNDLE_BASENAME}.txt"
META="$PROJECT_ROOT/encoded_releases/${BUNDLE_BASENAME}_metadata.json"
TEMP_META="/tmp/${PROJECT_NAME}_meta_$$.json"

printf 'Source branch: %s\n' "$CURRENT_BRANCH"
printf 'Source commit: %s\n' "$CURRENT_COMMIT"
printf 'Bundle file: %s\n' "$BUNDLE"
printf 'Metadata file: %s\n' "$META"
printf 'Target branch: %s\n' "$ENCODED_RELEASE_BRANCH"

if [ ! -f "$BUNDLE" ]; then
    printf 'ERROR: bundle file not found: %s\n' "$BUNDLE"
    exit 1
fi

if [ ! -f "$META" ]; then
    {
        printf '{\n'
        printf '  "generated_by": "background push script",\n'
        printf '  "bundle_file": "%s"\n' "$(basename "$BUNDLE")"
        printf '}\n'
    } > "$META"
fi

if [ -n "$PYTHON_BIN" ]; then
    "$PYTHON_BIN" - "$META" "$TEMP_META" "$CURRENT_COMMIT" "$CURRENT_BRANCH" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

meta_path = Path(sys.argv[1])
temp_path = Path(sys.argv[2])
source_commit = sys.argv[3]
source_branch = sys.argv[4]

with meta_path.open(encoding="utf-8") as handle:
    metadata = json.load(handle)

metadata["source_commit"] = source_commit
metadata["source_branch"] = source_branch
metadata["bundle_timestamp"] = datetime.now(timezone.utc).isoformat()

with temp_path.open("w", encoding="utf-8") as handle:
    json.dump(metadata, handle, indent=2)
PY
    PY_EXIT=$?
    if [ $PY_EXIT -ne 0 ]; then
        printf 'ERROR: metadata update failed (exit code: %s)\n' "$PY_EXIT"
        exit 1
    fi
else
    cp "$META" "$TEMP_META"
fi

cd "$PROJECT_ROOT" || exit 1

BLOB_BUNDLE="$(git hash-object -w "$BUNDLE")"
BLOB_META="$(git hash-object -w "$TEMP_META")"
rm -f "$TEMP_META"

printf 'Bundle blob: %s\n' "$BLOB_BUNDLE"
printf 'Meta blob:   %s\n' "$BLOB_META"

TREE="$(
    printf '100644 blob %s\t%s\n100644 blob %s\t%s\n' \
        "$BLOB_BUNDLE" "$(basename "$BUNDLE")" \
        "$BLOB_META" "$(basename "$META")" \
    | git mktree
)"
printf 'Tree: %s\n' "$TREE"

PARENT=""
if git show-ref --verify --quiet "refs/heads/$ENCODED_RELEASE_BRANCH"; then
    PARENT="$(git rev-parse "refs/heads/$ENCODED_RELEASE_BRANCH")"
elif git show-ref --verify --quiet "refs/remotes/origin/$ENCODED_RELEASE_BRANCH"; then
    git fetch -q origin "$ENCODED_RELEASE_BRANCH"
    PARENT="$(git rev-parse "refs/remotes/origin/$ENCODED_RELEASE_BRANCH")"
fi

COMMIT_MESSAGE="Auto-encoded release for commit $SHORT_COMMIT

Original commit: $COMMIT_MSG"

if [ -n "$PARENT" ]; then
    NEW_COMMIT="$(printf '%s\n' "$COMMIT_MESSAGE" | git commit-tree "$TREE" -p "$PARENT")"
else
    NEW_COMMIT="$(printf '%s\n' "$COMMIT_MESSAGE" | git commit-tree "$TREE")"
fi
printf 'New commit: %s\n' "$NEW_COMMIT"

git update-ref "refs/heads/$ENCODED_RELEASE_BRANCH" "$NEW_COMMIT"
git push origin "$ENCODED_RELEASE_BRANCH"
PUSH_EXIT=$?

if [ $PUSH_EXIT -eq 0 ]; then
    printf 'PASS: pushed to origin/%s\n' "$ENCODED_RELEASE_BRANCH"
else
    printf 'FAIL: push exited with code %s\n' "$PUSH_EXIT"
fi

printf '[%s] Background push completed\n' "$(date)"
exit "$PUSH_EXIT"
