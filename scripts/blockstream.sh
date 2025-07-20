blockstream() {
  echo "Activating blockstream venv..."
  source ~/programming/engineering/blockstream/venv/bin/activate

  echo "Syncing latest DuckDB artifact from GitHub…"

  OWNER="ChrisAdan"
  REPO="blockstream"
  WORKFLOW="Daily Binance.US Extraction"
  ARTIFACT_NAME="blockstream.duckdb"
  LOCAL_DB_PATH=~/programming/engineering/blockstream/data/blockstream.duckdb
  TEMP_DIR=$(mktemp -d)

  # 1️⃣ Get latest successful run for the workflow
  RUN_ID=$(gh run list -R "$OWNER/$REPO" \
           --workflow="$WORKFLOW" --limit 1 \
           --json conclusion,databaseId \
           --jq '.[] | select(.conclusion=="success") | .databaseId')

  if [[ -z "$RUN_ID" ]]; then
    echo "No successful workflow runs found – skipping sync."
    return 1
  fi
  echo "Latest successful workflow run ID: $RUN_ID"

  # 2️⃣ Download *that run’s* artifact
  echo "Downloading artifact $ARTIFACT_NAME from run $RUN_ID…"
  gh run download "$RUN_ID" -n "$ARTIFACT_NAME" -R "$OWNER/$REPO" --dir "$TEMP_DIR"

  TEMP_ARTIFACT_DB="$TEMP_DIR/$ARTIFACT_NAME"
  if [[ ! -f "$TEMP_ARTIFACT_DB" ]]; then
    echo "⛔  Artifact file not found at $TEMP_ARTIFACT_DB"
    rm -rf "$TEMP_DIR"
    return 1
  fi

  # 3️⃣ Incrementally merge into your local DuckDB
  echo "Merging artifact data into local DuckDB…"
  python ~/programming/engineering/blockstream/scripts/merge_duckdb_artifacts.py \
         "$LOCAL_DB_PATH" "$TEMP_ARTIFACT_DB"

  # 4️⃣ Cleanup
  rm -rf "$TEMP_DIR"
  echo "✅ Local DuckDB incrementally updated."
}
