blockstream() {
  echo "Activating blockstream venv..."
  source ~/programming/engineering/blockstream/venv/bin/activate

  echo "Syncing latest DuckDB artifact from GitHub..."
  
  OWNER="ChrisAdan"
  REPO="blockstream"
  WORKFLOW="Daily Binance.US Extraction"
  ARTIFACT_NAME="blockstream.duckdb"
  LOCAL_DB_PATH=~/programming/engineering/blockstream/data/blockstream.duckdb
  TEMP_DIR=$(mktemp -d)
  TEMP_ARTIFACT_ZIP=$TEMP_DIR/artifact.zip
  TEMP_ARTIFACT_DB=$TEMP_DIR/$ARTIFACT_NAME

  # Get latest run ID
  RUN_ID=$(gh run list -R $OWNER/$REPO --workflow="$WORKFLOW" --limit 1 --json databaseId --jq '.[0].databaseId')

  if [ -z "$RUN_ID" ]; then
    echo "No workflow runs found. Skipping artifact sync."
  else
    echo "Latest workflow run ID: $RUN_ID"

    ARTIFACT_ID=$(gh run artifact list $RUN_ID -R $OWNER/$REPO --json id,name --jq ".[] | select(.name==\"$ARTIFACT_NAME\") | .id")

    if [ -z "$ARTIFACT_ID" ]; then
      echo "Artifact $ARTIFACT_NAME not found in run $RUN_ID."
    else
      echo "Downloading artifact $ARTIFACT_NAME..."
      gh run artifact download $ARTIFACT_ID -R $OWNER/$REPO --archive -o $TEMP_ARTIFACT_ZIP

      echo "Extracting artifact..."
      unzip -o $TEMP_ARTIFACT_ZIP -d $TEMP_DIR

      echo "Merging artifact data into local DuckDB..."
      python ~/programming/engineering/blockstream/merge_duckdb_artifacts.py "$LOCAL_DB_PATH" "$TEMP_ARTIFACT_DB"

      echo "Cleaning up..."
      rm -rf $TEMP_DIR

      echo "Local DuckDB file incrementally updated."
    fi
  fi
}