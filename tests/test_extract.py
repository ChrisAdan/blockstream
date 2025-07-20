import pytest
import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from pathlib import Path
import duckdb
from scripts import extract

@pytest.fixture
def temp_duckdb(tmp_path):
    db_path = tmp_path / "test.duckdb"
    extract.ensure_table_exists(str(db_path))
    return str(db_path)

@pytest.fixture
def sample_data():
    return [{"symbol": "BTCUSDT", "priceChange": "100", "lastPrice": "20000"}]

def test_save_raw_json(tmp_path, sample_data):
    date_str = "20250720"
    extract.save_raw_json(sample_data, date_str, base_dir=tmp_path)
    saved_file = tmp_path / f"daily_24hr_ticker_{date_str}.json"
    assert saved_file.exists()
    content = json.loads(saved_file.read_text())
    assert isinstance(content, list)
    assert content[0]["symbol"] == "BTCUSDT"

def test_exists_for_date(temp_duckdb, sample_data):
    date_str = 20250720
    # Initially no data
    assert not extract.exists_for_date(temp_duckdb, date_str)
    # Insert sample data
    extract.write_raw_to_duckdb(sample_data, date_str, duckdb_path=temp_duckdb)
    assert extract.exists_for_date(temp_duckdb, date_str)

@patch("scripts.extract.requests.get")
def test_fetch_binance_24hr_ticker_success(mock_get, sample_data):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_data
    mock_get.return_value = mock_response

    result = extract.fetch_binance_24hr_ticker()
    assert result == sample_data
    mock_get.assert_called_once()

@patch("scripts.extract.requests.get")
def test_fetch_binance_24hr_ticker_429_then_success(mock_get, sample_data):
    # Simulate 429 response then 200 response
    response_429 = MagicMock(status_code=429)
    response_200 = MagicMock(status_code=200, json=lambda: sample_data)
    mock_get.side_effect = [response_429, response_200]

    with patch("time.sleep") as mock_sleep:
        result = extract.fetch_binance_24hr_ticker()
        assert result == sample_data
        assert mock_get.call_count == 2
        mock_sleep.assert_called()

@patch("scripts.extract.requests.get")
def test_fetch_binance_24hr_ticker_418_ban(mock_get):
    response_418 = MagicMock(status_code=418)
    response_418.text = "IP banned until 1893456000000"  # timestamp in ms
    mock_get.return_value = response_418

    with pytest.raises(SystemExit):
        extract.fetch_binance_24hr_ticker()

def test_write_raw_to_duckdb_duplicate_handling(temp_duckdb, sample_data):
    date_str = 20250720
    extract.write_raw_to_duckdb(sample_data, date_str, duckdb_path=temp_duckdb)
    # Insert again - should skip duplicate gracefully
    extract.write_raw_to_duckdb(sample_data, date_str, duckdb_path=temp_duckdb)

def test_backfill_range_skips_existing(tmp_path, sample_data):
    temp_db = tmp_path / "test.duckdb"
    extract.ensure_table_exists(str(temp_db))

    # Insert one date manually
    date_str = 20250720
    extract.write_raw_to_duckdb(sample_data, date_str, duckdb_path=str(temp_db))

    # Patch fetch_binance_24hr_ticker to always return sample_data
    with patch("scripts.extract.fetch_binance_24hr_ticker", return_value=sample_data) as mock_fetch:
        extract.backfill_range(
            start_date=datetime(2025, 7, 20, tzinfo=timezone.utc),
            end_date=datetime(2025, 7, 21, tzinfo=timezone.utc),
            duckdb_path=str(temp_db)
        )
        # Should fetch only for 2025-07-21 (once)
        mock_fetch.assert_called_once()

