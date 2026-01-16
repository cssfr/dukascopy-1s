#!/usr/bin/env python3
"""
Update instruments.json metadata by scanning actual data files in MinIO
"""

import json
import subprocess
import re
import polars as pl
import tempfile
import os
from datetime import datetime
from typing import Dict, Optional

def run_mc_command(cmd: str) -> str:
    """Run MinIO client command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command '{cmd}': {e}")
        return ""

def get_latest_date_for_symbol(symbol: str) -> Optional[str]:
    """
    Download and read the parquet file to get exact latest date
    Returns date in YYYY-MM-DD format or None if no data found
    """
    print(f"Scanning latest 1Ys data for {symbol} (accurate method)")
    
    # Path pattern: ohlcv/1Ys/symbol=SYMBOL/
    path_pattern = f"myminio/dukascopy-node/ohlcv/1Ys/symbol={symbol}/"
    
    # Get list of year directories
    years_output = run_mc_command(f"mc ls {path_pattern}")
    if not years_output:
        print(f"No data found for {symbol}")
        return None
    
    # Extract years and find the latest
    years = []
    for line in years_output.split('\n'):
        if 'year=' in line:
            year_match = re.search(r'year=(\d{4})/', line)
            if year_match:
                years.append(int(year_match.group(1)))
    
    if not years:
        print(f"No year directories found for {symbol}")
        return None
    
    # Start from the latest year and work backwards
    years.sort(reverse=True)
    
    for year in years:
        year_path = f"{path_pattern}year={year}/"
        files_output = run_mc_command(f"mc ls {year_path}")
        
        if not files_output:
            continue
            
        # Look for the yearly parquet file: SYMBOL_YEAR.parquet
        expected_filename = f"{symbol}_{year}.parquet"
        if expected_filename in files_output:
            print(f"Found yearly file for {symbol}: {expected_filename}")
            
            # Download and read the file to get exact latest date
            remote_file = f"{year_path}{expected_filename}"
            temp_file = None
            
            try:
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    temp_file = tmp_file.name
                    
                    # Download file
                    print(f"Downloading {remote_file}...")
                    download_cmd = f"mc cp {remote_file} {temp_file}"
                    run_mc_command(download_cmd)
                    
                    # Read parquet and get latest date
                    df = pl.read_parquet(temp_file)
                    print(f"Read {len(df)} rows from {expected_filename}")
                    
                    # Find date/timestamp column and get latest date
                    date_columns = [col for col in df.columns if any(word in col.lower() for word in ['date', 'time', 'timestamp'])]
                    
                    if not date_columns:
                        print(f"Warning: No date/timestamp column found in {symbol}. Columns: {df.columns}")
                        # Fallback: assume current year goes to today, past years to Dec 31
                        if year == datetime.now().year:
                            latest_date = datetime.now().strftime("%Y-%m-%d")
                        else:
                            latest_date = f"{year}-12-31"
                        print(f"Using fallback date for {symbol}: {latest_date}")
                        return latest_date
                    
                    # Use the first date column found
                    date_col = date_columns[0]
                    print(f"Using date column: {date_col}")
                    
                    # Get the latest date from the column
                    latest_timestamp = df[date_col].max()
                    
                    # Convert to date string based on the data type
                    if hasattr(latest_timestamp, 'date'):
                        latest_date = latest_timestamp.date().strftime("%Y-%m-%d")
                    elif hasattr(latest_timestamp, 'strftime'):
                        latest_date = latest_timestamp.strftime("%Y-%m-%d")
                    else:
                        # Try to parse as string if it's not a proper datetime
                        latest_date = str(latest_timestamp)[:10]  # Take first 10 chars (YYYY-MM-DD)
                    
                    print(f"Actual latest data for {symbol}: {latest_date}")
                    return latest_date
                    
            except Exception as e:
                print(f"Error processing parquet file for {symbol}: {e}")
                # Fallback to estimation
                if year == datetime.now().year:
                    latest_date = datetime.now().strftime("%Y-%m-%d")
                else:
                    latest_date = f"{year}-12-31"
                print(f"Using fallback date for {symbol}: {latest_date}")
                return latest_date
            finally:
                # Clean up temp file
                if temp_file and os.path.exists(temp_file):
                    try:
                        os.unlink(temp_file)
                        print(f"Cleaned up temp file for {symbol}")
                    except Exception as e:
                        print(f"Warning: Could not clean up temp file {temp_file}: {e}")
    
    print(f"No yearly parquet files found for {symbol}")
    return None

def update_instruments_metadata():
    """Update the instruments.json file with current data boundaries"""
    
    # Load current metadata
    try:
        with open('instruments.json', 'r') as f:
            metadata = json.load(f)
    except FileNotFoundError:
        print("Error: instruments.json not found")
        return
    except json.JSONDecodeError as e:
        print(f"Error parsing instruments.json: {e}")
        return
    
    current_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Update the global boundary update timestamp
    metadata["_data_boundaries_updated"] = current_timestamp
    
    # Process each instrument
    for symbol, instrument_data in metadata.items():
        if symbol.startswith('_'):  # Skip metadata fields
            continue
            
        print(f"\nProcessing {symbol}...")
        
        # Get the latest date for this symbol (using accurate method)
        latest_date = get_latest_date_for_symbol(symbol)
        
        if latest_date:
            # Update the main dataRange
            if "dataRange" not in instrument_data:
                instrument_data["dataRange"] = {}
            
            instrument_data["dataRange"]["latest"] = latest_date
            
            # Update the sources section
            if "sources" not in instrument_data["dataRange"]:
                instrument_data["dataRange"]["sources"] = {}
            
            if "1Ys" not in instrument_data["dataRange"]["sources"]:
                instrument_data["dataRange"]["sources"]["1Ys"] = {}
            
            instrument_data["dataRange"]["sources"]["1Ys"]["latest"] = latest_date
            
            print(f"Updated {symbol} latest date to: {latest_date}")
        else:
            print(f"Warning: No data found for {symbol}, keeping existing dates")
    
    # Save updated metadata
    try:
        with open('instruments.json', 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"\nMetadata updated successfully at {current_timestamp}")
    except Exception as e:
        print(f"Error saving instruments.json: {e}")

if __name__ == "__main__":
    update_instruments_metadata()
