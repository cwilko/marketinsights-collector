"""
UK Inflation Data Collector

Collects UK inflation data (CPI, CPIH, RPI) from ONS MM23 CSV file and populates
the uk_inflation database tables with proper COICOP hierarchy and price data.

Based on UK ONS MM23 dataset structure with 4,054 columns containing:
- CPI INDEX: 371 columns (316 hierarchical + 55 special aggregates) 
- CPI WEIGHTS: 322 columns matching hierarchical structure
- CPIH INDEX: Similar structure to CPI
- RPI INDEX: 3 main columns with different base periods

Supports the database schema designed for efficient rate calculations and contribution analysis.
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple
import logging
import io
import requests
import tempfile
import os

from .base import BaseCollector


class UKInflationCollector(BaseCollector):
    """Collector for UK inflation data from ONS MM23 CSV file."""
    
    # ONS MM23 CSV download URL
    ONS_MM23_URL = "https://www.ons.gov.uk/file?uri=/economy/inflationandpriceindices/datasets/consumerpriceindices/current/mm23.csv"
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.logger = logging.getLogger(__name__)
        
        # COICOP Level 1 categories mapping
        self.level1_categories = {
            '00': 'ALL ITEMS',
            '01': 'FOOD AND NON-ALCOHOLIC BEVERAGES',
            '02': 'ALCOHOLIC BEVERAGES,TOBACCO & NARCOTICS', 
            '03': 'CLOTHING AND FOOTWEAR',
            '04': 'HOUSING, WATER AND FUELS',
            '05': 'FURN, HH EQUIP & ROUTINE REPAIR OF HOUSE',
            '06': 'HEALTH',
            '07': 'TRANSPORT', 
            '08': 'COMMUNICATION',
            '09': 'RECREATION & CULTURE',
            '10': 'EDUCATION',
            '11': 'HOTELS, CAFES AND RESTAURANTS',
            '12': 'MISCELLANEOUS GOODS AND SERVICES'
        }
        
    def find_column_by_header_text(self, df: pd.DataFrame, header_text: str) -> Optional[int]:
        """Find column index by searching for header text in the first row."""
        header_row = df.iloc[0]
        for i, header in enumerate(header_row):
            if header_text in str(header):
                return i
        return None
    
    def find_column_by_regex(self, df: pd.DataFrame, pattern: str) -> Optional[int]:
        """Find column index by regex pattern matching in the first row."""
        header_row = df.iloc[0]
        regex = re.compile(pattern, re.IGNORECASE)
        for i, header in enumerate(header_row):
            if regex.search(str(header)):
                return i
        return None
    
    def download_mm23_csv(self) -> str:
        """
        Download the latest MM23.csv file from ONS website.
        
        Returns:
            Path to the downloaded CSV file
        """
        self.logger.info(f"Downloading MM23.csv from ONS: {self.ONS_MM23_URL}")
        
        try:
            # Download with progress tracking
            response = requests.get(self.ONS_MM23_URL, stream=True, timeout=300)
            response.raise_for_status()
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False)
            
            # Download with chunked reading for large files
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
                    downloaded_size += len(chunk)
                    
                    # Log progress for large files
                    if total_size > 0 and downloaded_size % (1024 * 1024) == 0:  # Every MB
                        progress = (downloaded_size / total_size) * 100
                        self.logger.debug(f"Download progress: {progress:.1f}% ({downloaded_size:,} / {total_size:,} bytes)")
            
            temp_file.close()
            
            # Verify file size
            file_size = os.path.getsize(temp_file.name)
            self.logger.info(f"MM23.csv downloaded successfully: {file_size:,} bytes to {temp_file.name}")
            
            return temp_file.name
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download MM23.csv from ONS: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error downloading MM23.csv: {e}")
            raise
    
    def parse_weight_data(self, df: pd.DataFrame, weight_col: int, series_type: str) -> Dict[int, float]:
        """Parse annual weight data from yearly sections of the CSV."""
        weights = {}
        
        if weight_col is None:
            return weights
            
        self.logger.info(f"Extracting {series_type} weight data from column {weight_col + 1}...")
        
        for i in range(len(df)):
            first_col = str(df.iloc[i, 0]).strip()
            
            # Check if this row contains a year (weights are annual)
            if first_col.isdigit() and len(first_col) == 4 and 1990 <= int(first_col) <= 2030:
                year = int(first_col)
                weight_val = pd.to_numeric(df.iloc[i, weight_col], errors='coerce')
                
                if pd.notna(weight_val) and 0 < weight_val <= 2000:  # Reasonable weight range
                    weights[year] = float(weight_val)
                    
        self.logger.info(f"Found {len(weights)} {series_type} weight entries")
        return weights
    
    def extract_monthly_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Extract monthly data rows from the CSV."""
        monthly_rows = []
        
        for i in range(len(df)):
            val = str(df.iloc[i, 0]).strip()
            if len(val.split()) == 2:
                parts = val.split()
                if (len(parts[0]) == 4 and parts[0].isdigit() and 
                    parts[1].upper() in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                                       'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']):
                    monthly_rows.append(i)
                    
        self.logger.info(f"Found {len(monthly_rows)} monthly data rows")
        return monthly_rows
    
    def extract_all_coicop_codes(self, df: pd.DataFrame) -> Dict[str, int]:
        """Extract all COICOP codes and their hierarchy levels from CPI INDEX columns."""
        coicop_codes = {}
        header_row = df.iloc[0]
        
        # Pattern to match both CPI INDEX and CPIH INDEX columns and extract COICOP code precisely
        # Match the code followed by a delimiter (space, colon, etc.) to avoid partial matches
        # Include optional alphabetic suffixes like A, B for categories like 07.1.1A, 07.1.1B
        index_pattern = re.compile(r'(?:CPI|CPIH) INDEX ([0-9]+(?:\.[0-9]+)*[A-Z]*)\s*[:=/\s]', re.IGNORECASE)
        
        for i, header in enumerate(header_row):
            header_str = str(header)
            match = index_pattern.search(header_str)
            if match:
                coicop_code = match.group(1)
                
                # Determine level based on dot structure
                # Remove alphabetic suffixes for level calculation
                numeric_code = re.sub(r'[A-Z]+$', '', coicop_code)
                if '.' not in numeric_code:
                    level = 1
                else:
                    level = len(numeric_code.split('.'))
                
                coicop_codes[coicop_code] = level
        
        self.logger.info(f"Found {len(coicop_codes)} COICOP categories across {max(coicop_codes.values())} levels")
        
        # Validate we have reasonable number of categories per level
        level_counts = {}
        for code, level in coicop_codes.items():
            level_counts[level] = level_counts.get(level, 0) + 1
        
        # Validate exact counts based on complete COICOP hierarchy
        # CPI has 316 categories, CPIH has 317 (adds 04.2 Owner Occupiers Housing + 04.9 Council Tax, removes 09.2.1 combined)
        # Since we extract from both CPI and CPIH together, we expect the union of both: 318 total unique categories
        expected_total = 318  # 316 (CPI base) + 2 (CPIH-only: 04.2, 04.9)
        expected_counts = {1: 13, 2: 42, 3: 71, 4: 192}  # Level 2: 40 (base) + 2 (04.2, 04.9) = 42
        
        total_found = sum(level_counts.values())
        if total_found != expected_total:
            raise ValueError(f"CRITICAL ERROR: Found {total_found} COICOP categories, expected exactly {expected_total}. Data integrity issue - missing or extra categories detected.")
        
        # Ensure we have all Level 1 categories (this is non-negotiable)
        if level_counts.get(1, 0) != 13:
            raise ValueError(f"CRITICAL ERROR: Level 1 COICOP categories - Found {level_counts.get(1, 0)}, must be exactly 13. Critical inflation categories missing.")
        
        # Check all levels have exact expected coverage
        for level, expected_count in expected_counts.items():
            if level == 1:  # Already checked above
                continue
            actual_count = level_counts.get(level, 0)
            if actual_count != expected_count:
                raise ValueError(f"CRITICAL ERROR: Level {level} COICOP categories - Found {actual_count}, expected exactly {expected_count}. Data integrity compromised.")
        
        # Log the actual structure found
        for level in sorted(level_counts.keys()):
            self.logger.info(f"Level {level}: {level_counts[level]} categories")
        
        return coicop_codes

    def find_series_columns(self, df: pd.DataFrame) -> Dict[str, Dict[str, Optional[int]]]:
        """Find column positions for all inflation series and weights."""
        columns = {
            'CPI': {'indices': {}, 'weights': {}},
            'CPIH': {'indices': {}, 'weights': {}}, 
            'RPI': {'indices': {}, 'weights': {}}
        }
        
        self.logger.info("Looking up column positions for all series...")
        
        # First, discover all COICOP codes available in the data
        all_coicop_codes = self.extract_all_coicop_codes(df)
        
        # Find all CPI columns (all COICOP levels)
        for coicop_code in all_coicop_codes.keys():
            # CPI INDEX columns - use regex to handle all formatting variations
            if coicop_code == '00':
                # Specific pattern for headline index
                pattern = rf"CPI INDEX {re.escape(coicop_code)}:\s*ALL ITEMS"
            else:
                # Flexible pattern: CPI INDEX XX - allow for combined categories and alphabetic suffixes
                pattern = rf"CPI INDEX {re.escape(coicop_code)}"
            
            cpi_col = self.find_column_by_regex(df, pattern)
            
            if cpi_col is not None:
                columns['CPI']['indices'][coicop_code] = cpi_col
                self.logger.debug(f"✅ Found CPI INDEX {coicop_code} at position {cpi_col + 1}")
            else:
                # Some categories may only exist in CPIH (like 04.2, 04.9), so this is not an error
                self.logger.debug(f"CPI INDEX {coicop_code} not found - may be CPIH-only category")
            
            # CPI WEIGHTS columns - use regex for flexible matching (only for some categories)
            if coicop_code == '00':
                # Special pattern for overall weights (HICP or Overall)
                pattern = rf"CPI WEIGHTS {re.escape(coicop_code)}\s*[:=-]\s*(HICP|Overall)"
                weight_col = self.find_column_by_regex(df, pattern)
                if weight_col is not None:
                    columns['CPI']['weights'][coicop_code] = weight_col
                    self.logger.debug(f"✅ Found CPI WEIGHTS {coicop_code} at position {weight_col + 1}")
                else:
                    # Only error for headline weights which are critical
                    raise ValueError(f"CRITICAL ERROR: CPI WEIGHTS {coicop_code} (headline) column not found in MM23.csv - data integrity compromised")
            elif '.' not in coicop_code:  # Only Level 1 categories typically have weights
                # Flexible pattern for Level 1 category weights
                pattern = rf"CPI WEIGHTS {re.escape(coicop_code)}\s*[:=-]"
                weight_col = self.find_column_by_regex(df, pattern)
                if weight_col is not None:
                    columns['CPI']['weights'][coicop_code] = weight_col
                    self.logger.debug(f"✅ Found CPI WEIGHTS {coicop_code} at position {weight_col + 1}")
                # Note: Not all Level 1 categories may have weights columns, so don't error
        
        self.logger.info(f"Found {len(columns['CPI']['indices'])} CPI INDEX columns and {len(columns['CPI']['weights'])} CPI WEIGHTS columns")
        
        # Find all CPIH columns (all COICOP levels)
        for coicop_code in all_coicop_codes.keys():
            # CPIH INDEX columns - use regex to handle all formatting variations
            if coicop_code == '00':
                # Specific pattern for headline index
                pattern = rf"CPIH INDEX {re.escape(coicop_code)}:\s*ALL ITEMS"
            else:
                # Flexible pattern: CPIH INDEX XX - allow for combined categories and alphabetic suffixes
                pattern = rf"CPIH INDEX {re.escape(coicop_code)}"
            
            cpih_col = self.find_column_by_regex(df, pattern)
            
            if cpih_col is not None:
                columns['CPIH']['indices'][coicop_code] = cpih_col
                self.logger.debug(f"✅ Found CPIH INDEX {coicop_code} at position {cpih_col + 1}")
            # Note: Not all CPI categories have corresponding CPIH categories, so don't error if missing
            
            # CPIH WEIGHTS columns - use regex for flexible matching (only for some categories)
            if coicop_code == '00':
                # Special pattern for overall weights (Overall or HICP)
                pattern = rf"CPIH WEIGHTS {re.escape(coicop_code)}\s*[:=-]\s*(Overall|HICP)"
                weight_col = self.find_column_by_regex(df, pattern)
                if weight_col is not None:
                    columns['CPIH']['weights'][coicop_code] = weight_col
                    self.logger.debug(f"✅ Found CPIH WEIGHTS {coicop_code} at position {weight_col + 1}")
                else:
                    # Only error for headline weights which are critical
                    raise ValueError(f"CRITICAL ERROR: CPIH WEIGHTS {coicop_code} (headline) column not found in MM23.csv - data integrity compromised")
            elif '.' not in coicop_code:  # Only Level 1 categories typically have weights
                # Flexible pattern for Level 1 category weights
                pattern = rf"CPIH WEIGHTS {re.escape(coicop_code)}\s*[:=-]"
                weight_col = self.find_column_by_regex(df, pattern)
                if weight_col is not None:
                    columns['CPIH']['weights'][coicop_code] = weight_col
                    self.logger.debug(f"✅ Found CPIH WEIGHTS {coicop_code} at position {weight_col + 1}")
                # Note: Not all Level 1 categories may have weights columns, so don't error
        
        self.logger.info(f"Found {len(columns['CPIH']['indices'])} CPIH INDEX columns and {len(columns['CPIH']['weights'])} CPIH WEIGHTS columns")
        
        # Find RPI columns (only headline for now)
        rpi_col = self.find_column_by_header_text(df, "RPI All Items Index: Jan 1987=100")
        if rpi_col is not None:
            columns['RPI']['indices']['00'] = rpi_col
            self.logger.info(f"✅ Found RPI All Items at position {rpi_col + 1}")
        else:
            raise ValueError("CRITICAL ERROR: RPI All Items Index column not found in MM23.csv - data integrity compromised")
        
        return columns
    
    def extract_coicop_descriptions_from_headers(self, df: pd.DataFrame) -> Dict[str, str]:
        """Extract COICOP descriptions from column headers for all levels."""
        descriptions = {}
        header_row = df.iloc[0]
        
        # Pattern to match CPI INDEX columns and extract COICOP code and description
        cpi_pattern = re.compile(r'CPI INDEX ([0-9.]+)\s*[:=-]\s*(.+?)\s+2015=100', re.IGNORECASE)
        
        for i, header in enumerate(header_row):
            header_str = str(header)
            match = cpi_pattern.search(header_str)
            if match:
                coicop_code = match.group(1)
                description = match.group(2).strip()
                
                # Clean up description
                description = description.replace('  ', ' ')  # Remove double spaces
                descriptions[coicop_code] = description
        
        self.logger.info(f"Extracted descriptions for {len(descriptions)} COICOP categories")
        return descriptions

    def build_coicop_hierarchy_records(self, coicop_codes: Dict[str, int], descriptions: Dict[str, str]) -> List[Dict[str, Any]]:
        """Build hierarchy records with parent-child relationships."""
        records = []
        
        for coicop_code, level in coicop_codes.items():
            # Determine parent_id based on hierarchy
            parent_id = None
            if level > 1:
                # Parent is the code with one less level
                if level == 2:
                    parent_id = coicop_code.split('.')[0]  # '01.1' -> '01'
                elif level == 3:
                    parts = coicop_code.split('.')
                    parent_id = '.'.join(parts[:2])  # '01.1.1' -> '01.1'
                elif level == 4:
                    parts = coicop_code.split('.')
                    parent_id = '.'.join(parts[:3])  # '01.1.1.1' -> '01.1.1'
            
            # Get description from headers or use Level 1 mapping
            description = descriptions.get(coicop_code)
            if not description and coicop_code in self.level1_categories:
                description = self.level1_categories[coicop_code]
            if not description:
                description = f"COICOP {coicop_code}"  # Fallback
            
            # Calculate sort order
            sort_order = self.calculate_sort_order(coicop_code)
            
            records.append({
                'coicop_id': coicop_code,
                'level': level,
                'parent_id': parent_id,
                'description': description,
                'sort_order': sort_order
            })
        
        return records

    def calculate_sort_order(self, coicop_id: str) -> int:
        """Calculate sort order for hierarchical sorting."""
        parts = coicop_id.split('.')
        
        # Pad each part to 3 digits and join
        padded_parts = []
        for part in parts:
            if part.isdigit():
                padded_parts.append(f"{int(part):03d}")
            else:
                padded_parts.append("000")
        
        # Join and convert to int
        sort_str = ''.join(padded_parts)
        
        # Ensure it fits in database integer limits
        if len(sort_str) > 18:
            sort_str = sort_str[:18]
        
        return int(sort_str) if sort_str else 0

    def populate_coicop_hierarchy(self, conn, csv_file_path: str = None) -> int:
        """Populate the COICOP hierarchy table with all levels."""
        if conn is None:
            # In safe mode, we need to discover the hierarchy from the data
            file_path = csv_file_path if csv_file_path else 'mm23.csv'
            df = pd.read_csv(file_path, header=None, low_memory=False) if hasattr(self, '_temp_df') else None
            if df is not None:
                coicop_codes = self.extract_all_coicop_codes(df)
                descriptions = self.extract_coicop_descriptions_from_headers(df)
                hierarchy_records = self.build_coicop_hierarchy_records(coicop_codes, descriptions)
                self.logger.info(f"Safe mode: Would populate COICOP hierarchy with {len(hierarchy_records)} categories across {max(coicop_codes.values())} levels")
                return len(hierarchy_records)
            else:
                self.logger.info("Safe mode: Would populate COICOP hierarchy table")
                return len(self.level1_categories)
            
        cursor = conn.cursor()
        
        try:
            # We need to re-read the data to extract all codes and descriptions
            # This is not ideal, but necessary for proper hierarchy building
            file_path = csv_file_path if csv_file_path else 'mm23.csv'
            df = pd.read_csv(file_path, header=None, low_memory=False)
            coicop_codes = self.extract_all_coicop_codes(df)
            descriptions = self.extract_coicop_descriptions_from_headers(df)
            hierarchy_records = self.build_coicop_hierarchy_records(coicop_codes, descriptions)
            
            # Insert all hierarchy records
            records_inserted = 0
            for record in hierarchy_records:
                cursor.execute("""
                    INSERT INTO uk_inflation_coicop_hierarchy 
                    (coicop_id, level, parent_id, description, sort_order)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (coicop_id) DO UPDATE SET
                        level = EXCLUDED.level,
                        parent_id = EXCLUDED.parent_id,
                        description = EXCLUDED.description,
                        sort_order = EXCLUDED.sort_order
                """, (
                    record['coicop_id'],
                    record['level'],
                    record['parent_id'],
                    record['description'],
                    record['sort_order']
                ))
                records_inserted += 1
            
            conn.commit()
            self.logger.info(f"Populated COICOP hierarchy: {records_inserted} categories across all levels")
            return records_inserted
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error populating COICOP hierarchy: {e}")
            raise
        finally:
            cursor.close()
    
    def collect_inflation_data(self) -> int:
        """
        Collect UK inflation data by downloading the latest MM23 CSV file from ONS website.
        
        Returns:
            Number of records collected
        """
        self.logger.info("Starting UK inflation data collection...")
        
        # Download latest MM23.csv from ONS
        csv_file_path = self.download_mm23_csv()
        
        try:
            # Read CSV file
            self.logger.info(f"Reading downloaded CSV file: {csv_file_path}")
            df = pd.read_csv(csv_file_path, header=None, low_memory=False)
            
            # Find column positions
            columns = self.find_series_columns(df)
            
            # Extract monthly data rows
            monthly_rows = self.extract_monthly_data(df)
            
            # Parse weight data for all series
            weights_by_series = {}
            for series_type in ['CPI', 'CPIH']:
                weights_by_series[series_type] = {}
                for cat, weight_col in columns[series_type]['weights'].items():
                    if weight_col is not None:
                        weights = self.parse_weight_data(df, weight_col, f"{series_type} {cat}")
                        weights_by_series[series_type][cat] = weights
            
            # Get database connection
            conn = self.get_db_connection()
            
            # Populate COICOP hierarchy
            hierarchy_records = self.populate_coicop_hierarchy(conn, csv_file_path)
            
            # Process monthly data
            records_collected = 0
            month_map = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                         'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
            
            price_data_records = []
            
            for row_idx in monthly_rows:
                date_str = str(df.iloc[row_idx, 0]).strip()
                try:
                    year, month = date_str.split()
                    year_int = int(year)
                    month_int = month_map[month.upper()]
                    obs_date = date(year_int, month_int, 1)
                    
                    # Process each series type
                    for series_type in ['CPI', 'CPIH', 'RPI']:
                        for cat, index_col in columns[series_type]['indices'].items():
                            if index_col is not None:
                                # Extract index value
                                index_val = pd.to_numeric(df.iloc[row_idx, index_col], errors='coerce')
                                
                                if pd.notna(index_val):
                                    # Get weight value if available
                                    weight_val = None
                                    if (series_type in weights_by_series and 
                                        cat in weights_by_series[series_type] and
                                        year_int in weights_by_series[series_type][cat]):
                                        weight_val = weights_by_series[series_type][cat][year_int]
                                    
                                    price_data_records.append({
                                        'date': obs_date,
                                        'coicop_id': cat,
                                        'series_type': series_type,
                                        'index_value': float(index_val),
                                        'weight_value': weight_val,
                                        'source_column': index_col + 1  # 1-based column number
                                    })
                                    records_collected += 1
                    
                except Exception as e:
                    self.logger.warning(f"Error processing row {row_idx} ({date_str}): {e}")
                    continue
            
            # Bulk insert price data
            if conn is not None:
                records_inserted = self.bulk_insert_price_data(conn, price_data_records)
                self.logger.info(f"Inserted {records_inserted} price data records")
            else:
                self.logger.info(f"Safe mode: Would insert {len(price_data_records)} price data records")
            
            self.logger.info(f"UK inflation data collection completed: {records_collected} records processed")
            
        finally:
            # Cleanup downloaded file
            try:
                os.unlink(csv_file_path)
                self.logger.debug(f"Cleaned up downloaded file: {csv_file_path}")
            except Exception as e:
                self.logger.warning(f"Failed to cleanup downloaded file {csv_file_path}: {e}")
        
        return records_collected
    
    def bulk_insert_price_data(self, conn, price_data_records: List[Dict[str, Any]]) -> int:
        """Bulk insert price data records into the database."""
        if not price_data_records:
            return 0
            
        if conn is None:
            self.logger.info(f"Safe mode: Would insert {len(price_data_records)} price data records")
            return len(price_data_records)
            
        cursor = conn.cursor()
        
        try:
            # Use COPY for efficient bulk insert
            data_tuples = [
                (
                    record['date'],
                    record['coicop_id'], 
                    record['series_type'],
                    record['index_value'],
                    record['weight_value'],
                    'ACTUAL',  # data_quality
                    record['source_column']
                )
                for record in price_data_records
            ]
            
            # Create CSV-like string for COPY
            csv_data = io.StringIO()
            for data_tuple in data_tuples:
                csv_data.write('\t'.join([
                    str(data_tuple[0]),  # date
                    str(data_tuple[1]),  # coicop_id
                    str(data_tuple[2]),  # series_type
                    str(data_tuple[3]),  # index_value
                    str(data_tuple[4]) if data_tuple[4] is not None else '\\N',  # weight_value
                    str(data_tuple[5]),  # data_quality
                    str(data_tuple[6])   # source_column
                ]) + '\n')
            csv_data.seek(0)
            
            # Use COPY for bulk insert with conflict resolution
            cursor.execute("""
                CREATE TEMP TABLE temp_price_data (LIKE uk_inflation_price_data INCLUDING ALL)
            """)
            
            cursor.copy_from(
                csv_data,
                'temp_price_data',
                columns=('date', 'coicop_id', 'series_type', 'index_value', 'weight_value', 'data_quality', 'source_column'),
                null='\\N'
            )
            
            # Insert with conflict resolution
            cursor.execute("""
                INSERT INTO uk_inflation_price_data 
                (date, coicop_id, series_type, index_value, weight_value, data_quality, source_column)
                SELECT date, coicop_id, series_type, index_value, weight_value, data_quality, source_column
                FROM temp_price_data
                ON CONFLICT (date, coicop_id, series_type) DO UPDATE SET
                    index_value = EXCLUDED.index_value,
                    weight_value = EXCLUDED.weight_value,
                    data_quality = EXCLUDED.data_quality,
                    source_column = EXCLUDED.source_column
            """)
            
            records_inserted = cursor.rowcount
            conn.commit()
            
            return records_inserted
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error in bulk insert: {e}")
            raise
        finally:
            cursor.close()


def collect_uk_inflation_data(database_url: Optional[str] = None) -> int:
    """
    Collect UK inflation data by downloading latest MM23.csv from ONS and populate database tables.
    
    Args:
        database_url: Database connection URL (None for safe mode)
        
    Returns:
        Number of records collected
    """
    collector = UKInflationCollector(database_url)
    return collector.collect_inflation_data()


if __name__ == "__main__":
    import sys
    
    # Usage: python uk_inflation_data.py [database_url]
    # Always downloads latest MM23.csv from ONS
    
    db_url = sys.argv[1] if len(sys.argv) > 1 else None
    
    if db_url == "--help" or db_url == "-h":
        print("Usage: python uk_inflation_data.py [database_url]")
        print("")
        print("Downloads the latest MM23.csv from ONS and collects UK inflation data.")
        print("")
        print("Arguments:")
        print("  database_url: Database connection URL (optional - safe mode if not provided)")
        print("")
        print("Examples:")
        print("  python uk_inflation_data.py                    # Download and run in safe mode")
        print("  python uk_inflation_data.py postgresql://...   # Download and insert into database")
        sys.exit(0)
    
    logging.basicConfig(level=logging.INFO)
    
    print("Downloading latest MM23.csv from ONS and collecting inflation data...")
    
    result = collect_uk_inflation_data(db_url)
    print(f"Collected {result} records")