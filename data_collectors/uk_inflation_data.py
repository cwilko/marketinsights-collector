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
        
        # Validate COICOP hierarchy coverage - be flexible since we'll filter by weight availability
        total_found = sum(level_counts.values())
        self.logger.info(f"Found {total_found} total COICOP categories across {max(level_counts.keys())} levels")
        
        # Ensure we have all Level 1 categories (this is non-negotiable for inflation analysis)
        if level_counts.get(1, 0) != 13:
            raise ValueError(f"CRITICAL ERROR: Level 1 COICOP categories - Found {level_counts.get(1, 0)}, must be exactly 13. Core inflation categories missing.")
        
        # Warn about incomplete coverage at other levels (but don't error since we'll filter by weights)
        expected_counts = {2: 42, 3: 71, 4: 192}
        for level, expected_count in expected_counts.items():
            actual_count = level_counts.get(level, 0)
            if actual_count != expected_count:
                self.logger.warning(f"Level {level} COICOP categories: Found {actual_count}, expected {expected_count}. Will filter by weight availability.")
        
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
            else:
                # Flexible pattern for all COICOP categories (Level 1, 2, 3, 4)
                # Pattern handles: "CPI WEIGHTS XX:", "CPI WEIGHTS XX Description", and "CPI WEIGHTS XX/YY Combined categories"
                pattern = rf"CPI WEIGHTS {re.escape(coicop_code)}(?:/[0-9]+)*(?:\s*[:=-]|\s+[A-Za-z])"
                weight_col = self.find_column_by_regex(df, pattern)
                if weight_col is not None:
                    columns['CPI']['weights'][coicop_code] = weight_col
                    self.logger.debug(f"✅ Found CPI WEIGHTS {coicop_code} at position {weight_col + 1}")
                # Note: Not all categories may have weights columns, so don't error
        
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
            else:
                # Flexible pattern for all COICOP categories (Level 1, 2, 3, 4)
                # Pattern handles: "CPIH WEIGHTS XX:", "CPIH WEIGHTS XX Description", and "CPIH WEIGHTS XX/YY Combined categories"
                pattern = rf"CPIH WEIGHTS {re.escape(coicop_code)}(?:/[0-9]+)*(?:\s*[:=-]|\s+[A-Za-z])"
                weight_col = self.find_column_by_regex(df, pattern)
                if weight_col is not None:
                    columns['CPIH']['weights'][coicop_code] = weight_col
                    self.logger.debug(f"✅ Found CPIH WEIGHTS {coicop_code} at position {weight_col + 1}")
                # Note: Not all categories may have weights columns, so don't error
        
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

    def _fix_coicop_hierarchy_levels(self, coicop_codes: Dict[str, int]) -> Dict[str, int]:
        """Fix COICOP hierarchy levels: 00 should be Level 0, 01-12 should be Level 1."""
        fixed_codes = {}
        
        for code, level in coicop_codes.items():
            if code == '00':
                # 00 (ALL ITEMS) should be Level 0 - the top of the hierarchy
                fixed_codes[code] = 0
                self.logger.debug(f"Fixed {code}: Level {level} -> Level 0 (root category)")
            elif code in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                # Level 1 categories (01-12) should remain Level 1 but will have parent=00
                fixed_codes[code] = 1
            else:
                # All other categories keep their original levels
                fixed_codes[code] = level
        
        self.logger.info(f"Fixed COICOP hierarchy: 00 is now Level 0 (root), 01-12 are Level 1 with parent=00")
        return fixed_codes

    def build_coicop_hierarchy_records(self, coicop_codes: Dict[str, int], descriptions: Dict[str, str]) -> List[Dict[str, Any]]:
        """Build hierarchy records with parent-child relationships."""
        # First, fix the hierarchy: 00 should be Level 0, 01-12 should be Level 1 with parent=00
        corrected_coicop_codes = self._fix_coicop_hierarchy_levels(coicop_codes)
        
        # Then identify and create missing parent categories
        missing_parents = self._identify_missing_parents(corrected_coicop_codes)
        if missing_parents:
            self.logger.info(f"Auto-generating {len(missing_parents)} missing parent categories to complete COICOP hierarchy")
            
        # Add missing parents to the codes dictionary
        complete_coicop_codes = corrected_coicop_codes.copy()
        for parent_id, level in missing_parents.items():
            complete_coicop_codes[parent_id] = level
            self.logger.info(f"Created missing parent: {parent_id} (Level {level})")
        
        records = []
        
        for coicop_code, level in complete_coicop_codes.items():
            # Determine parent_id based on hierarchy
            parent_id = None
            if level > 0:
                # Special case: Level 1 categories (01-12) have parent 00
                if level == 1 and coicop_code != '00':
                    parent_id = '00'
                elif level > 1:
                    # Parent is the code with one less level
                    if level == 2:
                        parent_id = coicop_code.split('.')[0]  # '01.1' -> '01'
                    elif level == 3:
                        parts = coicop_code.split('.')
                        parent_id = '.'.join(parts[:2])  # '01.1.1' -> '01.1'
                    elif level == 4:
                        parts = coicop_code.split('.')
                        parent_id = '.'.join(parts[:3])  # '01.1.1.1' -> '01.1.1'
                
                # Validate that parent exists in our complete codes - should never fail now
                if parent_id and parent_id not in complete_coicop_codes:
                    raise ValueError(f"CRITICAL ERROR: COICOP hierarchy integrity violation - {coicop_code} references parent {parent_id} which does not exist even after auto-generation. This indicates a fundamental data structure issue.")
            
            # Get description from headers or use Level 1 mapping
            description = descriptions.get(coicop_code)
            if not description and coicop_code in self.level1_categories:
                description = self.level1_categories[coicop_code]
            if not description:
                if coicop_code in missing_parents:
                    # For auto-generated parents, use simple "Unknown" description
                    description = "Unknown"
                else:
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

    def _identify_missing_parents(self, coicop_codes: Dict[str, int]) -> Dict[str, int]:
        """Identify missing parent categories that need to be auto-generated (recursively)."""
        missing_parents = {}
        all_codes = coicop_codes.copy()
        
        # Keep iterating until no new missing parents are found
        found_new_missing = True
        iteration = 0
        
        while found_new_missing and iteration < 10:  # Safety limit
            found_new_missing = False
            iteration += 1
            new_missing_this_iteration = {}
            
            # Take a snapshot of codes to iterate over (avoid modification during iteration)
            codes_to_check = all_codes.copy()
            
            for coicop_code, level in codes_to_check.items():
                if level > 1:
                    # Determine what the parent should be
                    if level == 2:
                        parent_id = coicop_code.split('.')[0]
                    elif level == 3:
                        parts = coicop_code.split('.')
                        parent_id = '.'.join(parts[:2])
                    elif level == 4:
                        parts = coicop_code.split('.')
                        parent_id = '.'.join(parts[:3])
                    else:
                        continue
                    
                    # Check if parent exists in current working set
                    if parent_id and parent_id not in all_codes:
                        # Calculate parent level
                        parent_level = level - 1
                        new_missing_this_iteration[parent_id] = parent_level
                        self.logger.debug(f"Iteration {iteration}: Identified missing parent: {parent_id} (Level {parent_level}) for child {coicop_code}")
            
            # Add new missing parents to both tracking dicts
            if new_missing_this_iteration:
                missing_parents.update(new_missing_this_iteration)
                all_codes.update(new_missing_this_iteration)
                found_new_missing = True
        
        if iteration >= 10:
            self.logger.warning("Missing parent detection reached maximum iterations - possible infinite loop in hierarchy")
        
        return missing_parents

    def _calculate_parent_weights(self, weights_by_series: Dict[str, Dict[str, Dict[int, float]]], hierarchy_records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[int, float]]]:
        """Calculate weights for auto-generated parent categories by summing children's weights."""
        # Build parent-child mapping
        children_by_parent = {}
        for record in hierarchy_records:
            parent_id = record.get('parent_id')
            if parent_id:
                if parent_id not in children_by_parent:
                    children_by_parent[parent_id] = []
                children_by_parent[parent_id].append(record['coicop_id'])
        
        # Calculate weights for each series type
        for series_type in ['CPI', 'CPIH']:
            if series_type not in weights_by_series:
                continue
                
            # Find categories that need weight calculation (auto-generated parents)
            existing_categories = set(weights_by_series[series_type].keys())
            all_categories = {record['coicop_id'] for record in hierarchy_records}
            missing_weight_categories = all_categories - existing_categories
            
            # Calculate weights for missing categories that have children
            for parent_id in missing_weight_categories:
                if parent_id in children_by_parent:
                    children = children_by_parent[parent_id]
                    parent_weights = {}
                    
                    # For each year, sum children's weights
                    all_years = set()
                    for child_id in children:
                        if child_id in weights_by_series[series_type]:
                            all_years.update(weights_by_series[series_type][child_id].keys())
                    
                    for year in all_years:
                        total_weight = 0.0
                        children_found = 0
                        
                        for child_id in children:
                            if child_id in weights_by_series[series_type]:
                                child_weights = weights_by_series[series_type][child_id]
                                if year in child_weights:
                                    total_weight += child_weights[year]
                                    children_found += 1
                        
                        # Only set parent weight if we found weights for at least one child
                        if children_found > 0:
                            parent_weights[year] = total_weight
                    
                    if parent_weights:
                        weights_by_series[series_type][parent_id] = parent_weights
                        self.logger.info(f"Calculated {series_type} weights for auto-generated parent {parent_id}: {len(parent_weights)} years, max weight = {max(parent_weights.values()):.2f}")
        
        return weights_by_series

    def _calculate_missing_weights_from_children(self, columns: Dict[str, Dict[str, Dict[str, int]]], hierarchy_records: List[Dict[str, Any]], df: pd.DataFrame) -> Dict[str, Dict[str, Dict[int, float]]]:
        """Calculate missing weights from children's weights, including auto-generated parents."""
        
        # Step 1: Parse weights for categories that have weight columns
        weights_by_series = {}
        for series_type in ['CPI', 'CPIH']:
            weights_by_series[series_type] = {}
            for cat, weight_col in columns[series_type]['weights'].items():
                if weight_col is not None:
                    weights = self.parse_weight_data(df, weight_col, f"{series_type} {cat}")
                    if weights:  # Only store if we got actual weight data
                        weights_by_series[series_type][cat] = weights
        
        # Step 2: Calculate missing weights from children using existing logic
        weights_by_series = self._calculate_parent_weights(weights_by_series, hierarchy_records)
        
        # Step 3: Validate that every category now has weights (either direct or calculated)
        for series_type in ['CPI', 'CPIH']:
            categories_with_indices = set(columns[series_type]['indices'].keys())
            categories_with_weights = set(weights_by_series[series_type].keys())
            
            still_missing = categories_with_indices - categories_with_weights
            if still_missing:
                # These categories have index data but no weights and no children with weights
                self.logger.error(f"{series_type}: {len(still_missing)} categories cannot be used - no weights available")
                sample_missing = list(still_missing)[:5]
                raise ValueError(f"CRITICAL ERROR: {series_type} categories have index data but no weights could be calculated: {sample_missing}. "
                               f"These categories have no weight columns and no children with weights for weight calculation.")
            
            self.logger.info(f"{series_type}: All {len(categories_with_indices)} categories have weights (direct or calculated)")
        
        return weights_by_series

    def _validate_weight_columns_exist(self, columns: Dict[str, Dict[str, Dict[str, int]]], coicop_codes: Dict[str, int]) -> None:
        """Validate that every category with index data has a corresponding weight column."""
        for series_type in ['CPI', 'CPIH']:
            categories_with_indices = set(columns[series_type]['indices'].keys())
            categories_with_weights = set(columns[series_type]['weights'].keys())
            
            missing_weights = categories_with_indices - categories_with_weights
            if missing_weights:
                # Separate by level for clearer error messages
                missing_by_level = {}
                for cat in missing_weights:
                    level = coicop_codes.get(cat, 'Unknown')
                    if level not in missing_by_level:
                        missing_by_level[level] = []
                    missing_by_level[level].append(cat)
                
                error_details = []
                for level in sorted(missing_by_level.keys()):
                    cats = missing_by_level[level]
                    error_details.append(f"Level {level}: {len(cats)} categories ({cats[:5]}{'...' if len(cats) > 5 else ''})")
                
                raise ValueError(f"CRITICAL ERROR: {series_type} categories have index data but no weight columns. "
                               f"Every category must have weights for contribution analysis. "
                               f"Missing weight columns: {'; '.join(error_details)}")
            
            self.logger.info(f"{series_type}: All {len(categories_with_indices)} categories have both index and weight columns")

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
            
            # Sort hierarchy records by level to ensure parents are inserted before children
            hierarchy_records = sorted(hierarchy_records, key=lambda x: x['level'])
            
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
            
            # Extract all COICOP codes for validation
            coicop_codes = self.extract_all_coicop_codes(df)
            descriptions = self.extract_coicop_descriptions_from_headers(df)
            hierarchy_records = self.build_coicop_hierarchy_records(coicop_codes, descriptions)
            
            # Calculate missing weights from children instead of erroring
            weights_by_series = self._calculate_missing_weights_from_children(columns, hierarchy_records, df)
            
            # Extract monthly data rows
            monthly_rows = self.extract_monthly_data(df)
            
            # weights_by_series is now calculated above in _calculate_missing_weights_from_children
            
            # Get database connection
            conn = self.get_db_connection()
            
            # Populate COICOP hierarchy
            hierarchy_records_count = self.populate_coicop_hierarchy(conn, csv_file_path)
            
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
                                    # Get weight value - REQUIRED for CPI/CPIH categories
                                    weight_val = None
                                    if (series_type in weights_by_series and 
                                        cat in weights_by_series[series_type] and
                                        year_int in weights_by_series[series_type][cat]):
                                        weight_val = weights_by_series[series_type][cat][year_int]
                                    
                                    # Note: weight_val may be None for some years (e.g., pre-2015 data)
                                    # This is acceptable - we store records with available weights only
                                    
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