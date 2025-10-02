#!/usr/bin/env python3
"""
Prototype script to download and parse GDP sector weights from ONS Excel file.

The weights show how each sector (A, B--E, F, G--T) combines to form the overall A--T GDP growth rate.
"""

import pandas as pd
import requests
import logging
from typing import Dict, Optional
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_ons_gdp_weights_file(url: str, local_path: str = "mgdpdatasourcescatalogue.xlsx") -> str:
    """Download the ONS GDP weights Excel file."""
    logger.info(f"Downloading GDP weights file from: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Successfully downloaded {len(response.content)} bytes to {local_path}")
        return local_path
        
    except Exception as e:
        logger.error(f"Failed to download GDP weights file: {str(e)}")
        raise

def explore_excel_sheets(file_path: str) -> Dict[str, pd.DataFrame]:
    """Explore all sheets in the Excel file to understand structure."""
    logger.info(f"Exploring Excel file structure: {file_path}")
    
    try:
        # Read all sheets with different header strategies
        all_sheets = {}
        
        # Try reading with different header rows to handle multi-row headers
        for header_row in [0, 1, 2, 3]:
            try:
                sheets_attempt = pd.read_excel(file_path, sheet_name=None, engine='openpyxl', header=header_row)
                for sheet_name, df in sheets_attempt.items():
                    if sheet_name not in all_sheets:
                        all_sheets[sheet_name] = df
                        
                        # Check if this header row gives us better column names
                        cols_text = ' '.join([str(col).lower() for col in df.columns])
                        if 'section weight' in cols_text or 'sic 2007' in cols_text:
                            logger.info(f"Found weight-related columns in {sheet_name} with header row {header_row}")
                            all_sheets[f"{sheet_name}_header_{header_row}"] = df
                            
            except Exception:
                continue
        
        logger.info(f"Found {len(all_sheets)} sheet variants:")
        for sheet_name, df in all_sheets.items():
            if df.empty:
                continue
                
            logger.info(f"  - {sheet_name}: {df.shape[0]} rows x {df.shape[1]} columns")
            
            # Show first few column names
            cols = list(df.columns)[:8]  # First 8 columns
            logger.info(f"    Sample columns: {cols}")
            
            # Look for weight-related keywords in column names
            weight_cols = [col for col in df.columns if any(keyword in str(col).lower() 
                          for keyword in ['weight', 'contribution', 'share', 'proportion'])]
            if weight_cols:
                logger.info(f"    Weight-related columns: {weight_cols}")
            
            # Look for sector-related keywords  
            sector_cols = [col for col in df.columns if any(keyword in str(col).lower()
                          for keyword in ['sector', 'industry', 'classification', 'sic'])]
            if sector_cols:
                logger.info(f"    Sector-related columns: {sector_cols}")
            
            # Look in cell data for weight/sic keywords
            df_str = df.astype(str).values.flatten()
            text_content = ' '.join(df_str).lower()
            if 'section weight' in text_content:
                logger.info(f"    Found 'section weight' in cell data")
            if 'sic 2007' in text_content:
                logger.info(f"    Found 'sic 2007' in cell data")
        
        return all_sheets
        
    except Exception as e:
        logger.error(f"Failed to explore Excel file: {str(e)}")
        raise

def find_gdp_weights_data(all_sheets: Dict[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Find the sheet/data containing GDP sector weights."""
    logger.info("Searching for GDP sector weights data...")
    
    # First priority: sheets with proper column headers containing "Section weight" or "SIC 2007"
    for sheet_name, df in all_sheets.items():
        if df.empty:
            continue
            
        cols_text = ' '.join([str(col).lower() for col in df.columns])
        if 'section weight' in cols_text or 'sic 2007' in cols_text:
            logger.info(f"Found sheet with proper weight columns: {sheet_name}")
            return df
    
    # Second priority: sheets containing the text "section weight" in data
    for sheet_name, df in all_sheets.items():
        if df.empty:
            continue
            
        df_str = df.astype(str).values.flatten()
        text_content = ' '.join(df_str).lower()
        if 'section weight' in text_content and 'sic 2007' in text_content:
            logger.info(f"Found sheet with weight data in cells: {sheet_name}")
            return df
    
    # Fallback to specific target sheets
    target_sheets = [
        "BB19 - Currentprice  & Volume",
        "BB24 - CURRENT PRICE & VOLUME", 
        "BB19 - CURRENT PRICE & VOLUME"
    ]
    
    for target_sheet in target_sheets:
        if target_sheet in all_sheets:
            logger.info(f"Fallback to target sheet: {target_sheet}")
            return all_sheets[target_sheet]
    
    logger.warning("Could not find sheet with section weights")
    return None

def parse_sector_weights(df: pd.DataFrame) -> Dict[str, float]:
    """Parse sector weights from the DataFrame using 'Level', 'Section', and 'weight' columns."""
    logger.info("Parsing sector weights from 'Level', 'Section', and 'weight' columns...")
    
    # Find the 'Level', 'Section', and 'weight' columns
    level_col = None
    section_col = None
    weight_col = None
    
    # First try to find in column headers
    for col in df.columns:
        col_str = str(col).lower().strip()
        if col_str == 'level':
            level_col = col
            logger.info(f"Found 'Level' column: {col}")
        elif col_str == 'section':
            section_col = col
            logger.info(f"Found 'Section' column: {col}")
        elif col_str == 'weight':
            weight_col = col
            logger.info(f"Found 'weight' column: {col}")
    
    # If not found in headers, search for header row in the data
    if level_col is None or section_col is None or weight_col is None:
        logger.info("Required columns not in headers, searching data for header row...")
        
        for i, row in df.iterrows():
            if i > 20:  # Don't search too far down
                break
                
            row_values = [str(cell).lower().strip() for cell in row if pd.notna(cell)]
            if 'level' in row_values and 'section' in row_values and 'weight' in row_values:
                logger.info(f"Found header row at index {i}")
                
                # Use this row as column headers
                for j, cell in enumerate(row):
                    cell_str = str(cell).lower().strip()
                    if cell_str == 'level':
                        level_col = j
                        logger.info(f"Found Level at column {j}")
                    elif cell_str == 'section':
                        section_col = j
                        logger.info(f"Found Section at column {j}")
                    elif cell_str == 'weight':
                        weight_col = j
                        logger.info(f"Found weight at column {j}")
                
                # Use data starting from the next row
                df = df.iloc[i+1:].reset_index(drop=True)
                break
    
    if level_col is None or section_col is None or weight_col is None:
        logger.warning(f"Could not find required columns. Available columns: {list(df.columns)}")
        return {}
    
    # Extract individual section weights where Level = "Section"
    section_weights = {}
    
    logger.info("Extracting weights where Level = 'Section'...")
    
    if isinstance(level_col, int) and isinstance(section_col, int) and isinstance(weight_col, int):
        # Using column indices
        for i, row in df.iterrows():
            try:
                level_value = row.iloc[level_col] if level_col < len(row) else None
                section_value = row.iloc[section_col] if section_col < len(row) else None
                weight_value = row.iloc[weight_col] if weight_col < len(row) else None
                
                if (pd.notna(level_value) and pd.notna(section_value) and pd.notna(weight_value) and
                    str(level_value).strip().lower() == 'section'):
                    
                    section_str = str(section_value).strip().upper()
                    weight_float = float(weight_value)
                    
                    # Check if section is single letter A-T
                    if len(section_str) == 1 and section_str.isalpha() and 'A' <= section_str <= 'T':
                        section_weights[section_str] = weight_float
                        logger.info(f"Section {section_str}: {weight_float}")
                        
            except (ValueError, TypeError, IndexError):
                continue
    else:
        # Using column names  
        for i, row in df.iterrows():
            try:
                level_value = row[level_col] if level_col else None
                section_value = row[section_col] if section_col else None
                weight_value = row[weight_col] if weight_col else None
                
                if (pd.notna(level_value) and pd.notna(section_value) and pd.notna(weight_value) and
                    str(level_value).strip().lower() == 'section'):
                    
                    section_str = str(section_value).strip().upper()
                    weight_float = float(weight_value)
                    
                    # Check if section is single letter A-T
                    if len(section_str) == 1 and section_str.isalpha() and 'A' <= section_str <= 'T':
                        section_weights[section_str] = weight_float
                        logger.info(f"Section {section_str}: {weight_float}")
                        
            except (ValueError, TypeError, KeyError):
                continue
    
    logger.info(f"Found {len(section_weights)} individual section weights")
    
    # Verify total sums to 1000
    total_weight = sum(section_weights.values())
    logger.info(f"Total individual section weights: {total_weight} (should be 1000)")
    
    return calculate_aggregate_weights(section_weights)

def calculate_aggregate_weights(section_weights: Dict[str, float]) -> Dict[str, float]:
    """Calculate aggregate weights from individual section weights."""
    logger.info(f"Calculating aggregate weights from {len(section_weights)} individual sections")
    
    aggregate_weights = {}
    
    # A sector (just section A)
    if 'A' in section_weights:
        aggregate_weights['A'] = section_weights['A']
        logger.info(f"A sector weight: {aggregate_weights['A']}")
    
    # B--E sector (sum sections B, C, D, E)
    b_to_e_sections = ['B', 'C', 'D', 'E']
    b_to_e_total = sum(section_weights.get(sec, 0) for sec in b_to_e_sections)
    if b_to_e_total > 0:
        aggregate_weights['B--E'] = b_to_e_total
        logger.info(f"B--E sector weight: {b_to_e_total} (sum of {b_to_e_sections})")
    
    # F sector (just section F)
    if 'F' in section_weights:
        aggregate_weights['F'] = section_weights['F']
        logger.info(f"F sector weight: {aggregate_weights['F']}")
    
    # G--T sector (sum sections G through T)
    g_to_t_sections = ['G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T']
    g_to_t_total = sum(section_weights.get(sec, 0) for sec in g_to_t_sections)
    if g_to_t_total > 0:
        aggregate_weights['G--T'] = g_to_t_total
        logger.info(f"G--T sector weight: {g_to_t_total} (sum of {g_to_t_sections})")
    
    # A--T sector (sum all sections A through T)
    all_sections = ['A'] + b_to_e_sections + ['F'] + g_to_t_sections
    a_to_t_total = sum(section_weights.get(sec, 0) for sec in all_sections)
    if a_to_t_total > 0:
        aggregate_weights['A--T'] = a_to_t_total
        logger.info(f"A--T sector weight: {a_to_t_total} (sum of all sections)")
    
    return aggregate_weights

def validate_weights(weights: Dict[str, float]) -> bool:
    """Validate that sector weights sum to approximately 1000."""
    if not weights:
        return False
    
    # Exclude A--T as it's the total, not a component
    component_sectors = ['A', 'B--E', 'F', 'G--T']
    component_weights = [weights.get(sector) for sector in component_sectors if sector in weights]
    
    if not component_weights:
        return False
    
    total_weight = sum(component_weights)
    logger.info(f"Component sectors total weight: {total_weight}")
    
    # Check if weights sum to ~1000 (as specified by user)
    is_valid = (990 <= total_weight <= 1010)
    
    if is_valid:
        logger.info("✅ Weights validation passed")
    else:
        logger.warning(f"⚠️ Weights validation failed - total: {total_weight} (should be ~1000)")
    
    return is_valid

def main():
    """Main function to download and parse GDP sector weights."""
    url = "https://www.ons.gov.uk/file?uri=/economy/grossdomesticproductgdp/datasets/gdpodatasourcescatalogue/current/mgdpdatasourcescatalogue.xlsx"
    
    try:
        # Download the file
        file_path = download_ons_gdp_weights_file(url)
        
        # Explore all sheets
        all_sheets = explore_excel_sheets(file_path)
        
        # Find the weights data
        weights_df = find_gdp_weights_data(all_sheets)
        
        if weights_df is not None:
            # Parse sector weights
            weights = parse_sector_weights(weights_df)
            
            # Validate weights
            validate_weights(weights)
            
            # Display results
            print("\n" + "="*50)
            print("GDP SECTOR WEIGHTS ANALYSIS")
            print("="*50)
            
            if weights:
                print("\nExtracted Sector Weights:")
                for sector, weight in weights.items():
                    print(f"  {sector}: {weight}")
                    
                # Calculate component breakdown if we have A--T total
                if 'A--T' in weights:
                    total_gdp = weights['A--T']
                    print(f"\nSector Contributions to A--T GDP ({total_gdp}):")
                    for sector in ['A', 'B--E', 'F', 'G--T']:
                        if sector in weights:
                            contribution = (weights[sector] / total_gdp) * 100 if total_gdp != 0 else 0
                            print(f"  {sector}: {contribution:.1f}%")
            else:
                print("\n❌ No sector weights could be extracted")
                print("Manual inspection of the Excel file may be required")
        else:
            print("\n❌ Could not locate GDP weights data in the Excel file")
            print("Available sheets:", list(all_sheets.keys()))
        
        # Cleanup
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up temporary file: {file_path}")
            
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()