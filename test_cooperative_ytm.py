#!/usr/bin/env python3
"""
Test script to manually call the corporate bond collector and check
the Co-operative Group bond YTM calculation.
"""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, '/Users/cwilkin/Documents/Development/repos/econometrics')

from data_collectors.gilt_market_data import collect_corporate_bond_prices

def test_cooperative_bond():
    """Test the corporate bond collector and look for Co-operative Group bond."""
    print("Testing Corporate Bond Collector - Co-operative Group YTM")
    print("=" * 60)
    
    try:
        print("Calling collect_corporate_bond_prices()...")
        
        # This will scrape the corporate bonds page and return the data
        from data_collectors.gilt_market_data import CorporateBondCollector
        collector = CorporateBondCollector(database_url=None)
        bonds = collector.scrape_corporate_bond_prices()
        
        print(f"Successfully collected {len(bonds)} corporate bonds")
        print()
        
        # Look for Co-operative Group bond specifically
        cooperative_bonds = []
        for bond in bonds:
            if 'co-operative' in bond.get('company_name', '').lower() or \
               'co-operative' in bond.get('bond_name', '').lower():
                cooperative_bonds.append(bond)
        
        if cooperative_bonds:
            print(f"Found {len(cooperative_bonds)} Co-operative Group bond(s):")
            print("-" * 60)
            
            for i, bond in enumerate(cooperative_bonds, 1):
                print(f"Bond {i}:")
                print(f"  Company: {bond.get('company_name', 'N/A')}")
                print(f"  Bond Name: {bond.get('bond_name', 'N/A')}")
                print(f"  Combined ID: {bond.get('combined_id', 'N/A')}")
                print(f"  Clean Price: £{bond.get('clean_price', 'N/A')}")
                print(f"  Dirty Price: £{bond.get('dirty_price', 'N/A')}")
                print(f"  Coupon Rate: {bond.get('coupon_rate', 'N/A')*100:.3f}%")
                print(f"  Years to Maturity: {bond.get('years_to_maturity', 'N/A'):.2f}")
                print(f"  YTM: {bond.get('ytm', 'N/A')*100:.3f}%")
                print(f"  After-tax YTM: {bond.get('after_tax_ytm', 'N/A')*100:.3f}%")
                print(f"  Maturity Date: {bond.get('maturity_date', 'N/A')}")
                print()
                
                # Manual verification calculation
                if all(key in bond for key in ['clean_price', 'coupon_rate', 'years_to_maturity']):
                    clean_price = bond['clean_price']
                    coupon_rate = bond['coupon_rate']
                    years_to_maturity = bond['years_to_maturity']
                    
                    # Simple approximation formula for comparison
                    approx_ytm = (coupon_rate + (100 - clean_price) / years_to_maturity) / ((100 + clean_price) / 2)
                    
                    print(f"  Manual approximation YTM: {approx_ytm*100:.3f}%")
                    print(f"  Expected vs Actual difference: {abs(approx_ytm - bond.get('ytm', 0))*100:.3f} percentage points")
                    print()
        else:
            print("❌ No Co-operative Group bonds found!")
            print()
            print("All bonds found:")
            for i, bond in enumerate(bonds[:10], 1):  # Show first 10 bonds
                company = bond.get('company_name', 'Unknown')
                bond_name = bond.get('bond_name', 'Unknown')
                ytm = bond.get('ytm', 0) * 100 if bond.get('ytm') else 'N/A'
                print(f"  {i}. {company} - YTM: {ytm}%")
            
            if len(bonds) > 10:
                print(f"  ... and {len(bonds) - 10} more bonds")
    
    except Exception as e:
        print(f"❌ Error during collection: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = test_cooperative_bond()
    if success:
        print("✅ Test completed successfully")
    else:
        print("❌ Test failed")