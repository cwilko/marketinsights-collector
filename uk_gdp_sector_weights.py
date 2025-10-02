#!/usr/bin/env python3
"""
UK GDP Sector Weights - Standard ONS Classification Weights

These are approximate weights based on ONS data structure for GDP sector contributions.
The exact weights vary over time, but these provide a baseline for validation.

Source: ONS National Accounts and GDP methodology
"""

# Standard UK GDP sector weights (approximate percentages)
UK_GDP_SECTOR_WEIGHTS = {
    "A": 0.7,      # Agriculture, forestry and fishing (~0.7% of GDP)
    "B--E": 19.0,  # Production industries (Mining, Manufacturing, Electricity, Water) (~19% of GDP)  
    "F": 6.5,      # Construction (~6.5% of GDP)
    "G--T": 73.8,  # Services (Trade, Transport, Accommodation, etc.) (~73.8% of GDP)
    "A--T": 100.0  # Total GDP (All sectors) - should equal sum of components
}

def validate_uk_gdp_weights(weights: dict) -> bool:
    """Validate that GDP sector weights sum to approximately 100%."""
    component_sectors = ["A", "B--E", "F", "G--T"]
    total = sum(weights.get(sector, 0) for sector in component_sectors)
    
    print(f"Sector weight validation:")
    for sector in component_sectors:
        print(f"  {sector}: {weights.get(sector, 0):.1f}%")
    print(f"  Total: {total:.1f}%")
    
    # Should sum to approximately 100%
    is_valid = 95.0 <= total <= 105.0
    print(f"  Valid: {'✅' if is_valid else '❌'}")
    
    return is_valid

def calculate_sector_contributions(sector_growth_rates: dict, weights: dict = None) -> dict:
    """Calculate each sector's contribution to overall GDP growth.
    
    Args:
        sector_growth_rates: Dict with growth rates for each sector (A, B--E, F, G--T)
        weights: Optional custom weights (defaults to UK_GDP_SECTOR_WEIGHTS)
    
    Returns:
        Dict with contribution of each sector to total GDP growth
    """
    if weights is None:
        weights = UK_GDP_SECTOR_WEIGHTS
    
    contributions = {}
    total_contribution = 0
    
    for sector in ["A", "B--E", "F", "G--T"]:
        if sector in sector_growth_rates and sector in weights:
            # Contribution = (sector_weight / 100) * sector_growth_rate
            contribution = (weights[sector] / 100.0) * sector_growth_rates[sector]
            contributions[sector] = contribution
            total_contribution += contribution
    
    contributions["A--T"] = total_contribution  # Total GDP growth
    
    return contributions

def example_calculation():
    """Example of how sector contributions work."""
    print("Example: UK GDP Sector Contribution Calculation")
    print("=" * 50)
    
    # Example quarterly growth rates (annualized %)
    example_growth_rates = {
        "A": 1.2,      # Agriculture grows 1.2%
        "B--E": -0.5,  # Production declines 0.5%  
        "F": 2.0,      # Construction grows 2.0%
        "G--T": 1.8    # Services grow 1.8%
    }
    
    print("Sector Growth Rates:")
    for sector, rate in example_growth_rates.items():
        print(f"  {sector}: {rate:+.1f}%")
    
    print("\nSector Weights:")
    validate_uk_gdp_weights(UK_GDP_SECTOR_WEIGHTS)
    
    print("\nSector Contributions to GDP Growth:")
    contributions = calculate_sector_contributions(example_growth_rates)
    
    for sector in ["A", "B--E", "F", "G--T"]:
        if sector in contributions:
            print(f"  {sector}: {contributions[sector]:+.2f} percentage points")
    
    print(f"\nTotal GDP Growth: {contributions['A--T']:+.2f}%")
    
    # Verification
    manual_total = sum(contributions[s] for s in ["A", "B--E", "F", "G--T"])
    print(f"Manual verification: {manual_total:+.2f}%")

if __name__ == "__main__":
    validate_uk_gdp_weights(UK_GDP_SECTOR_WEIGHTS)
    print()
    example_calculation()