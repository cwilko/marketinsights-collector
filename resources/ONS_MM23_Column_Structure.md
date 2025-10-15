# UK ONS MM23 Dataset - Column Structure Analysis

## Overview
The UK Office for National Statistics (ONS) MM23 dataset contains **4,054 columns** of economic indicators and price data. The dataset follows a hierarchical numbering system for inflation indices and includes various types of measurements across different economic sectors.

## Column Categories

### 1. Consumer Price Index (CPI) - 464 columns total

#### CPI Weights - Two Distinct Systems:

**CPI wts: (49 columns, positions 2-50)**
- **Purpose**: High-level aggregate categories for broad economic analysis
- **Structure**: Non-hierarchical, descriptive names
- **Examples**: 
  - "Non-energy industrial goods GOODS"
  - "Durables GOODS"
  - "Processed food & non-alcoholic beverages GOODS"
- **Use case**: Policy analysis and understanding broad economic trends

**CPI WEIGHTS (322 columns, positions 292-3915)**
- **Purpose**: Detailed item-by-item weights matching the full COICOP hierarchy
- **Structure**: Hierarchical numbering system identical to CPI INDEX structure
- **Coverage**: Exact 1:1 correspondence with numbered CPI INDEX categories
- **Examples**:
  - "00 : HICP (Overall index)"
  - "01.1.1 : BREAD & CEREALS"
  - "01.1.1.1 Rice"
- **Use case**: Technical CPI calculation, detailed sectoral analysis

#### CPI Index Values (371 columns)
- **Hierarchical numbered indices (316 columns)**: Follow COICOP classification (00-12.x.x.x)
- **Special aggregate indices (55 columns)**: Non-hierarchical categories including:
  - Goods vs Services splits ("Goods 2015=100", "Services 2015=100")
  - Economic aggregates ("Non-energy industrial goods", "Durables")
  - Special exclusions ("Excluding tobacco", "Excluding energy")
  - Housing sub-types ("Private rentals", "Local authority rentals")
- Base year: 2015=100

**Key Insight**: The 322 CPI WEIGHTS columns correspond exactly to the 316 numbered CPI INDEX categories plus 6 additional weight categories, but there are NO weights for the 55 special aggregate indices. This is because special aggregates are calculated combinations of the base hierarchical categories, so they don't need separate weights.

- **Hierarchical Structure (316 categories with matching weights):**
  - **Level 1 (13 items)**: Main categories (00-12)
    - `00`: ALL ITEMS
    - `01`: FOOD AND NON-ALCOHOLIC BEVERAGES  
    - `02`: ALCOHOLIC BEVERAGES, TOBACCO & NARCOTICS
    - `03`: CLOTHING AND FOOTWEAR
    - `04`: HOUSING, WATER AND FUELS
    - `05`: FURNITURE, HOUSEHOLD EQUIPMENT & ROUTINE REPAIR
    - `06`: HEALTH
    - `07`: TRANSPORT
    - `08`: COMMUNICATION
    - `09`: RECREATION & CULTURE
    - `10`: EDUCATION
    - `11`: RESTAURANTS & HOTELS
    - `12`: MISCELLANEOUS GOODS & SERVICES

  - **Level 2 (40 items)**: Subcategories (e.g., 01.1, 01.2)
  - **Level 3 (71 items)**: Sub-subcategories (e.g., 01.1.1, 01.1.2)
  - **Level 4 (192 items)**: Detailed categories (e.g., 01.1.1.1, 01.1.1.2)
  - **Further levels**: Up to 6 decimal places for very specific items

#### CPI Percentage Changes (16 columns)
- Month-over-month and year-over-year percentage changes
- Includes CPI-CT (Core CPI) and CPIY variations

#### CPI Contribution Points (28 columns)
- Point contributions to overall inflation rate changes
- Format: `CPI: % points change over previous month (12 month rate): [Category]`

### 2. Consumer Price Index Including Owner Occupiers' Housing (CPIH) - 1,539 columns
- **Largest category** in the dataset
- Similar hierarchical structure to CPI but includes housing costs
- **Key difference**: Includes category `04.2` for Owner Occupiers' Housing Costs
- Same 4-level hierarchy as CPI with identical numbering system
- Additional variations: excluding different components (tobacco, council tax, etc.)

### 3. Retail Price Index (RPI) - 813 columns total
#### RPI Index Values (3 columns)
- Main RPI indices with different base periods
- `RPI All Items Index: Jan 1987=100` (our target column)
- `RPI All Items Index Excl Mortgage Interest (RPIX): Jan 1987=100`

#### RPI Percentage Changes (255 columns)
- Year-over-year and month-over-month changes for detailed categories
- Format: `RPI:Percentage change over [period] - [Item]`

#### RPI Average Prices (61 columns)
- Actual price levels for specific goods
- Examples: Petrol per litre, bread per loaf, cigarettes per pack
- Format: `RPI: Ave price - [Item], per [Unit]`

#### Other RPI Data (494 columns)
- Weights for different household types
- Special calculations and adjustments

### 4. Internal Purchasing Power (30 columns)
- Purchasing power of the pound relative to different base years
- Base years from 1977 to 2009
- Format: `Internal purchasing power of the pound (based on RPI): [Year]=100`

### 5. Other Categories (1,208 columns)
- Long-run historical series
- Contribution calculations
- Weight calculations
- Special aggregates and derived measures

## Hierarchical Numbering System

The CPI and CPIH indices follow the **COICOP** (Classification of Individual Consumption by Purpose) international standard:

### Level 1: Major Groups (00-12)
```
00 - All Items (Headline rate)
01 - Food and Non-alcoholic Beverages
02 - Alcoholic Beverages, Tobacco & Narcotics  
03 - Clothing and Footwear
04 - Housing, Water, Electricity, Gas and Other Fuels
05 - Furniture, Household Equipment & Routine Repair
06 - Health
07 - Transport
08 - Communication
09 - Recreation & Culture
10 - Education
11 - Restaurants & Hotels
12 - Miscellaneous Goods & Services
```

### Level 2: Groups (XX.X)
Example for Food (01):
- `01.1` - Food
- `01.2` - Non-alcoholic Beverages

### Level 3: Classes (XX.X.X)
Example for Food (01.1):
- `01.1.1` - Bread & Cereals
- `01.1.2` - Meat
- `01.1.3` - Fish
- `01.1.4` - Milk, Cheese & Eggs
- etc.

### Level 4: Subclasses (XX.X.X.X)
Example for Bread & Cereals (01.1.1):
- `01.1.1.1` - Rice
- `01.1.1.2` - Flours and other cereals
- `01.1.1.3` - Bread
- `01.1.1.4` - Other bakery products
- etc.

## Key Target Columns

For UK inflation analysis, the main columns of interest are:
- **Column 756**: `CPI INDEX 00: ALL ITEMS 2015=100`
- **Column 2407**: `CPIH INDEX 00: ALL ITEMS 2015=100` 
- **Column 254**: `RPI All Items Index: Jan 1987=100`

## Data Quality Notes

- **Base years vary**: CPI/CPIH use 2015=100, RPI uses Jan 1987=100
- **Coverage differences**: CPIH includes owner-occupier housing costs, CPI does not
- **Historical availability**: RPI data starts 1987, CPI data starts 1988, CPIH data starts 1988
- **Update frequency**: All series are monthly
- **Seasonal adjustment**: Most series are not seasonally adjusted

## Usage Recommendations

1. **For headline inflation**: Use CPIH 00 (most comprehensive)
2. **For international comparisons**: Use CPI 00 (harmonized standard)
3. **For historical analysis**: Use RPI (longest time series)
4. **For sectoral analysis**: Use the detailed hierarchical categories
5. **For policy analysis**: Consider both headline and core measures (excluding volatile items)

This hierarchical structure allows for detailed analysis at multiple levels of aggregation, from overall inflation down to specific product categories.

## Understanding the Weight Systems

The ONS provides **two complementary weighting approaches**:

### 1. **CPI wts:** - Aggregate Policy Weights
- **When to use**: Broad economic analysis, policy discussions
- **Advantage**: Simpler categories aligned with economic concepts
- **Example**: Analyzing impact of "energy" vs "non-energy" inflation

### 2. **CPI WEIGHTS** - Technical Statistical Weights  
- **When to use**: Detailed CPI construction, technical analysis
- **Advantage**: Exact correspondence with price index structure
- **Example**: Understanding contribution of "bread" vs "rice" to overall inflation

### 3. **Why No Weights for Special Aggregates?**
The 55 special aggregate indices (like "Goods", "Services", "Excluding energy") don't have corresponding weights because they are **derived calculations** from the base 316 hierarchical categories. For example:
- "CPI INDEX: Goods" = weighted combination of all goods categories (01.1.1.x, 03.1.1.x, etc.)
- "CPI INDEX: Excluding energy" = total CPI minus energy components (04.5.x categories)

The weights for these aggregates are implicitly determined by the weights of their underlying components, making separate weight columns redundant.

---

## Complete CPI/CPIH Hierarchical Structure (COICOP Classification)

This is the complete 4-level hierarchy extracted from the ONS MM23 dataset column headers.
CPI INDEX and CPIH INDEX follow nearly identical structures with a few key differences:

**Hierarchy Totals:**
- **CPI INDEX**: 316 hierarchical categories
- **CPIH INDEX**: 317 hierarchical categories (+1)

**Key Differences:**
- **CPIH includes**: 04.2 (Owner Occupiers Housing Costs) and 04.9 (Council Tax and rates)
- **CPI includes**: 09.2.1 (Major durables for recreation - classification difference)

The hierarchy below shows the CPI structure, which forms the base for both indices.

### Level 1: Major Groups (13 categories)

- **00**: ALL ITEMS
- **01**: FOOD AND NON-ALCOHOLIC BEVERAGES
- **02**: ALCOHOLIC BEVERAGES,TOBACCO & NARCOTICS
- **03**: CLOTHING AND FOOTWEAR
- **04**: HOUSING, WATER AND FUELS
- **05**: FURN, HH EQUIP & ROUTINE REPAIR OF HOUSE
- **06**: HEALTH
- **07**: TRANSPORT
- **08**: COMMUNICATION
- **09**: RECREATION & CULTURE
- **10**: EDUCATION
- **11**: HOTELS, CAFES AND RESTAURANTS
- **12**: MISCELLANEOUS GOODS AND SERVICES

### Level 2: Groups (40 categories)

- **01.1**: FOOD
- **01.2**: NON-ALCOHOLIC BEVERAGES
- **02.1**: ALCOHOLIC BEVERAGES
- **02.2**: TOBACCO
- **03.1**: CLOTHING
- **03.2**: FOOTWEAR INCLUDING REPAIRS
- **04.1**: ACTUAL RENTS FOR HOUSING
- **04.3**: REGULAR MAINTENANCE AND REPAIR OF THE DWELLING
- **04.4**: Water supply and misc. services for the dwelling
- **04.5**: ELECTRICITY, GAS AND OTHER FUELS
- **05.1**: Furniture, furnishings and carpets
- **05.2**: HOUSEHOLD TEXTILES
- **05.3**: Household appliances, fitting and repairs
- **05.4**: GLASSWARE, TABLEWARE AND HOUSEHOLD UTENSILS
- **05.5**: TOOLS AND EQUIPMENT FOR HOUSE AND GARDEN
- **05.6**: Goods and services for routine maintenance
- **06.1**: MEDICAL PRODUCTS APPLIANCES AND EQUIPMENT
- **06.2**: OUT-PATIENT SERVICES:
- **06.3**: HOSPITAL SERVICES
- **07.1**: PURCHASE OF VEHICLES
- **07.2**: OPERATION OF PERSONAL TRANSPORT EQUIPMENT
- **07.3**: TRANSPORT SERVICES
- **08.1**: POSTAL SERVICES
- **08.2**: /3: TELEPHONE AND TELEFAX EQUIPMENT AND SERVICES
- **09.1**: Audio-visual equipment and related products
- **09.2**: OTHER MAJOR DURABLES FOR RECREATION AND CULTURE
- **09.3**: OTHER RECREATIONAL ITEMS AND EQUIPMENT GARDENS & PETS
- **09.4**: RECREATIONAL AND CULTURAL SERVICES
- **09.5**: Books, newspapers and stationery
- **09.6**: PACKAGE HOLIDAY
- **10.1**: /2/5 Pre-Primary, Primary & Secondary education (incl not definable by level)
- **10.4**: Tertiary education
- **11.1**: Catering services
- **11.2**: ACCOMMODATION SERVICES
- **12.1**: PERSONAL CARE
- **12.3**: PERSONAL EFFECTS N.E.C.
- **12.4**: SOCIAL PROTECTION
- **12.5**: INSURANCE
- **12.6**: FINANCIAL SERVICES N.E.C.
- **12.7**: OTHER SERVICES NEC

### Level 3: Classes (71 categories)

<details>
<summary>Click to expand Level 3 categories</summary>

- **01.1.1**: BREAD & CEREALS
- **01.1.2**: MEAT
- **01.1.3**: FISH
- **01.1.4**: MILK, CHEESE & EGGS
- **01.1.5**: OILS & FATS
- **01.1.6**: FRUIT
- **01.1.7**: VEGETABLES INCLUDING POTATOES AND OTHER TUBERS
- **01.1.8**: SUGAR, JAM, HONEY, SYRUPS, CHOCOLATE & CONFECTIONERY
- **01.1.9**: FOOD PRODUCTS
- **01.2.1**: COFFEE, TEA, COCOA
- **01.2.2**: MINERAL WATERS, SOFT DRINKS AND JUICES
- **02.1.1**: SPIRITS
- **02.1.2**: WINE
- **02.1.3**: BEER
- **03.1.2**: GARMENTS
- **03.1.3**: OTHER ARTICLES OF CLOTHING & ACCESSORIES
- **03.1.4**: Cleaning, repair and hire of clothing
- **04.3.1**: Materials for maintenance and repair
- **04.3.2**: Services for maintenance and repair
- **04.4.1**: WATER SUPPLY
- **04.4.3**: SEWERAGE COLLECTION
- **04.5.1**: ELECTRICITY
- **04.5.2**: GAS
- **04.5.3**: LIQUID FUELS
- **04.5.4**: SOLID FUELS
- **05.1.1**: Furniture and furnishings
- **05.1.2**: CARPETS & OTHER FLOOR COVERINGS
- **05.3.1**: /2 : Major appliances and small electric goods
- **05.3.3**: REPAIR OF HOUSEHOLD APPLIANCES
- **05.6.1**: NON-DURABLE HOUSEHOLD GOODS
- **05.6.2**: Domestic services and household services
- **06.1.1**: PHARMACEUTICAL PRODUCTS
- **06.1.2**: /3 : OTHER MEDICAL AND THERAPEUTIC EQUIPMENT
- **06.2.1**: /3 : MEDICAL SERVICES AND PARAMEDICAL SERVICES:
- **06.2.2**: DENTAL SERVICES:
- **07.1.1**: A : NEW CARS
- **07.1.1**: B : SECOND-HAND CARS
- **07.1.2**: /3 : MOTOR CYCLES AND BICYCLES
- **07.2.1**: SPARE PARTS & ACCESSORIES
- **07.2.2**: FUELS & LUBRICANTS
- **07.2.3**: MAINTENANCE & REPAIRS
- **07.2.4**: OTHER SERVICES
- **07.3.1**: PASSENGER TRANSPORT BY RAILWAY
- **07.3.2**: /6 : Passenger transport by road and other transport services
- **07.3.3**: PASSENGER TRANSPORT BY AIR
- **07.3.4**: PASSENGER TRANSPORT BY SEA AND INLAND WATERWAY
- **09.1.1**: Reception and reproduction of sound and pictures
- **09.1.2**: PHOTOGRAPHIC, CINEMATOGRAPHIC & OPTICAL EQUIPMENT
- **09.1.3**: Data processing equipment
- **09.1.4**: RECORDING MEDIA
- **09.1.5**: Repair of audio-visual equipment & related products
- **09.2.1**: /2/3 : Major durables for in/outdoor recreation & their maintenance
- **09.3.1**: GAMES TOYS AND HOBBIES
- **09.3.2**: EQUIPMENT FOR SPORT CAMPING AND OPEN-AIR RECREATION
- **09.3.3**: GARDEN PLANTS AND FLOWERS
- **09.3.4**: /5 : Pets, related products and services
- **09.4.1**: RECREATIONAL AND SPORTING SERVICES
- **09.4.2**: CULTURAL SERVICES
- **09.5.1**: BOOKS
- **09.5.2**: NEWSPAPERS AND PERIODICALS
- **09.5.3**: /4 :MISC. PRINTED MATTER STATIONERY & DRAWING MATERIALS
- **11.1.1**: RESTAURANTS & CAFES
- **11.1.2**: CANTEENS
- **12.1.1**: Hairdressing and personal grooming establishments
- **12.1.2**: /3 : APPLIANCES, ARTICLES & PRODUCTS FOR PERSONAL CARE
- **12.3.1**: JEWELLERY CLOCKS AND WATCHES
- **12.3.2**: OTHER PERSONAL EFFECTS
- **12.5.2**: House contents insurance
- **12.5.3**: /5 : Health insurance and other insurance
- **12.5.4**: Transport insurance
- **12.6.2**: Other financial services (nec)

</details>

### Level 4: Sub-classes (192 categories)

<details>
<summary>Click to expand Level 4 categories</summary>

- **01.1.1.1**: Rice
- **01.1.1.2**: Flours and other cereals
- **01.1.1.3**: Bread
- **01.1.1.4**: Other bakery products
- **01.1.1.5**: Pizza and quiche
- **01.1.1.6**: Pasta products and couscous
- **01.1.1.7**: /8 Breakfast cereals and other cereal products
- **01.1.2.1**: Beef and veal
- **01.1.2.2**: Pork
- **01.1.2.3**: Lamb and goat
- **01.1.2.4**: Poultry
- **01.1.2.6**: Edible offal
- **01.1.2.7**: Dried, salted or smoked meat
- **01.1.2.8**: Other meat preparations
- **01.1.3.1**: Fresh or chilled fish
- **01.1.3.4**: Frozen seafood
- **01.1.3.6**: Other preserved processed fish & seafood-based prep
- **01.1.4.1**: Whole milk
- **01.1.4.2**: Low fat milk
- **01.1.4.4**: Yoghurt
- **01.1.4.5**: Cheese and curd
- **01.1.4.6**: Other milk products
- **01.1.4.7**: Eggs
- **01.1.5.1**: Butter
- **01.1.5.2**: Margarine and other vegetable fats
- **01.1.5.3**: Olive oil
- **01.1.6.1**: Fresh or chilled fruit
- **01.1.6.3**: Dried fruit and nuts
- **01.1.6.4**: Preserved fruit and fruit-based products
- **01.1.7.1**: Fresh or chilled vegetables other than potatoes and other tubers
- **01.1.7.2**: Frozen vegetables other than potatoes and other tubers
- **01.1.7.3**: Dried vegetables, other preserved or processed vegetables
- **01.1.7.4**: Potatoes
- **01.1.7.5**: Crisps
- **01.1.7.6**: Other tubers and products of tuber vegetables
- **01.1.8.1**: Sugar
- **01.1.8.2**: Jams, marmalades and honey
- **01.1.8.3**: Chocolate
- **01.1.8.4**: Confectionery products
- **01.1.8.5**: Edible ices and ice cream
- **01.1.9.1**: /2 Sauces, condiments, salt, spices and culinary herbs
- **01.1.9.4**: Ready-made meals
- **01.1.9.9**: Other food products n.e.c.
- **01.2.1.1**: Coffee
- **01.2.1.2**: Tea
- **01.2.1.3**: Cocoa and powdered chocolate
- **01.2.2.1**: Mineral or spring waters
- **01.2.2.2**: Soft drinks
- **01.2.2.3**: Fruit and vegetable juices
- **02.1.2.1**: Wine from grapes
- **02.1.2.2**: Wine from other fruits
- **02.1.2.3**: Fortified wines
- **02.1.3.1**: Lager beer
- **02.1.3.2**: Other alcoholic beer
- **02.2.0.1**: Cigarettes
- **02.2.0.2**: Cigars
- **02.2.0.3**: Other tobacco products
- **03.1.2.1**: Garments for men
- **03.1.2.2**: Garments for women
- **03.1.2.3**: Garments for infants (0-2 yrs) & children (3-13 yrs)
- **03.1.3.1**: Other articles of clothing
- **03.1.3.2**: Clothing accessories
- **03.1.4.1**: Cleaning of clothing
- **03.1.4.2**: Repair and hire of clothing
- **03.2.1.1**: Footwear for men
- **03.2.1.2**: Footwear for women
- **03.2.1.3**: Footwear for infants and children
- **04.3.2.1**: Services of plumbers
- **04.3.2.2**: Services of electricians
- **04.3.2.4**: Services of painters
- **04.3.2.5**: Services of carpenters
- **04.5.2.1**: Natural gas and town gas
- **04.5.2.2**: Liquefied hydrocarbons (butane, propane etc)
- **05.1.1.1**: Household furniture
- **05.1.1.2**: Garden furniture
- **05.1.1.3**: Lighting equipment
- **05.1.1.9**: Other furniture and furnishings
- **05.1.2.1**: Carpets and rugs
- **05.1.2.2**: Other floor coverings
- **05.2.0.1**: Furnishing fabrics and curtains
- **05.2.0.2**: Bed linen
- **05.2.0.3**: Table linen and bathroom linen
- **05.3.1.1**: Refrigerators, freezers and fridge freezers
- **05.3.1.2**: Clothes washing machines, clothes drying machines and dish washing machines
- **05.3.1.3**: Cookers
- **05.3.1.4**: Heaters, air conditioners
- **05.3.1.5**: Cleaning equipment
- **05.3.2.2**: Coffee machines, tea makers & similar appliances
- **05.3.2.3**: Irons
- **05.3.2.9**: Other small electric household appliances
- **05.4.0.1**: Glassware, crystal-ware, ceramic ware & chinaware
- **05.4.0.2**: Cutlery, flatware and silverware
- **05.4.0.3**: Non-electric kitchen utensils and articles
- **05.5.1.1**: Motorized major tools and equipment
- **05.5.1.2**: Repair, leasing and rental of major tools and equipment
- **05.5.2.1**: Non-motorized small tools
- **05.5.2.2**: Miscellaneous small tool accessories
- **05.6.1.1**: Cleaning and maintenance products
- **05.6.1.2**: Other non-durable small household articles
- **05.6.2.1**: Domestic services by paid staff
- **05.6.2.9**: Other domestic services and household services
- **06.1.2.1**: Pregnancy tests and mechanical contraceptive devices
- **06.1.2.9**: Other medical products n.e.c.
- **06.1.3.1**: Corrective eye-glasses and contact lenses
- **07.1.2.0**: Motor cycles
- **07.1.3.0**: Bicycles
- **07.2.1.1**: Tyres
- **07.2.1.2**: Spare parts for personal transport equipment
- **07.2.2.1**: Diesel
- **07.2.2.2**: Petrol
- **07.2.2.4**: Lubricants
- **07.2.4.1**: Hire of garages, parking spaces and personal transport equipment
- **07.2.4.2**: Toll facilities and parking meters
- **07.2.4.3**: Driving lessons, test licences & road worthiness test
- **07.3.1.1**: Passenger transport by train
- **07.3.1.2**: Passenger transport by underground and tram
- **07.3.2.1**: Passenger transport by bus and coach
- **07.3.2.2**: Passenger transport by taxi & hired car with driver
- **07.3.6.2**: Removal and Storage Services
- **08.2.0.1**: Fixed telephone equipment
- **08.2.0.2**: Mobile telephone equipment
- **08.3.0.1**: Wired telephone services
- **08.3.0.2**: Wireless telephone services
- **08.3.0.3**: Internet access provision services
- **08.3.0.4**: Bundled telecommunication services
- **09.1.1.1**: Equipment for the reception, recording and reproduction of sound
- **09.1.1.2**: Equipment for the reception, recording and reproduction of sound and vision
- **09.1.1.3**: Portable sound and vision devices
- **09.1.1.9**: Other equipment for the reception, recording and reproduction of sound and picture
- **09.1.3.1**: Personal computers
- **09.1.3.2**: Accessories for information processing equipment
- **09.1.3.3**: Software
- **09.1.4.1**: Pre-recorded recording media
- **09.1.4.2**: Unrecorded recording media
- **09.1.4.9**: Other recording media
- **09.2.1.1**: Camper vans, caravans and trailers
- **09.2.1.3**: Boats, outboard motors and fitting out of boats
- **09.2.1.5**: Major items for games and sport
- **09.2.2.1**: Musical instruments
- **09.2.3.0**: Maintenance and repair of other major durables for recreation and culture
- **09.3.1.1**: Games and hobbies
- **09.3.1.2**: Toys and celebration articles
- **09.3.2.1**: Equipment for sport
- **09.3.2.2**: Equipment for camping and open-air recreation
- **09.3.3.1**: Garden products
- **09.3.3.2**: Plants and flowers
- **09.3.4.1**: Purchase of pets
- **09.3.4.2**: Products for pets
- **09.3.5.0**: Veterinary and other services for pets
- **09.4.1.1**: Recreational and sporting services - Attendance
- **09.4.1.2**: Recreational and sporting services - Participation
- **09.4.2.1**: Cinemas, theatres, concerts
- **09.4.2.2**: Museums, libraries, zoological gardens
- **09.4.2.3**: Television and radio licence fees, subscriptions
- **09.4.2.4**: Hire of equipment and accessories for culture
- **09.4.2.5**: Photographic services
- **09.5.1.1**: Fiction books
- **09.5.1.3**: Other non-fiction books
- **09.5.1.4**: Binding services and e-book downloads
- **09.5.2.1**: Newspapers
- **09.5.2.2**: Magazines and periodicals
- **09.5.3.0**: Miscellaneous printed matter
- **09.5.4.1**: Paper products
- **09.5.4.9**: Other stationery and drawing materials
- **11.1.1.1**: Restaurants, cafes and dancing establishments
- **11.1.1.2**: Fast food and take away food services
- **11.2.0.1**: Hotels, motels and similar accommodation services
- **11.2.0.2**: Holiday centres, camping sites, youth hostels and similar accommodation services
- **11.2.0.3**: Accommodation services of other establishments
- **12.1.1.1**: Hairdressing for men and children
- **12.1.1.2**: Hairdressing for women
- **12.1.1.3**: Personal grooming treatments
- **12.1.2.1**: Electric appliances for personal care
- **12.1.3.1**: Non-electrical appliances
- **12.1.3.2**: Articles for personal hygiene and wellness
- **12.3.1.1**: Jewellery
- **12.3.1.2**: Clocks and watches
- **12.3.1.3**: Repair of jewellery, clocks and watches
- **12.3.2.1**: Travel goods
- **12.3.2.2**: Articles for babies
- **12.3.2.9**: Other personal effects n.e.c.
- **12.4.0.1**: Child care services
- **12.4.0.2**: Retirement homes for elderly persons and residences for disabled persons
- **12.4.0.3**: Services to maintain people in their private homes
- **12.5.4.1**: Motor vehicle insurance
- **12.5.4.2**: Travel insurance
- **12.6.2.1**: Charges by banks and post offices
- **12.6.2.2**: Fees and service charges of brokers, investment counsellors
- **12.7.0.1**: Administrative fees
- **12.7.0.2**: Legal services and accountancy
- **12.7.0.3**: Funeral services
- **12.7.0.4**: Other fees and services

</details>

**Total: 316 hierarchical categories** (13 + 40 + 71 + 192)

This complete hierarchy shows the incredible detail available in the ONS dataset, from headline inflation (00) down to specific products like "Lager beer" (02.1.3.1) or "Driving lessons" (07.2.4.3). Each level provides increasingly granular analysis capabilities for economic research and policy making.