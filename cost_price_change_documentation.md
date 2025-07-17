# Cost Price Change Analysis Documentation

## Overview

The Cost Price Change Analysis is a Databricks notebook designed to provide automated price analysis and comparison across multiple countries. It enables users to select a focus country and analyze price differences, opportunities, and segments compared to other countries in the dataset.

## Architecture

### Technology Stack
- **Platform**: Databricks Notebook
- **Language**: Python with PySpark
- **Database**: Spark SQL
- **Visualization**: Databricks display functions

### Data Source
- **Source Table**: `all_working.vg_top_sku_price_benchmark_v5`
- **Data Scope**: Multi-country retail price and sales data

## Country Configuration

### Supported Countries
The system supports 10 countries with their respective keys:

| Country | Key | Full Name |
|---------|-----|-----------|
| KSA | 11 | Saudi Arabia |
| UAE | 1 | United Arab Emirates |
| BAHRAIN | 2 | Bahrain |
| EGYPT | 3 | Egypt |
| JORDAN | 5 | Jordan |
| KUWAIT | 6 | Kuwait |
| LEBANON | 7 | Lebanon |
| QATAR | 10 | Qatar |
| GEORGIA | 14 | Georgia |
| KENYA | 18 | Kenya |

### Interactive Selection
- Uses Databricks `dbutils.widgets.dropdown` for country selection
- Default selection: KSA (Saudi Arabia)
- Dynamic analysis based on selected country

## Core Functions

### 1. `create_base_data()`
**Purpose**: Creates the foundational data view from the source table

**Process**:
- Extracts key fields from source table
- Creates temporary view `base_data`
- Validates data with row count and unique item checks

**Key Fields**:
- Country information (COUNTRY_NAME, COUNTRY_KEY)
- Product details (ITEM_BARCODE, DEPT_NAME, SECTION_NAME, etc.)
- Pricing data (ITEM_COST_PRICE_AED_MEAN, ITEM_COST_PRICE_AED_LATEST)
- Sales metrics (DAY_SLS_QTY_SUM, DAY_SLS_AMT, BEM_MONTH_PROFIT_N)

### 2. `create_pivoted_data()`
**Purpose**: Transforms country-based rows into column-based structure for comparison

**Process**:
- Pivots data to create country-specific columns
- Aggregates data by item barcode and product hierarchy
- Creates separate columns for each metric per country

**Generated Columns**:
- `{COUNTRY}_ITEM_COST_LATEST`: Latest cost price per country
- `{COUNTRY}_ITEM_COST_MEAN`: Average cost price per country
- `{COUNTRY}_SLS_QTY_SUM`: Sales quantity per country
- `{COUNTRY}_PROFIT`: Profit per country
- `{COUNTRY}_SALES_AMT`: Sales amount per country

### 3. `generate_price_analysis_query(focus_country)`
**Purpose**: Generates dynamic SQL queries for price analysis based on selected focus country

**Parameters**:
- `focus_country`: The country to use as baseline for comparison

**Process**:
1. **Filtering**: Creates filtered dataset with focus country data and comparison countries
2. **Price Change Calculations**: Calculates percentage change for each country vs focus country
3. **Price Segmentation**: Categorizes price differences into segments
4. **Minimum Cost Analysis**: Identifies cheapest alternative country
5. **Opportunity Calculation**: Calculates cost opportunity amounts

**Generated Views**:
- `filtered_data_{focus_country}`: Filtered dataset
- `{focus_country}_price_segmentation`: Price segments and changes
- `final_price_analysis_{focus_country}`: Complete analysis with opportunities

### 4. `build_final_select_query(focus_country)`
**Purpose**: Constructs the final SELECT query for result presentation

**Returns**: Formatted SQL query with rounded values and proper column ordering

## Data Flow

```
Source Table (vg_top_sku_price_benchmark_v5)
    ↓
Base Data View (base_data)
    ↓
Pivoted Data View (pivoted_data)
    ↓
Filtered Data View (filtered_data_{country})
    ↓
Price Segmentation View ({country}_price_segmentation)
    ↓
Final Analysis View (final_price_analysis_{country})
    ↓
Results Display & Summary Statistics
```

## Price Segmentation Logic

### Individual Country Segments
- **"Cheaper at {focus_country}"**: Other country is more expensive
- **"Expensive by 0-10%"**: 0-10% price difference
- **"Expensive by 10-25%"**: 10-25% price difference
- **"Expensive by 25-50%"**: 25-50% price difference
- **"Expensive by >50%"**: More than 50% price difference
- **"Not available"**: No price data available

### Minimum Cost Segments
- **"Cheaper elsewhere"**: Focus country is more expensive than cheapest alternative
- **"Expensive by 0-10%"**: Focus country is 0-10% more expensive
- **"Expensive by 10-25%"**: Focus country is 10-25% more expensive
- **"Expensive by 25-50%"**: Focus country is 25-50% more expensive
- **"Expensive by >50%"**: Focus country is >50% more expensive

## Key Metrics

### Cost Opportunity Amount
**Formula**: `(focus_country_cost - min_cost_other_countries) × focus_country_sales_quantity`

**Conditions**:
- Only calculated when focus country cost > minimum cost elsewhere
- Only positive values (opportunities) are included
- Represents potential savings if prices were aligned with cheapest country

### Price Change Percentage
**Formula**: `(other_country_cost / focus_country_cost - 1) × 100`

**Interpretation**:
- Positive values: Other country is more expensive
- Negative values: Other country is cheaper
- Used for comparative analysis

## Output Structure

### Main Results Table
- **ITEM_BARCODE**: Product identifier
- **ITEM_BRAND_NAME**: Brand name
- **DEPT_NAME**: Department
- **SECTION_NAME**: Section
- **FAMILY_NAME**: Product family
- **{FOCUS_COUNTRY}_ITEM_COST_LATEST**: Focus country cost price
- **{FOCUS_COUNTRY}_SLS_QTY_SUM**: Focus country sales quantity
- **{COUNTRY}_PRICE_CHANGE_PCT**: Price change percentage for each country
- **{COUNTRY}_PRICE_SEGMENT**: Price segment for each country
- **CHEAPEST_COUNTRY**: Country with lowest cost
- **MIN_COST_OTHER_COUNTRIES**: Minimum cost among other countries
- **MIN_COST_SEGMENT**: Segment based on minimum cost comparison
- **COST_OPPORTUNITY_AMOUNT**: Calculated opportunity amount

### Summary Statistics
- **total_items**: Total number of items analyzed
- **avg_opportunity**: Average opportunity amount
- **total_opportunity**: Total opportunity amount
- **max_opportunity**: Maximum opportunity amount
- **items_count**: Count by cheapest country and segment

## Usage Instructions

### Step-by-Step Process
1. **Select Country**: Use the dropdown widget to select focus country
2. **Run All Cells**: Execute all notebook cells sequentially
3. **View Results**: Review the main analysis table
4. **Analyze Summary**: Check summary statistics for insights
5. **Export (Optional)**: Save results to permanent table if needed

### Key Features
- **Fully Automated**: Works for any of the 10 supported countries
- **Interactive Selection**: Easy country switching via dropdown
- **Dynamic SQL Generation**: Automatically adapts queries based on selection
- **Comprehensive Analysis**: Multiple price comparison methods
- **Opportunity Identification**: Quantifies cost savings potential

## Data Validation

### Quality Checks
- Row count validation at each step
- Unique item count verification
- Duplicate detection and reporting
- Data availability confirmation

### Error Handling
- SQL execution error catching
- Data validation warnings
- Missing data handling

## Performance Considerations

### Optimization Features
- Temporary views for intermediate results
- Efficient aggregation strategies
- Filtered datasets to reduce processing
- Proper indexing on ITEM_BARCODE

### Scalability
- Handles large datasets through Spark processing
- Memory-efficient view creation
- Optimized SQL queries

## Limitations

### Current Constraints
- Fixed to 10 predefined countries
- Requires specific source table structure
- AED-based pricing (currency conversion needed for local analysis)
- Snapshot-based analysis (not real-time)

### Future Enhancements
- Dynamic country addition
- Currency conversion capabilities
- Historical trend analysis
- Real-time data integration

## Troubleshooting

### Common Issues
1. **No Data for Selected Country**: Verify country key exists in source data
2. **Duplicate Items**: Check source data quality and aggregation logic
3. **Performance Issues**: Consider data volume and Spark cluster resources
4. **Missing Comparisons**: Ensure other countries have data for selected items

### Debugging Tips
- Check intermediate view row counts
- Verify country key mappings
- Review SQL query generation
- Validate data availability across countries

## Maintenance

### Regular Tasks
- Update country mappings if new countries added
- Verify source table structure changes
- Monitor data quality metrics
- Review performance optimization opportunities

### Version Control
- Track notebook changes
- Document configuration updates
- Maintain country mapping consistency
- Update documentation with changes