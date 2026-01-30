# Customer 360 Analytics Demo

A comprehensive Customer 360 analytics solution built on Databricks using the **Medallion Architecture** (Bronze/Silver/Gold). This demo showcases how to build a unified customer view by integrating data from multiple sources including transactions, loyalty programs, customer service interactions, and marketing campaigns.

## Table of Contents

- [Overview](#overview)
- [Demo Use Cases](#demo-use-cases)
- [Data Sources](#data-sources)
- [Medallion Architecture](#medallion-architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Table Reference](#table-reference)
- [Sample Outputs](#sample-outputs)

---

## Overview

### What is Customer 360?

Customer 360 is a unified view of customer data aggregated from multiple touchpoints, enabling:

- **Single Customer View**: One consolidated profile per customer across all channels
- **Customer Segmentation**: RFM analysis, churn prediction, high-value customer identification
- **Personalization**: Next-best-offer recommendations, campaign optimization
- **Analytics**: Customer lifetime value, campaign ROI, retention analysis

### Medallion Architecture

| Layer | Purpose | Data Quality |
|-------|---------|--------------|
| **Bronze** | Raw data ingestion, append-only | As-is from source |
| **Silver** | Cleaned, conformed, enriched | Validated & standardized |
| **Gold** | Business-level aggregates | Analytics-ready |

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  CSV Files  │ ──► │   BRONZE    │ ──► │   SILVER    │ ──► │    GOLD     │
│  (Raw Data) │     │ (Ingestion) │     │  (Cleaned)  │     │ (Analytics) │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

---

## Demo Use Cases

### Use Case 1: Unified Customer Profile (Single View)

**Business Question**: *"Who is this customer across all our touchpoints?"*

| Aspect | Details |
|--------|---------|
| **Tables Used** | Customers, LoyaltyAccounts, OnlineTransactions, InStoreTransactions, CustomerServiceInteractions |
| **KPIs** | Total spend, loyalty tier, support history, preferred channels, activity status |
| **Visualization** | Customer profile card with 360-degree metrics |

**Value**: Enables customer service reps and marketers to instantly understand customer context.

---

### Use Case 2: RFM Customer Segmentation

**Business Question**: *"Which customers are most valuable? Who is at risk of churning?"*

| Aspect | Details |
|--------|---------|
| **Tables Used** | Customers, OnlineTransactions, InStoreTransactions |
| **KPIs** | Recency (days since last purchase), Frequency (# transactions), Monetary (total spend) |
| **Visualization** | Segment distribution chart, customer value matrix |

**Customer Segments**:
| Segment | Description | Action |
|---------|-------------|--------|
| Champions | High R, F, M scores | Reward & retain |
| Loyal Customers | High F & M, good R | Upsell opportunities |
| New Customers | High R, low F | Nurture & onboard |
| Potential Loyalists | Good R & F | Convert to loyal |
| At Risk | Low R, high F & M | Win-back campaigns |
| Can't Lose Them | Low R, very high F & M | Urgent retention |
| Hibernating | Low R & F | Re-engagement |
| Lost | Low across all | Consider reactivation |

---

### Use Case 3: Customer Lifetime Value (CLV) Estimation

**Business Question**: *"What is each customer worth to us over their lifetime?"*

| Aspect | Details |
|--------|---------|
| **Tables Used** | Customer360 profile, All Transactions, LoyaltyAccounts |
| **KPIs** | Historical CLV, Predicted 3-year CLV, Purchase frequency, CLV tier |
| **Visualization** | CLV distribution, top customers ranking |

**CLV Tiers**:
- **Platinum**: Historical CLV >= $1,000
- **Gold**: Historical CLV >= $500
- **Silver**: Historical CLV >= $200
- **Bronze**: Historical CLV < $200

---

### Use Case 4: Campaign Performance & Attribution

**Business Question**: *"Which marketing campaigns drive the most valuable customers?"*

| Aspect | Details |
|--------|---------|
| **Tables Used** | Customer360Insights (campaign data) |
| **KPIs** | Conversion rate, Revenue per campaign, Avg order value, Return rate |
| **Visualization** | Campaign performance dashboard, attribution funnel |

**Tracked Campaigns**: Instagram-ads, Google-ads, Facebook-ads, Twitter-ads, Billboard-QR code

---

### Use Case 5: Churn Risk & Support Impact Analysis

**Business Question**: *"Which customers are likely to churn? How does support quality impact retention?"*

| Aspect | Details |
|--------|---------|
| **Tables Used** | CustomerServiceInteractions, Transactions, LoyaltyAccounts |
| **KPIs** | Churn risk score, Support ticket frequency, Resolution rate |
| **Visualization** | Churn risk heatmap, support impact correlation |

**Churn Risk Levels**:
- **Critical**: Hibernating or Lost customers
- **High**: At Risk or Can't Lose Them
- **Medium**: Needs Attention
- **Low**: All other active segments

---

## Data Sources

### Core 360 Data (`data/360/`)

| File | Rows | Description | Key Columns |
|------|------|-------------|-------------|
| `Customers.csv` | 100 | Customer master | CustomerID, Name, Email, Address |
| `Products.csv` | 100 | Product catalog | ProductID, Name, Category, Price |
| `Stores.csv` | 100 | Store locations | StoreID, Location, Manager, OpenHours |
| `Agents.csv` | 100 | Support agents | AgentID, Name, Department, Shift |
| `OnlineTransactions.csv` | 100 | E-commerce orders | OrderID, CustomerID, ProductID, Amount, Status |
| `InStoreTransactions.csv` | 100 | In-store purchases | TransactionID, CustomerID, StoreID, Amount |
| `LoyaltyAccounts.csv` | 100 | Loyalty membership | LoyaltyID, CustomerID, PointsEarned, TierLevel |
| `LoyaltyTransactions.csv` | 100 | Points activity | LoyaltyID, PointsChange, Reason |
| `CustomerServiceInteractions.csv` | 100 | Support tickets | InteractionID, CustomerID, IssueType, ResolutionStatus |

### Customer Insights (`data/`)

| File | Rows | Description |
|------|------|-------------|
| `Customer360Insights.csv` | 2,000 | Comprehensive analytics with demographics, transactions, campaigns |

**Columns**: SessionStart, CustomerID, FullName, Gender, Age, CreditScore, MonthlyIncome, Country, State, City, Category, Product, Cost, Price, Quantity, CampaignSchema, PaymentMethod, OrderReturn, ReturnReason

### Shopping Behavior (`data/shopping_behaviour/`)

| File | Rows | Description |
|------|------|-------------|
| `customer_transactions_final.csv` | 3,900 | Detailed shopping patterns |

**Columns**: customer_id, age, age_group, gender, item_purchased, category, purchase_amount, location, review_rating, subscription_status, shipping_type, discount_applied, frequency_of_purchases_days

---

## Medallion Architecture

### Bronze Layer (Raw Ingestion)

Raw data preserved as-is with metadata for lineage tracking.

| Table Name | Source File | Grain |
|------------|-------------|-------|
| `bronze_customers` | Customers.csv | CustomerID |
| `bronze_products` | Products.csv | ProductID |
| `bronze_stores` | Stores.csv | StoreID |
| `bronze_agents` | Agents.csv | AgentID |
| `bronze_online_transactions` | OnlineTransactions.csv | OrderID |
| `bronze_instore_transactions` | InStoreTransactions.csv | TransactionID |
| `bronze_loyalty_accounts` | LoyaltyAccounts.csv | LoyaltyID |
| `bronze_loyalty_transactions` | LoyaltyTransactions.csv | LoyaltyID + DateTime |
| `bronze_service_interactions` | CustomerServiceInteractions.csv | InteractionID |
| `bronze_customer_insights` | Customer360Insights.csv | SessionStart + CustomerID |
| `bronze_shopping_behavior` | customer_transactions_final.csv | customer_id |

**Metadata Columns Added**:
- `_source_file`: Original file path
- `_ingestion_timestamp`: When data was loaded
- `_ingestion_date`: Date partition

---

### Silver Layer (Cleaned & Conformed)

Standardized, validated, and enriched data ready for joining.

| Table Name | Grain | Key Transformations |
|------------|-------|---------------------|
| `silver_customers` | customer_id | Email validation, domain extraction, snake_case columns |
| `silver_transactions` | transaction_id | Unified online + in-store, channel indicator, date parts |
| `silver_loyalty` | loyalty_id | Tier ranking, tenure calculation |
| `silver_service_interactions` | interaction_id | Resolution flags, escalation tracking |
| `silver_customer_insights` | customer_id + session | Age groups, income brackets, profit margins |

**Data Quality Checks Applied**:
- Duplicate detection and removal
- Null value counting and handling
- Data type validation
- Schema standardization (snake_case)

---

### Gold Layer (Analytics-Ready)

Business-level aggregates optimized for reporting and ML.

| Table Name | Grain | Description | Key Metrics |
|------------|-------|-------------|-------------|
| `gold_customer_360` | customer_id | Unified customer profile | total_spend, loyalty_tier, preferred_channel, is_active |
| `gold_customer_segments` | customer_id | RFM segmentation | r_score, f_score, m_score, customer_segment, churn_risk |
| `gold_customer_clv` | customer_id | Lifetime value | historical_clv, predicted_clv_3yr, clv_tier |
| `gold_campaign_performance` | campaign_source | Campaign analytics | conversion_rate, total_revenue, avg_order_value |
| `gold_daily_metrics` | transaction_date | Daily trends | total_transactions, total_revenue, unique_customers |

---

## Quick Start

### Prerequisites

- Databricks workspace (Runtime 11.3+ recommended)
- Delta Lake enabled
- Unity Catalog (optional, can use Hive metastore)

### Steps

1. **Clone the repository** to your Databricks workspace:
   ```
   Repos > Add Repo > https://github.com/your-org/customer360
   ```

2. **Update configuration** in `notebooks/config.py`:
   ```python
   BASE_RAW_PATH = "/Workspace/Repos/customer360/data"  # Your data path
   CATALOG_NAME = "customer360_demo"                     # Your catalog
   SCHEMA_NAME = "analytics"                             # Your schema
   USE_UNITY_CATALOG = True                              # Or False for Hive
   ```

3. **Run the pipelines in order**:
   ```
   Step 1: Run notebooks/01_bronze_ingestion.py
   Step 2: Run notebooks/02_silver_transformation.py
   Step 3: Run notebooks/03_gold_analytics.py
   ```

4. **Each pipeline produces**:
   - **Bronze**: Raw Delta tables with lineage metadata
   - **Silver**: Cleaned, validated, and enriched tables
   - **Gold**: Analytics-ready aggregations and KPIs

### Pipeline Execution Order

```
┌─────────────────────┐
│   config.py         │  ◄── Shared configuration (run first or %run ./config)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 01_bronze_ingestion │  ◄── CSV → Bronze Delta tables
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 02_silver_transform │  ◄── Bronze → Silver (cleaned/unified)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 03_gold_analytics   │  ◄── Silver → Gold (aggregations)
└─────────────────────┘
```

---

## Configuration

### Key Variables

```python
# Base path where raw CSV files are stored
BASE_RAW_PATH = "/Workspace/Repos/customer360/data"

# Unity Catalog configuration
CATALOG_NAME = "customer360_demo"
SCHEMA_NAME = "analytics"

# Set to False to use legacy Hive metastore
USE_UNITY_CATALOG = True

# Alternative paths for different environments:
# DBFS mount:        "/dbfs/mnt/datalake/raw/customer360"
# Unity Catalog vol: "/Volumes/catalog/schema/raw_data"
# Local testing:     "./data"
```

### File Pattern Mapping

The pipeline auto-discovers CSVs using these patterns:

```python
CSV_PATTERNS = {
    "customers": ["Customers*.csv", "customers*.csv"],
    "products": ["Products*.csv", "products*.csv"],
    "online_transactions": ["OnlineTransactions*.csv"],
    # ... etc
}
```

---

## Table Reference

### Full Table Lineage

```
CSV Files
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ BRONZE (11 tables)                                                │
│ ├── bronze_customers ◄─────────────── Customers.csv               │
│ ├── bronze_products ◄──────────────── Products.csv                │
│ ├── bronze_online_transactions ◄───── OnlineTransactions.csv      │
│ ├── bronze_instore_transactions ◄──── InStoreTransactions.csv     │
│ ├── bronze_loyalty_accounts ◄──────── LoyaltyAccounts.csv         │
│ ├── bronze_service_interactions ◄──── CustomerServiceInteract...  │
│ └── bronze_customer_insights ◄─────── Customer360Insights.csv     │
└───────────────────────────────────────────────────────────────────┘
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ SILVER (5 tables)                                                 │
│ ├── silver_customers ◄───────────────── bronze_customers          │
│ ├── silver_transactions ◄──────────────┬ bronze_online_trans      │
│ │                                       └ bronze_instore_trans    │
│ ├── silver_loyalty ◄───────────────────── bronze_loyalty_accounts │
│ ├── silver_service_interactions ◄──────── bronze_service_inter... │
│ └── silver_customer_insights ◄─────────── bronze_customer_insig...│
└───────────────────────────────────────────────────────────────────┘
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ GOLD (5 tables)                                                   │
│ ├── gold_customer_360 ◄────────────────┬ silver_customers         │
│ │                                       ├ silver_transactions     │
│ │                                       ├ silver_loyalty          │
│ │                                       └ silver_service_inter... │
│ ├── gold_customer_segments ◄───────────── gold_customer_360       │
│ ├── gold_customer_clv ◄────────────────── gold_customer_360       │
│ ├── gold_campaign_performance ◄────────── silver_customer_insig...│
│ └── gold_daily_metrics ◄───────────────── silver_transactions     │
└───────────────────────────────────────────────────────────────────┘
```

---

## Sample Outputs

### Customer 360 Profile Sample

| customer_id | customer_name | total_spend | loyalty_tier | preferred_channel | churn_risk |
|-------------|---------------|-------------|--------------|-------------------|------------|
| 1001 | Brittany Franklin | $2,450.00 | Gold | Online | Low |
| 1002 | Scott Stewart | $1,890.50 | Silver | Omnichannel | Medium |
| 1003 | Elizabeth Fowler | $3,200.75 | Platinum | In-Store | Low |

### Segment Distribution Sample

| customer_segment | customer_count | avg_spend | avg_transactions |
|------------------|----------------|-----------|------------------|
| Champions | 45 | $1,250.00 | 12.5 |
| Loyal Customers | 120 | $850.00 | 8.2 |
| At Risk | 35 | $950.00 | 6.8 |
| New Customers | 80 | $150.00 | 1.5 |

### Campaign Performance Sample

| campaign_source | conversions | conversion_rate | total_revenue |
|-----------------|-------------|-----------------|---------------|
| Google-ads | 450 | 12.5% | $45,000 |
| Instagram-ads | 380 | 15.2% | $38,500 |
| Facebook-ads | 320 | 11.8% | $32,000 |

---

## Next Steps

### Production Enhancements

1. **Incremental Processing**: Use Delta Lake MERGE for CDC patterns
2. **Data Quality**: Integrate Great Expectations or DLT expectations
3. **Orchestration**: Schedule with Databricks Workflows
4. **ML Integration**: Add churn prediction, propensity scoring models
5. **Visualization**: Connect to Databricks SQL dashboards

### Extending the Demo

- Add real-time streaming with Structured Streaming
- Implement ML-based customer propensity scores
- Build recommendation engine for next-best-offer
- Create executive dashboards in Databricks SQL

---

## Project Structure

```
customer360/
├── README.md                              # This documentation
├── data/
│   ├── 360/                               # Core customer 360 CSVs
│   │   ├── Customers.csv
│   │   ├── Products.csv
│   │   ├── Stores.csv
│   │   ├── Agents.csv
│   │   ├── OnlineTransactions.csv
│   │   ├── InStoreTransactions.csv
│   │   ├── LoyaltyAccounts.csv
│   │   ├── LoyaltyTransactions.csv
│   │   └── CustomerServiceInteractions.csv
│   ├── Customer360Insights.csv            # Comprehensive insights
│   └── shopping_behaviour/
│       └── customer_transactions_final.csv
└── notebooks/
    ├── config.py                          # Shared configuration & utilities
    ├── 01_bronze_ingestion.py             # Bronze layer pipeline
    ├── 02_silver_transformation.py        # Silver layer pipeline
    └── 03_gold_analytics.py               # Gold layer pipeline
```

## Pipeline Details

### config.py - Shared Configuration

Contains all configurable parameters used across pipelines:
- `BASE_RAW_PATH`: Location of source CSV files
- `CATALOG_NAME` / `SCHEMA_NAME`: Unity Catalog settings
- `DATABASE_NAME`: Legacy Hive metastore database
- `USE_UNITY_CATALOG`: Toggle between UC and Hive
- `FILE_TO_TABLE_MAPPING`: CSV to table name mappings
- Helper functions: `get_full_table_name()`, `setup_database()`

### 01_bronze_ingestion.py - Bronze Layer

**Purpose**: Ingest raw CSVs into Delta tables with minimal transformation.

**Features**:
- Auto-discovers CSV files in configured path
- Infers schemas and displays summary report
- Adds lineage metadata columns (`_source_file`, `_ingestion_timestamp`)
- Supports overwrite (initial) and append (incremental) modes

**Output Tables**: 11 Bronze tables (one per CSV)

### 02_silver_transformation.py - Silver Layer

**Purpose**: Clean, validate, and enrich data for analytics.

**Features**:
- Data quality checks (nulls, duplicates)
- Schema standardization (snake_case naming)
- Type casting and validation
- Unifies online + in-store transactions
- Adds derived columns (age_group, income_bracket, etc.)

**Output Tables**: 7 Silver tables

### 03_gold_analytics.py - Gold Layer

**Purpose**: Create business-ready aggregated tables.

**Features**:
- Customer 360 unified profile
- RFM segmentation with churn risk
- Customer Lifetime Value estimation
- Campaign performance analytics
- Daily KPI metrics

**Output Tables**: 5 Gold tables

---

## License

This demo is provided for educational and demonstration purposes.

## Contact

For questions or customization requests, contact the Customer Analytics Team.
