# Global Wholesale Distributor Data Engineering Pipeline
## Complete Technical Documentation

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Project Architecture](#project-architecture)
3. [Data Flow & Orchestration](#data-flow--orchestration)
4. [Layer-by-Layer Implementation](#layer-by-layer-implementation)
5. [Data Model](#data-model)
6. [KPI Metrics](#kpi-metrics)
7. [Technologies & Tools](#technologies--tools)
8. [Execution Workflow](#execution-workflow)
9. [Best Practices](#best-practices)

---

## 1. Executive Summary

This project implements a **production-grade data engineering pipeline** for a global wholesale distributor using **PySpark, Databricks, and Azure Blob Storage**. The pipeline transforms raw transactional data into actionable business intelligence through a **layered medallion architecture**.

### Key Highlights

* **Data Volume**: Processes 7 source systems with 30 analytical tables
* **Architecture**: 4-layer medallion (Bronze → Silver → Gold → Reporting)
* **Tables**: 30 tables across Raw (7), Staging (7), Curated (5), Reporting (11)
* **KPIs**: 11 business metrics covering revenue, customer behavior, and product performance
* **Technology Stack**: PySpark, Databricks, Delta Lake, Azure Storage
* **Orchestration**: Automated using Databricks Jobs with dependency management

---

## 2. Project Architecture

### 2.1 Medallion Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    GLOBAL WHOLESALE DISTRIBUTOR PIPELINE                 │
└─────────────────────────────────────────────────────────────────────────┘

   AZURE BLOB STORAGE              DATABRICKS LAKEHOUSE
┌──────────────────┐         ┌────────────────────────────────┐
│                  │         │                                │
│  CSV Files       │────────▶│  BRONZE LAYER (Raw)            │
│  - customers     │         │     • Ingestion                │
│  - products      │         │     • Schema enforcement       │
│  - invoices      │         │     • 7 tables                 │
│  - payments      │         │                                │
│  - regions       │         └────────┬───────────────────────┘
│  - exchange      │                  │
│  - line_items    │                  ▼
│                  │         ┌────────────────────────────────┐
└──────────────────┘         │  SILVER LAYER (Staging)        │
                             │     • Data cleaning            │
                             │     • Validation               │
                             │     • Deduplication            │
                             │     • 7 tables                 │
                             │                                │
                             └────────┬───────────────────────┘
                                      │
                                      ▼
                             ┌────────────────────────────────┐
                             │  GOLD LAYER (Curated)          │
                             │     • Business modeling        │
                             │     • Fact & Dimension tables  │
                             │     • Currency conversion      │
                             │     • 5 tables                 │
                             │                                │
                             └────────┬───────────────────────┘
                                      │
                                      ▼
                             ┌────────────────────────────────┐
                             │  REPORTING LAYER (KPI)         │
                             │     • Business metrics         │
                             │     • Aggregations             │
                             │     • Analytics-ready          │
                             │     • 11 KPI tables            │
                             │                                │
                             └────────────────────────────────┘
                                      
```

### 2.2 Catalog & Schema Structure

```
global_wholesale_distributor (CATALOG)
├── raw_wholesale_distributor (SCHEMA)
│   ├── customers
│   ├── products
│   ├── invoices
│   ├── invoice_line_items
│   ├── payments
│   ├── exchange_rates
│   └── regions
│
├── staging_wholesale_distributor (SCHEMA)
│   ├── sl_customers
│   ├── sl_products
│   ├── sl_invoices
│   ├── sl_invoice_line_items
│   ├── sl_payments
│   ├── sl_exchange_rates
│   └── sl_regions
│
├── curated_wholesale_distributor (SCHEMA)
│   ├── dim_customers
│   ├── dim_products
│   ├── dim_invoices
│   ├── fact_invoice_line_items
│   └── fact_payments
│
└── reporting_wholesale_distributor (SCHEMA)
    ├── kpi_total_revenue
    ├── kpi_total_quantity_sold
    ├── kpi_average_invoice_value
    ├── kpi_top_10_customers
    ├── kpi_top_products
    ├── kpi_invoice_cancellation_rate
    ├── kpi_invoice_cancellation_df
    ├── kpi_discount_metrics
    ├── kpi_revenue_by_region
    ├── kpi_invoices_per_customer
    └── kpi_product_revenue
```

---

## 3. Data Flow & Orchestration

### 3.1 Pipeline Orchestration

**Databricks Job**: `wholesale_distributor_pipeline`

```
┌──────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION WORKFLOW                       │
└──────────────────────────────────────────────────────────────┘

Task 1: Bronze Ingestion
┌────────────────────────────┐
│ nb_bronze.ipynb            │
│ Read CSV from Azure        │
│ Write to Delta tables      │
└────────────┬───────────────┘
             │ SUCCESS
             ▼
Task 2: Silver Cleaning
┌────────────────────────────┐
│ sl_cleaning_notebook.ipynb │
│ Remove duplicates          │
│ Handle nulls               │
│ Standardize values         │
└────────────┬───────────────┘
             │ SUCCESS
             ▼
Task 3: Silver Transform
┌────────────────────────────┐
│ sl_transform_notebook.ipynb│
│ Type conversions           │
│ Date parsing               │
│ Currency conversion prep   │
└────────────┬───────────────┘
             │ SUCCESS
             ▼
Task 4: Gold Modeling
┌────────────────────────────┐
│ nb_gold.ipynb              │
│ Create dimensions          │
│ Create facts               │
│ Currency conversion        │
│ Calculate revenue          │
└────────────┬───────────────┘
             │ SUCCESS
             ▼
Task 5: KPI Generation
┌────────────────────────────┐
│ nb_kpi.ipynb               │
│ Aggregate metrics          │
│ Calculate KPIs             │
│ Write reporting tables     │
└────────────────────────────┘
```

---

## 4. Layer-by-Layer Implementation

### 4.1 Bronze Layer (Raw Data Ingestion)

**Notebook**: `01_Bronze/01_NoteBooks/nb_bronze.ipynb`

**Objective**: Ingest raw data from Azure Blob Storage into Delta tables

**Process**:
1. **Secrets Management**: Retrieve storage access key from Databricks Secret Scope
2. **Catalog Setup**: Create catalog and schemas if not exists
3. **Configuration**: Configure Azure storage account connection
4. **Data Ingestion**: Read CSV files and write to Delta format

**Key Features**:
* Schema inference enabled
* Metadata columns added (`_ingestion_time`, `_source_file`)
* Append-only pattern
* No data transformation

**Source Files**:
* customers.csv
* products.csv
* invoices.csv
* invoice_line_items.csv
* payments.csv
* exchange_rates.csv
* regions.csv

**Output Tables (7)**:
```
global_wholesale_distributor.raw_wholesale_distributor.customers
global_wholesale_distributor.raw_wholesale_distributor.products
global_wholesale_distributor.raw_wholesale_distributor.invoices
global_wholesale_distributor.raw_wholesale_distributor.invoice_line_items
global_wholesale_distributor.raw_wholesale_distributor.payments
global_wholesale_distributor.raw_wholesale_distributor.exchange_rates
global_wholesale_distributor.raw_wholesale_distributor.regions
```

---

### 4.2 Silver Layer (Data Cleaning & Validation)

**Notebooks**: 
* `02_Silver/01_Notebooks/sl_cleaning_notebook.ipynb`
* `02_Silver/01_Notebooks/sl_transform_notebook.ipynb`

**Objective**: Clean, validate, and standardize raw data

**Transformations**:

#### Customers Table
* Drop metadata columns (`_ingestion_time`, `_source_file`)
* Standardize `customer_type`: "TEST" → "Test"
* Standardize `country`: "U.S.A" → "USA", "india" → "India"
* Fill null countries with "Unknown"
* Remove duplicates

#### Products Table
* Drop metadata columns
* Fill null `cost_price` with 0
* Fill null `category` with "Unknown"
* Standardize `category`: "electronics" → "Electronics"
* Remove duplicates

#### Invoices Table
* Drop metadata columns
* Standardize `region`: "EAST" → "East", "west" → "West", etc.
* Standardize `invoice_status`: "paid" → "Paid"
* Convert `invoice_date` to date type (format: yyyy/M/d)
* Remove duplicates

#### Invoice Line Items Table
* Drop metadata columns
* Fill null `discount` with 0.0
* Fill null `unit_price` with 0
* Remove duplicates

#### Payments, Exchange Rates, Regions
* Drop metadata columns
* Remove duplicates

**Output Tables (7)**:
```
global_wholesale_distributor.staging_wholesale_distributor.sl_customers
global_wholesale_distributor.staging_wholesale_distributor.sl_products
global_wholesale_distributor.staging_wholesale_distributor.sl_invoices
global_wholesale_distributor.staging_wholesale_distributor.sl_invoice_line_items
global_wholesale_distributor.staging_wholesale_distributor.sl_payments
global_wholesale_distributor.staging_wholesale_distributor.sl_exchange_rates
global_wholesale_distributor.staging_wholesale_distributor.sl_regions
```

---

### 4.3 Gold Layer (Business Modeling)

**Notebook**: `03_gold/01__notebooks/nb_gold.ipynb`

**Objective**: Create dimensional model for analytics

**Data Model**:

#### Dimension Tables (3)

**1. dim_customers**
* Source: `sl_customers`
* Columns: customer_id, customer_name, customer_type, country, signup_date
* Purpose: Customer master data

**2. dim_products**
* Source: `sl_products`
* Columns: product_id, product_name, category
* Dropped: cost_price, cost_price_usd, original_currency
* Purpose: Product catalog

**3. dim_invoices**
* Source: `sl_invoices`
* Columns: invoice_id, customer_id, invoice_date, invoice_month, currency, region, invoice_status
* Purpose: Invoice header data

#### Fact Tables (2)

**1. fact_invoice_line_items**
* Source: `sl_invoice_line_items`
* Dropped: product_name, customer_name, region, original_currency
* **Calculated Column**: `usd_revenue = quantity × (1 - discount) × unit_price_usd`
* Purpose: Transactional sales data

**2. fact_payments**
* Source: `sl_payments`
* Dropped: payment_method, customer_name, region, original_currency, payment_date
* Purpose: Payment transactions

**Currency Conversion**:
* All monetary values converted to USD
* Exchange rates joined from `sl_exchange_rates`
* Ensures consistent reporting across regions

**Output Tables (5)**:
```
global_wholesale_distributor.curated_wholesale_distributor.dim_customers
global_wholesale_distributor.curated_wholesale_distributor.dim_products
global_wholesale_distributor.curated_wholesale_distributor.dim_invoices
global_wholesale_distributor.curated_wholesale_distributor.fact_invoice_line_items
global_wholesale_distributor.curated_wholesale_distributor.fact_payments
```

---

### 4.4 Reporting Layer (KPI Generation)

**Notebook**: `03_gold/01__notebooks/nb_kpi.ipynb`

**Objective**: Generate business metrics for analytics and dashboards

**Output Tables (11)**:
```
global_wholesale_distributor.reporting_wholesale_distributor.kpi_total_revenue
global_wholesale_distributor.reporting_wholesale_distributor.kpi_total_quantity_sold
global_wholesale_distributor.reporting_wholesale_distributor.kpi_average_invoice_value
global_wholesale_distributor.reporting_wholesale_distributor.kpi_top_10_customers
global_wholesale_distributor.reporting_wholesale_distributor.kpi_top_products
global_wholesale_distributor.reporting_wholesale_distributor.kpi_invoice_cancellation_rate
global_wholesale_distributor.reporting_wholesale_distributor.kpi_invoice_cancellation_df
global_wholesale_distributor.reporting_wholesale_distributor.kpi_discount_metrics
global_wholesale_distributor.reporting_wholesale_distributor.kpi_revenue_by_region
global_wholesale_distributor.reporting_wholesale_distributor.kpi_invoices_per_customer
global_wholesale_distributor.reporting_wholesale_distributor.kpi_product_revenue
```

---

## 5. Data Model

### 5.1 Entity Relationship Diagram

```
┌─────────────────────┐
│   dim_customers     │
├─────────────────────┤
│ PK customer_id      │
│    customer_name    │
│    customer_type    │
│    country          │
│    signup_date      │
└──────────┬──────────┘
           │ 1
           │
           │ *
┌──────────▼──────────┐        ┌─────────────────────┐
│   dim_invoices      │        │   dim_products      │
├─────────────────────┤        ├─────────────────────┤
│ PK invoice_id       │        │ PK product_id       │
│ FK customer_id      │        │    product_name     │
│    invoice_date     │        │    category         │
│    invoice_month    │        └──────────┬──────────┘
│    currency         │                   │ 1
│    region           │                   │
│    invoice_status   │                   │ *
└──────────┬──────────┘        ┌──────────▼──────────┐
           │ 1                 │fact_invoice_line_it │
           │                   ├─────────────────────┤
           │ *                 │ PK line_item_id     │
           └───────────────────│ FK invoice_id       │
                               │ FK product_id       │
                               │    quantity         │
                               │    unit_price_usd   │
                               │    discount         │
                               │    usd_revenue      │
                               └─────────────────────┘

           ┌──────────┐
           │ 1        │
           │          │ *
           │   ┌──────▼──────────┐
           └───│  fact_payments  │
               ├─────────────────┤
               │ PK payment_id   │
               │ FK invoice_id   │
               │    payment_amt  │
               └─────────────────┘
```

---

## 6. KPI Metrics

### 6.1 Revenue Metrics

**1. Total Revenue**
* **Formula**: `SUM(usd_revenue)`
* **Table**: `kpi_total_revenue`
* **Current Value**: $12,163,531.55
* **Description**: Sum of all invoice line-item amounts in USD

**2. Average Invoice Value**
* **Formula**: `AVG(SUM(usd_revenue) GROUP BY invoice_id)`
* **Table**: `kpi_average_invoice_value`
* **Current Value**: $6,081.77
* **Description**: Average total value per invoice

**3. Revenue by Region**
* **Formula**: `SUM(usd_revenue) GROUP BY region`
* **Table**: `kpi_revenue_by_region`
* **Breakdown**:
  * West: $3,479,022.15
  * South: $3,001,760.49
  * North: $2,885,715.24
  * East: $2,797,033.67

### 6.2 Product Metrics

**4. Total Quantity Sold**
* **Formula**: `SUM(quantity)`
* **Table**: `kpi_total_quantity_sold`
* **Current Value**: 34,151 units
* **Description**: Sum of quantities sold across all products

**5. Top Products by Revenue**
* **Formula**: `SUM(usd_revenue) GROUP BY product_id ORDER BY revenue DESC`
* **Table**: `kpi_top_products`
* **Top 3**:
  * Product_14: $223,548.42
  * Product_25: $246,087.24
  * Product_9: $261,046
* **Description**: Products generating the highest sales revenue

**6. Product Revenue**
* **Formula**: `SUM(usd_revenue) GROUP BY product_id, product_name`
* **Table**: `kpi_product_revenue`
* **Description**: Revenue breakdown by individual products

### 6.3 Customer Metrics

**7. Top Customers by Revenue**
* **Formula**: `SUM(usd_revenue) GROUP BY customer_id ORDER BY revenue DESC LIMIT 10`
* **Table**: `kpi_top_10_customers`
* **Top Customer**: Customer_274 - $110,858.74
* **Description**: Customers contributing the most revenue

**8. Invoices per Customer**
* **Formula**: `COUNT(invoice_id) GROUP BY customer_id`
* **Table**: `kpi_invoices_per_customer`
* **Description**: Measures customer purchase frequency

**9. Discount Metrics**
* **Formula**: `SUM(discount), AVG(discount) GROUP BY customer_id`
* **Table**: `kpi_discount_metrics`
* **Description**: Total and average discount per customer

### 6.4 Operational Metrics

**10. Invoice Cancellation Rate**
* **Formula**: `(SUM(CASE WHEN status='Cancelled' THEN 1 ELSE 0) / COUNT(*)) * 100`
* **Table**: `kpi_invoice_cancellation_rate`
* **Current Value**: 25.6%
* **Description**: Percentage of cancelled invoices vs total invoices

**11. Invoice Cancellation DataFrame**
* **Table**: `kpi_invoice_cancellation_df`
* **Description**: Detailed cancellation analysis data

---

## 7. Technologies & Tools

### 7.1 Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Compute** | Databricks | Unified data analytics platform |
| **Processing** | PySpark | Distributed data processing |
| **Cloud Storage** | Azure Blob Storage | Raw data storage |
| **Orchestration** | Databricks Jobs | Workflow automation |
| **Security** | Databricks Secrets | Credential management |
| **Language** | Python | Data transformation logic |
| **SQL** | Spark SQL | Data queries and DDL |

### 7.2 Databricks Features Used

* **Unity Catalog**: Multi-layer namespace (catalog.schema.table)
* **Secrets Manager**: Secure credential storage
* **Jobs**: Automated pipeline execution
* **Notebooks**: Interactive development
* **DBFS**: Distributed file system

---

## 8. Execution Workflow

### 8.1 Manual Execution

```bash
# Step 1: Bronze Layer
Run notebook: de_mini_project/01_Bronze/01_NoteBooks/nb_bronze.ipynb

# Step 2: Silver Layer - Cleaning
Run notebook: de_mini_project/02_Silver/01_Notebooks/sl_cleaning_notebook.ipynb

# Step 3: Silver Layer - Transform
Run notebook: de_mini_project/02_Silver/01_Notebooks/sl_transform_notebook.ipynb

# Step 4: Gold Layer
Run notebook: de_mini_project/03_gold/01__notebooks/nb_gold.ipynb

# Step 5: Reporting Layer
Run notebook: de_mini_project/03_gold/01__notebooks/nb_kpi.ipynb

# Step 6: Datacube Layer
Run notebook: de_mini_project/03_gold/01__notebooks/nb_datacube.ipynb
```

### 8.2 Automated Execution

**Job Name**: `wholesale_distributor_pipeline`

**Schedule**: On-demand / Scheduled (can be configured)

**Dependencies**: Sequential execution with failure handling

---

## 9. Best Practices

### 9.1 Data Engineering

**Layered Architecture**: Separation of concerns (Bronze → Silver → Gold → Reporting)

**Idempotent Operations**: Overwrite mode ensures repeatable runs

**Schema Enforcement**: Explicit schemas in Bronze layer

**Data Quality**: Deduplication, null handling, standardization

**Column Naming**: Clean names without special characters

**Currency Normalization**: Consistent USD reporting

### 9.2 PySpark Optimization

**Broadcast Joins**: For small dimension tables

**Column Pruning**: Drop unnecessary columns early

**Partitioning**: Consider partitioning by date for large tables

### 9.3 Security

**Secrets Management**: Use Databricks Secrets for credentials

---

## Project Statistics

| Metric | Count |
|--------|-------|
| **Total Tables** | 30 |
| **Datacube Tables** | 2 |
| **Bronze Tables** | 7 |
| **Silver Tables** | 7 |
| **Gold Tables** | 5 |
| **Reporting Tables** | 11 |
| **Notebooks** | 5 |
| **KPIs** | 11 |
| **Source Files** | 7 CSV files |
| **Total Revenue** | $12.16M USD |
| **Total Quantity** | 34,151 units |
| **Cancellation Rate** | 25.6% |

---

## Project Structure

```
de_mini_project/
├── README.md
├── 01_Bronze/
│   └── 01_NoteBooks/
│       └── nb_bronze.ipynb
├── 02_Silver/
│   └── 01_Notebooks/
│       ├── sl_cleaning_notebook.ipynb
│       └── sl_transform_notebook.ipynb
└── 03_gold/
    └── 01__notebooks/
        ├── nb_gold.ipynb
        ├── nb_datacube.ipynb
        └── nb_kpi.ipynb
```

---

## Conclusion

This data engineering pipeline demonstrates enterprise-grade practices for building scalable, maintainable, and robust data platforms. The medallion architecture ensures data quality improves at each layer, while the dimensional model provides a solid foundation for business intelligence and analytics.

**Key Achievements**:
* Automated end-to-end data pipeline
* 30 production tables across 4 layers
* 11 business-critical KPIs
* Currency-normalized revenue reporting
* Scalable medallion architecture
* Production-ready code quality

---