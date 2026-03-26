# Global Wholesale Distributor Data Engineering Pipeline

## 1. Overview

This project implements an end-to-end **data engineering pipeline** using **PySpark, Databricks, and Azure Storage**. It processes raw wholesale distributor data into structured analytical datasets and KPIs.

The pipeline follows a **layered architecture (Medallion-style)** and is orchestrated using **Databricks Jobs**.

---

## 2. Architecture

The pipeline consists of three main layers:

RAW → STAGING → CURATED → KPI

| Stage   | Schema                                                       | Description           |
| ------- | ------------------------------------------------------------ | --------------------- |
| Raw     | global_wholesale_distributor.raw_wholesale_distributor       | Ingest raw data       |
| Staging | global_wholesale_distributor.staging_wholesale_distributor   | Clean & validate data |
| Curated | global_wholesale_distributor.curated_wholesale_distributor   | Business-ready data   |
| KPI     | global_wholesale_distributor.reporting_wholesale_distributor | Analytical metrics    |

---

## 3. Raw Layer (Ingestion)

### Objective

* Ingest raw data from **Azure Blob Storage (Containers)** into Delta tables.

### Features

* Schema-defined ingestion (no inference)
* Append-only loads
* Data stored in original format

---

## 4. Staging Layer (Data Cleaning)

### Objective

* Clean, standardize, and validate raw data.

### Transformations

* Deduplication using `.dropDuplicates()`
* Data type casting
* Column renaming for consistency
* Null handling and validation checks

---

## 5. Curated Layer (Business Modeling)

### Objective

* Build structured datasets for analytics.

### Tables

* Fact tables:

  * fact_invoice_line_items
  * fact_payments
* Dimension tables:

  * dim_invoices
  * dim_customers
  * dim_products

### Features

* Optimized joins
* Business-level schema
* Ready for KPI calculations

---

## 6. KPI Layer (Reporting)

### Objective

* Generate business metrics for analytics and dashboards.

### KPI Tables

* kpi_total_revenue
* kpi_total_quantity_sold
* kpi_average_invoice_value
* kpi_discount_metrics
* kpi_invoice_cancellation_rate
* kpi_invoices_per_customer
* kpi_revenue_by_region
* kpi_product_revenue

### Examples

* Total Revenue
* Average Invoice Value (AOV)
* Customer Discount Analysis
* Revenue by Region
* Invoice Cancellation Rate

---

## 7. Currency Conversion

* Revenue is standardized using **currency conversion**
* Ensures consistent reporting across regions
* Implemented during transformation stage

---

## 8. Orchestration

The pipeline is orchestrated using **Databricks Jobs**:

* Sequential execution:

  * Raw → Staging → Curated → KPI
* Ensures:

  * Data consistency
  * Dependency management
  * Automated workflows

---

## 9. Technologies Used

* PySpark
* Databricks
* Azure Blob Storage (Containers)
* Delta Lake

---

## 10. Best Practices

* Use of **Delta tables** for reliability
* Clean column naming (no special characters)
* Aggregations with aliasing
* Layered architecture for maintainability
* Efficient joins and transformations

---

## 11. Conclusion

This project demonstrates a complete **data engineering pipeline** from ingestion to analytics.

It showcases:

* Data ingestion from cloud storage
* Data transformation using PySpark
* Business modeling
* KPI generation
* Workflow orchestration using Databricks

---

## 🚀 Future Enhancements

* Add incremental loading
* Implement SCD (Slowly Changing Dimensions)
* Partitioning & performance optimization
* Dashboard integration (Power BI / Tableau)
