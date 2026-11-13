# applab-senior-data-engineer-assessment_2026

A PySpark-based batch ETL pipeline for sales data, with data-quality validation, deduplication, product enrichment, currency conversion, exception handling, and SQL Server verification.

> **Assessment result:** The supplied batch contained 20 source rows. After removing 1 exact duplicate, 19 rows remained: 13 valid and 6 rejected. The rejection rate was 31.58%, above the configured 5% threshold. The pipeline correctly withheld `FactSales` publication and retained the rejected records and error events.

## 1. Project overview

This project demonstrates an end-to-end batch data-engineering workflow:

- Ingest sales and product-reference CSV files in Databricks.
- Enforce schemas and profile source data.
- Standardise and validate records against business rules.
- Remove exact duplicate rows.
- Enrich sales with product-reference attributes.
- Retrieve exchange rates through an API and calculate USD amounts, with configured fallback handling.
- Separate rejected records from individual validation-error events.
- Reconcile batch counts and enforce a 5% rejection threshold.
- Load exception outputs into SQL Server and verify the results.

## 2. Repository structure

```text
applab-senior-data-engineer-assessment/
├── README.md
├── notebooks/
│   └── AppLab_Advanced_ETL_Pipeline.ipynb
├── sql/
│   └── AppLab_SQLServer_Setup_And_Verification.sql
├── scripts/
│   └── AppLab_SQLServer_Import.py
├── architecture/
│   ├── AppLab_HLD_Improved.png
│   ├── AppLab_LLD_Improved.png
│   └── AppLab_Architecture_Improved.pdf
├── presentation/
│   └── AppLab_Senior_Data_Engineer_Assessment_Presentation_Complete.pptx
└── docs/
    └── technical_decisions_and_assumptions.md
```

The Databricks notebook (`.ipynb`) is the sole and primary ETL implementation deliverable. It contains the executable PySpark workflow together with its explanations and recorded execution outputs. No separate ETL `.py` source file is included in this submission.

## 3. Architecture: proposed versus implemented

### Proposed production architecture

The architecture diagrams describe a proposed Azure target state:

1. Source systems: sales/POS files, product reference data, customer data and FX API.
2. Azure Data Factory: scheduling, orchestration, parameters, dependencies and retries.
3. ADLS Gen2 Bronze: immutable raw landing data with run metadata.
4. Azure Databricks / PySpark Silver: schema enforcement, standardisation, validation, deduplication, enrichment and currency conversion.
5. ADLS Gen2 Gold: quality-approved fact and dimension datasets.
6. Azure SQL Database or Synapse SQL: serving layer for Power BI.
7. Cross-cutting controls: Microsoft Entra ID, managed identities, Key Vault, RBAC, private networking, monitoring, audit, lineage and CI/CD.

**These Azure services are proposed architecture, not infrastructure deployed for this assessment.**

### Implemented assessment solution

The work actually executed used Databricks Free Edition, PySpark, supplied CSV files, an exchange-rate API call, CSV exports, local Jupyter/Python and SQL Server Express. SQL Server exception-table loading was verified in SSMS.

## 4. PySpark pipeline

The notebook implements these logical stages:

1. Imports and pipeline configuration.
2. Source paths and run-specific output configuration.
3. Explicit schemas and CSV ingestion.
4. Source profiling.
5. Data standardisation and cleaning.
6. Business-rule validation.
7. Exact duplicate removal.
8. Product-reference enrichment.
9. Exchange-rate API integration and fallback handling.
10. Currency conversion to USD.
11. Rejected-record and error-event creation.
12. Reconciliation and rejection-rate calculation.
13. 5% batch quality gate.
14. Exception output archival/export and conditional `FactSales` export.
15. Final run status and reconciliation.

### Validation rules demonstrated

| Rule | Error code |
|---|---|
| Customer ID missing | `MISSING_CUSTOMER_ID` |
| Product reference not found | `PRODUCT_NOT_FOUND` |
| Order date missing | `MISSING_ORDER_DATE` |
| Order date invalid | `INVALID_ORDER_DATE` |
| Sales amount missing | `MISSING_SALE_AMOUNT` |
| Sales amount negative | `NEGATIVE_SALE_AMOUNT` |

One rejected record may generate multiple error events. For example, OrderID `1005` generated two error events but remained one rejected record.

## 5. Execution results

| Metric | Result |
|---|---:|
| Source rows | 20 |
| Exact duplicates removed | 1 |
| Rows after deduplication | 19 |
| Valid rows | 13 |
| Rejected rows | 6 |
| Rejection rate | 31.58% |
| Configured maximum | 5.00% |
| Quality gate | Failed — expected behaviour |

Reconciliation:

- `20 = 19 + 1` (source rows = deduplicated rows + duplicates removed)
- `19 = 13 + 6` (deduplicated rows = valid rows + rejected rows)
- `6 / 19 × 100 = 31.58%`

Because the rejection rate exceeded 5%, the pipeline withheld `FactSales` publication for this batch. Do not manually load the 13 otherwise-valid rows into `FactSales` for this failed batch; doing so would bypass the batch-level quality gate.

## 6. SQL Server implementation and verified results

The SQL Server database is `AppLabDB`, with these tables:

| Table | Purpose |
|---|---|
| `dbo.FactSales` | Accepted sales facts |
| `dbo.RejectedRecords` | Rejected source records and consolidated rejection reasons |
| `dbo.ErrorLogs` | Individual validation-error events |

`dbo.ErrorLogs.ErrorID` is an SQL Server `IDENTITY` column and is generated by SQL Server during inserts. The rejected-record `OrderDate` is stored as text to preserve invalid source values for audit.

### Loading approach used

Databricks Free Edition could not directly reach the locally hosted SQL Server Express instance in this assessment setup. The exception outputs were therefore exported as CSV, read in local Jupyter/Python and inserted into SQL Server using `pyodbc` with explicit column lists. The result was verified in SSMS.

**Observed SQL Server row counts:**

| Table | Verified rows |
|---|---:|
| `dbo.FactSales` | 0 |
| `dbo.RejectedRecords` | 6 |
| `dbo.ErrorLogs` | 7 |

The seven error events reconcile to six rejected records because OrderID `1005` has two errors.

## 7. Setup and run instructions

### A. Databricks

1. Open `notebooks/AppLab_Advanced_ETL_Pipeline.ipynb` in the Databricks workspace.
2. Ensure the source files are available at the paths configured in the notebook. The assessment paths used were:
   - `/Volumes/workspace/default/source_data/sales_data 2.csv`
   - `/Volumes/workspace/default/source_data/product_reference 2.csv`
3. Update the configured paths if your files are stored elsewhere.
4. Run the notebook from top to bottom.
5. Review the reconciliation output and quality-gate result.
6. Download the generated `RejectedRecords` and `ErrorLogs` CSV outputs from the run-specific export location. Download `FactSales` only when a batch passes the quality gate and that output is generated.

### B. SQL Server

1. Install SQL Server Express and SQL Server Management Studio (SSMS), if not already installed.
2. Run `sql/AppLab_SQLServer_Setup_And_Verification.sql` to create/check the database tables and prepare verification queries.
3. Confirm the SQL Server instance and database connection details.
4. On the Windows machine that can reach SQL Server, install Microsoft ODBC Driver 17 or 18 for SQL Server.
5. In local Jupyter, install Python dependencies:

```python
%pip install pandas pyodbc
```

6. Read the downloaded CSV files into Pandas, then load them with explicit destination column lists. Exclude `ErrorID` from the `ErrorLogs` insert because it is an identity column.
7. Verify row counts and error-event counts in SSMS using the SQL queries in the setup/verification script.

Example Windows Integrated Authentication connection:

```python
import pyodbc

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    r"SERVER=DESKTOP-PEIIU7J\SQLEXPRESS;"
    "DATABASE=AppLabDB;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(connection_string)
```

Change the driver name if ODBC Driver 17 is installed instead. Use your own SQL Server instance name if it differs. Do not commit passwords, API keys or tokens to GitHub.

> **Important:** The insert process appends rows. Check existing table counts before rerunning the load to avoid duplicates. Use a controlled truncate/reload or idempotent load strategy if you need to rerun it.

## 8. Technical trade-offs and assumptions

| Area | Assessment choice | Trade-off / production consideration |
|---|---|---|
| Source access | Supplied CSV files | Production should use governed source connectors and managed landing zones. |
| Compute | Databricks Free Edition | Suitable for assessment development; networking and service limits differ from production. |
| SQL connectivity | CSV handoff + local Jupyter/`pyodbc` | Enabled verified local loading despite hosted-to-local network restrictions. Production should use secure automated connectivity and orchestration. |
| Deduplication | Exact-row duplicate removal | Production should define a business key and record-survivorship rules. |
| Reject threshold | 5% of deduplicated rows | Threshold should be configurable and agreed with data owners. |
| Rejected data | Separate record-level output | Production should define retention, access and remediation processes. |
| Error logs | One row per individual validation issue | Production should add run ID, source file, rule version, severity and correlation metadata. |
| FX API | API lookup with configured fallback | Production should govern provider, effective date, freshness, caching, credentials and fallback approval. |
| SQL rejected date | `VARCHAR` | Preserves invalid source date text; accepted fact dates should be typed. |
| SQL load | Parameterised inserts in a transaction | Production should use batch-aware idempotency and controlled upserts/`MERGE`. |

### Assumptions

- The supplied CSVs are the assessment source extracts; no live OLTP database connection was provided.
- The sample is not a production-volume performance benchmark.
- Exact duplicate rows are considered duplicates for this exercise.
- Rejection rate is calculated as rejected rows divided by deduplicated rows.
- A rejected row may create more than one error event.
- The maximum allowed rejection rate is 5%.
- The Azure architecture is a proposed target design and was not deployed for this assessment.

## 9. Tools and technologies

- **Apache Spark / PySpark** — distributed data processing and transformations.
- **Databricks Free Edition** — notebook execution environment.
- **Python** — API integration and local loading utility.
- **Pandas** — local CSV inspection and preparation.
- **Requests** — exchange-rate API calls.
- **SQL Server Express / T-SQL / SSMS** — relational schema, loading and verification.
- **pyodbc** — local Python-to-SQL Server connectivity.
- **GitHub** — source control and submission repository.
- **Proposed target services:** Azure Data Factory, ADLS Gen2, Azure Databricks, Azure SQL Database or Synapse SQL, Key Vault, Microsoft Entra ID, Azure Monitor and Power BI.

## 10. Troubleshooting

| Issue | What to check |
|---|---|
| Source file not found in Databricks | Confirm Volume path, filename and permissions; update notebook configuration. |
| Exchange-rate API request fails | Check outbound HTTPS access, endpoint, timeout, response payload and fallback logs. |
| `pyodbc` driver not found | Install Microsoft ODBC Driver 17/18 and match the driver name in the connection string. |
| SQL Server connection fails | Confirm SQL Server service, instance name, authentication and database permissions. Jupyter must run on a machine that can reach the SQL Server instance. |
| Invalid object name | Confirm `AppLabDB`, `dbo` schema and table creation. |
| Duplicate rows after rerun | The sample insert is append-based; inspect counts and use a controlled reload/idempotent strategy. |
| `FactSales` output is absent | Check the rejection rate. For the supplied run, 31.58% correctly exceeded the 5% threshold. |

## 11. Final status

- PySpark ETL and data-quality workflow: executed for the supplied sample.
- Reconciliation and 5% quality gate: executed; batch correctly failed at 31.58%.
- Rejected-record output: generated and loaded; 6 rows verified.
- Error-event output: generated and loaded; 7 rows verified.
- `FactSales` for this batch: correctly withheld; 0 rows.
- SQL Server DDL and verification: completed.
- Azure production architecture: documented as proposed, not deployed.

**Conclusion:** The assessment demonstrates a batch ETL workflow that validates and enriches sales data, retains rejected records and individual error events, reconciles outcomes, and prevents a batch that exceeds the agreed rejection threshold from being published to the fact table.
