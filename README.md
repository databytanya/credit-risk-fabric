# credit-risk-fabric
End-to-end credit risk analytics platform built on Microsoft Fabric
# Credit Risk & Portfolio Analytics Platform

> A full end-to-end Microsoft Fabric project showcasing medallion lakehouse architecture,
> PySpark transformations, DirectLake reporting, and row-level security —
> built on the Lending Club loan dataset (50,000 rows sample).

---

## Overview

This project demonstrates how a modern data engineering stack on **Microsoft Fabric**
can power real-time credit risk monitoring across a large loan portfolio.
It is designed as a freelance portfolio piece, covering every layer from raw CSV
ingest through to a governed, executive-ready Power BI report.

A prospect or client reviewing this repo can expect to see:

- Production-grade medallion architecture (Bronze / Silver / Gold)
- Parameterised ingestion pipelines built for reuse across time slices
- PySpark notebooks with documented transformation logic
- A semantic model with DAX measures ready for a finance audience
- Row-Level Security scoped by regional portfolio manager role

---

## Architecture

```
Lending Club CSV (50k rows sample)
       │
       ▼
┌─────────────────────┐
│   Bronze layer       │  Raw CSV loaded into Delta table
│   bronze_loans_raw   │  17 columns selected, schema defined manually
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Silver layer       │  silver_loans_enriched
│                      │  → int_rate and dti % signs stripped
│                      │  → loan_status standardised into 3 buckets
│                      │  → fico_mid calculated
│                      │  → risk_band added (Low / Medium / High)
│                      │  → is_default flag added (1 or 0)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Gold layer         │  gold_loan_summary (fact table)
│                      │  gold_portfolio_monthly (trend data)
│                      │  gold_state_risk (geographic data)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Semantic model     │  DirectLake mode
│   sm_credit_risk     │  DAX measures
│                      │  Row-Level Security (RLS)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Power BI report    │  Page 1 — Executive portfolio dashboard
│                      │  Page 2 — Borrower risk drill-through
└─────────────────────┘
```

---

## Fabric features demonstrated

| Feature | Where used |
|---|---|
| Lakehouse (OneLake / Delta) | All three medallion layers |
| PySpark Notebooks | Bronze ingest, Silver cleaning, Gold aggregations |
| Manual schema definition | Bronze notebook — avoids inferSchema on large files |
| DirectLake semantic model | Zero-copy connection from Gold tables to Power BI |
| DAX measures | Default rate, LTV bands, interest yield, recovery rate |
| Row-Level Security | Regional portfolio manager roles (East / West) |

---

## Dataset

**Source:** [Lending Club Accepted Loans — Kaggle](https://www.kaggle.com/datasets/wordsforthewise/lending-club)

50,000 row sample used for this portfolio project.

Key fields used:

| Field | Description |
|---|---|
| `loan_amnt` | Requested loan amount ($) |
| `funded_amnt` | Amount actually funded ($) |
| `int_rate` | Interest rate (%) |
| `grade` / `sub_grade` | Lending Club risk grade (A–G) |
| `dti` | Debt-to-income ratio |
| `fico_range_low/high` | FICO score band |
| `loan_status` | Repayment status (raw) |
| `issue_d` | Loan origination date |
| `addr_state` | Borrower US state |
| `annual_inc` | Self-reported annual income |
| `total_pymnt` | Total payments received |
| `recoveries` | Post-default recovery amount |

> Raw data files are excluded from this repo via `.gitignore`.
> Download from Kaggle and upload to your Fabric Lakehouse at
> `Files/raw/lending_club/` before running the notebooks.

---

## Project structure

```
credit-risk-fabric/
│
├── README.md
├── .gitignore
│
├── docs/
│   ├── architecture.md          ← Layer by layer technical design
│   └── data-dictionary.md       ← Source and derived field definitions
│
├── notebooks/
│   ├── 01_bronze_register.ipynb ← Read CSV, write Bronze Delta table
│   ├── 02_silver_transform.ipynb← Clean, type fix, risk score
│   └── 03_gold_aggregates.ipynb ← Build Gold summary tables
│
└── semantic-model/
    └── dax-measures.md          ← All DAX measure definitions
```

---

## How to run this project

### Prerequisites

- Microsoft Fabric workspace (Trial or paid capacity)
- Lakehouse named `lh_credit_risk`
- Lending Club CSV downloaded from Kaggle

### Step 1 — Upload source data

In your Fabric Lakehouse create this path:
```
Files/raw/lending_club/
```
Upload `lending_club_5k.csv` or `lending_club_sample.csv` here.

### Step 2 — Run the notebooks in order

Import each notebook from the `notebooks/` folder into your
Fabric workspace and run them in this order:

```
01_bronze_register.ipynb   → creates bronze_loans_raw
02_silver_transform.ipynb  → creates silver_loans_enriched
03_gold_aggregates.ipynb   → creates gold_loan_summary
                              gold_portfolio_monthly
                              gold_state_risk
```

### Step 3 — Create the semantic model

In Fabric create a new Semantic model named `sm_credit_risk`.
Select the three Gold tables.
Add the DAX measures from `semantic-model/dax-measures.md`.
Add the RLS roles below.

### Step 4 — Build the Power BI report

Connect Power BI to `sm_credit_risk` via the OneLake data hub.
Build two report pages:
- **Page 1** — Executive portfolio dashboard
- **Page 2** — Borrower risk drill-through

---

## DAX measures

```dax
Default Rate =
DIVIDE(SUM(gold_loan_summary[is_default]), COUNTROWS(gold_loan_summary))

Total Funded =
SUM(gold_loan_summary[funded_amnt])

Avg Interest Yield =
CALCULATE(
    AVERAGE(gold_loan_summary[int_rate]),
    gold_loan_summary[loan_status_std] <> "Defaulted"
)

Recovery Rate =
DIVIDE(
    SUM(gold_loan_summary[recoveries]),
    SUMX(FILTER(gold_loan_summary, [is_default] = 1), [funded_amnt])
)
```

---

## Row-Level Security roles

| Role | Filter | Intended user |
|---|---|---|
| `RM_East` | NY, NJ, CT, MA | East coast portfolio managers |
| `RM_West` | CA, WA, OR, NV | West coast portfolio managers |
| `Executive` | No filter — full portfolio | C-suite, risk committee |

---

## Medallion layer summary

| Layer | Table | Rows | Key transformations |
|---|---|---|---|
| Bronze | `bronze_loans_raw` | 50,000 | Raw CSV, 17 columns selected |
| Silver | `silver_loans_enriched` | 50,000 | Types fixed, risk scored, status standardised |
| Gold | `gold_loan_summary` | 50,000 | Fact table for Power BI |
| Gold | `gold_portfolio_monthly` | ~500 | Grouped by month and grade |
| Gold | `gold_state_risk` | ~150 | Grouped by state and risk band |

---

## Key design decisions

**Why manual schema instead of inferSchema?**
On a free trial capacity, inferSchema reads the entire file twice
to guess column types — doubling compute usage and triggering
capacity throttle errors. Defining schema manually reads the file
once and is best practice for production pipelines regardless.

**Why three Gold tables instead of one?**
Power BI performs best with pre-aggregated tables.
Separating monthly and state aggregates avoids heavy GroupBy
operations inside DAX at query time, keeping dashboard interactions
fast even on large datasets.

**Why DirectLake instead of Import mode?**
DirectLake queries Delta tables in OneLake directly with no data
copy and no scheduled refresh. This means the report always shows
current data the moment a new notebook run completes.

---

## Report preview

![Executive dashboard](docs/dashboard-preview.png)

> Screenshot of the Power BI executive portfolio dashboard
> showing default rate, total funded, interest yield,
> and geographic risk distribution across US states.

---

## Author

Built by **[Your Name]** — Freelance Microsoft Fabric and Power BI consultant.

Available for data engineering, analytics engineering, and reporting
projects on Microsoft Fabric, Azure Synapse, and Power BI.

[LinkedIn](https://www.linkedin.com/in/tanyagulati96/) ·
[GitHub](https://github.com/databytanya)

---

## Licence

This project is released under the MIT Licence.
The Lending Club dataset is subject to Kaggle terms of use.
