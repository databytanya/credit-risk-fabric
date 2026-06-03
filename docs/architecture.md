
# Architecture

## Medallion layers

| Layer | Table | Tool | Purpose |
|---|---|---|---|
| Bronze | bronze_loans_raw | Pipeline | Raw CSV as Delta table |
| Silver | silver_loans_enriched | Notebook | Cleaned and risk scored |
| Gold | gold_loan_summary | Notebook | Fact table for Power BI |
| Gold | gold_portfolio_monthly | Notebook | Monthly aggregates |
| Gold | gold_state_risk | Notebook | State level risk data |

## Data flow
```
CSV → Pipeline → Bronze → Silver → Gold → Semantic model → Power BI
```
