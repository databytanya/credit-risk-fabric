
# DAX Measures

## Default Rate
```dax
Default Rate =
DIVIDE(SUM(gold_loan_summary[is_default]), COUNTROWS(gold_loan_summary))
```

## Total Funded
```dax
Total Funded =
SUM(gold_loan_summary[funded_amnt])
```

## Avg Interest Yield
```dax
Avg Interest Yield =
CALCULATE(
    AVERAGE(gold_loan_summary[int_rate]),
    gold_loan_summary[loan_status_std] <> "Defaulted"
)
```

## Recovery Rate
```dax
Recovery Rate =
DIVIDE(
    SUM(gold_loan_summary[recoveries]),
    SUMX(FILTER(gold_loan_summary, [is_default] = 1), [funded_amnt])
)
```
