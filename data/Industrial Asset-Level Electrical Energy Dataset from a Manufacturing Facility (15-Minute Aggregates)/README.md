# Industrial Energy Monitoring Dataset

## Overview

This data package contains a processed industrial electricity dataset at 15-minute resolution, together with metadata, validation outputs, and reproducible analysis scripts.

Only aggregated data are published. Raw and intermediate layers are not included.

---

## Contents

```
industrial_energy_dataset_medallion/
│
├── code/
│   Scripts used to generate validation and summary outputs
│
├── data_parquet/
│   Gold-layer dataset (15-minute aggregates, partitioned Parquet)
│
├── data_csv/
│   Gold-layer dataset (15-minute aggregates, partitioned CSV)
│
├── metadata/
│   Asset definitions, relationships, schema, and data quality logs
│
├── validation/
│   ├── topology/
│   ├── solar_distribution_correlation/
│   └── dataset_quantitative_summary/
│
└── README.md
```

---

## Data description

The dataset consists of fixed 15-minute windows per asset, including:

* `Energy_kWh_15m` — energy per interval
* `Demand_kW` — average demand
* `AvgPower_kW_15m` — average power
* `DataCoveragePct`, `ReliableCoveragePct` — coverage metrics
* `IsReliableWindow` — reliability flag

Timestamps are in UTC.

---

## Metadata

The `metadata/` folder provides:

* asset list (`AssetList.csv`)
* declared relationships (`EdgeList.csv`)
* data quality log (`meter_data_quality_log.csv`)
* schema definition (`schema_gold_table.csv`)

Relationships represent **monitoring/supply context**, not strict electrical topology.

---

## Validation and summary outputs

The `validation/` folder contains:

* topology consistency checks (containment, closure, correlations)
* solar distribution behaviour analysis
* dataset-level quantitative summaries (coverage, energy totals, per-asset statistics)

---

## Reproducibility

Outputs based on the published 15-minute interval data can be regenerated using the scripts in `code/`.

Inputs are parameterised and should be pointed to the local `data_parquet/` directory.

---

## Notes

* The dataset reflects a partially observable industrial system.
* Minor inconsistencies may arise due to aggregation, measurement noise, or unmetered loads.

---

## License

This dataset is licensed under the Creative Commons Attribution 4.0 International License (CC BY 4.0).

© 2026 [Christopher Flynn / Munster Technological University]

---
