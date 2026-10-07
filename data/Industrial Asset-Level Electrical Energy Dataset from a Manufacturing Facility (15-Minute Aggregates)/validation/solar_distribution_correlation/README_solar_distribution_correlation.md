# Solar Distribution Correlation Validation

This folder contains outputs from a focused raw-signal validation of the relationship between the following meters:

- `mi_b` — photovoltaic generation meter
- `sb_c` — solar distribution meter
- `mi_a` — grid import meter
- `hp_a` — heat pump 1
- `hp_b` — heat pump 2

## Purpose

The purpose of this validation is to assess whether the declared solar-distribution subnetwork is physically consistent with the observed raw meter signals.

In particular, the analysis tests whether the solar distribution meter behaves like a net boundary meter whose observed power is influenced by downstream heat-pump demand and photovoltaic generation.

## Method summary

The included script:

- reads raw Parquet files for the five meters listed above
- preserves raw signal polarity/sign
- converts the grid meter from W to kW
- aligns streams by nearest timestamp using an as-of merge
- evaluates several candidate balance models, including:

  - `solar_db ≈ hp_total - pv`
  - `solar_db ≈ pv - hp_total`
  - `solar_db ≈ hp_total`
  - `solar_db ≈ -hp_total`

Separate evaluations are also performed for:

- full-period data
- sunny periods (`pv > 0.5 kW`)
- night periods (`|pv| < 0.1 kW`)

## Included files

- `summary_metrics.txt`  
  Full-period model comparison metrics.

- `sunny_only_metrics.txt`  
  Model comparison metrics for periods with materially positive photovoltaic generation.

- `night_only_metrics.txt`  
  Model comparison metrics for periods with negligible photovoltaic generation.

- `validate_solar_db_balance.py`  
  Script used to generate the validation outputs.

## Interpretation

The included metrics support the following interpretation:

- Across the full observation period, the best-fitting model is `solar_db ≈ hp_total - pv`.
- During sunny periods, the same relationship remains the strongest candidate.
- During night periods, when photovoltaic generation is negligible, `solar_db ≈ hp_total` provides the best fit.

These results support the declared relationships in this subnetwork as a **physically coherent supply-context model**, while not implying a strict parent-child containment hierarchy.

## Notes

The script can also generate additional diagnostic artefacts, including aligned raw time series exports and PNG figures. These are not required to interpret the validation outcome and are therefore not included in the frozen package by default.