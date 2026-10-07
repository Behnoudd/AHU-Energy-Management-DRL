# Metadata

This folder contains supporting metadata describing the structure, relationships, and quality of the energy monitoring dataset.

## Files

### AssetList.csv
Defines the set of monitored assets.

Typical fields:
- `AssetId` — unique identifier used throughout the dataset
- `AssetType` — high-level classification (e.g. Press, Compressor, HVAC, Supply)
- Additional descriptive or grouping fields where applicable

This table provides the canonical list of assets referenced in the Gold-layer dataset.

---

### EdgeList.csv
Defines declared relationships between assets.

Columns:
- `Source` — upstream or supplying asset
- `Target` — downstream or receiving asset
- `RelationshipNote` — description of the relationship (e.g. supply, distribution)

These relationships represent **monitoring or supply context**, not necessarily strict electrical containment.

They are used in validation analyses (e.g. topology verification) to test consistency between declared structure and measured energy flows.

---

### meter_data_quality_log.csv
Provides asset-level data quality diagnostics.

Typical fields may include:
- coverage metrics
- reliability flags
- anomaly indicators

This file supports interpretation of validation results by identifying assets with incomplete or lower-quality data.

---

### schema_gold_table.csv
Describes the schema of the Gold-layer dataset.

Includes:
- column names
- data types
- units (e.g. kWh, kW)
- semantic descriptions

This serves as the formal reference for interpreting Gold-layer data fields.

---

## Notes

- All AssetIds referenced in the dataset and validation outputs correspond to entries in `AssetList.csv`.
- Relationship definitions in `EdgeList.csv` are used as inputs to topology validation scripts.
- Metadata files are static and apply to the full dataset unless otherwise noted.