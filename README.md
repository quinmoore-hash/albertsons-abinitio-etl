# albertsons-abinitio-etl

Legacy **Ab Initio** batch ETL for Albertsons retail analytics. This is the
*original* codebase slated for migration to **PySpark**. It intentionally
uses the classic Ab Initio artifact layout (graphs, DML, XFR, DBC, sandbox
parameters, and a Conduct>It plan) so the migration can be demonstrated
end to end.

> Scope: two nightly pipelines feeding the Enterprise Data Warehouse (EDW):
> daily POS sales aggregation and an on-hand inventory valuation snapshot.

## Pipelines

### 1. `daily_pos_sales` (`mp/daily_pos_sales.mp`)
Aggregates raw Point-of-Sale register lines into a daily sales fact.

```
INPUT FILE (pos_sales)    REFORMAT       SORT        JOIN            SORT       ROLLUP        OUTPUT FILE
raw register feed  ---->  cleanse  ----> upc  ----> dim lookup ---> key -----> daily grain -> daily_sales_summary
                          (voids,                   (product_dim,              (txn_count,    -> EDW.F_DAILY_SALES_SUMMARY
                           tender norm,              store_dim)                 net_sales,
                           null fill)                    |                      PL sales,
                                                     unused -> reject file      loyalty)
```

### 2. `inventory_snapshot` (`mp/inventory_snapshot.mp`)
Values on-hand inventory per store and department.

```
INPUT FILE (inventory) -> FILTER_BY_EXPRESSION (active stores) -> SORT -> ROLLUP -> OUTPUT FILE
                                                                          (cost & retail value) -> EDW.F_INVENTORY_VALUE
```

Both are orchestrated nightly by `plan/nightly_batch.plan` (Conduct>It),
followed by a data-quality gate (`run/dq_check.ksh`).

## Repository layout

| Path | Ab Initio artifact | Purpose |
|------|--------------------|---------|
| `mp/`   | Graphs (`.mp`)          | Component/flow definitions (checked-in text form) |
| `dml/`  | Record formats (`.dml`) | Input/output/lookup schemas |
| `xfr/`  | Transforms (`.xfr`)     | REFORMAT / JOIN / ROLLUP business logic |
| `run/`  | Deployed scripts (`.ksh`)| Runnable Co>Op scripts generated from graphs |
| `dbc/`  | DB config (`.dbc`)      | EDW (Oracle) connection profile |
| `sand/` | Sandbox/project params  | `PROJECT_DIR`, path mappings, DQ thresholds |
| `plan/` | Conduct>It plan         | Nightly orchestration + dependencies |
| `data/` | Staging                 | `in/` feeds, `serial/` dims, `out/` results (sample data included) |

## Running locally (demo)

The deployed `.ksh` scripts assume a Co>Operating System install
(`m_dump`, `m_sort`, `m_join`, `m_rollup`, `m_db`). On a machine with
Ab Initio installed:

```sh
export PROJECT_DIR=$(pwd)
. sand/project.pset
run/daily_pos_sales.ksh 20260713
run/inventory_snapshot.ksh 20260713
run/dq_check.ksh 20260713
```

Sample inputs for business date `20260713` are provided under `data/in/`
and dimension lookups under `data/serial/`. The POS feed deliberately
includes a voided line and an unknown UPC (`9999999999`) to exercise the
reject path.

## Target grains

- **`EDW.F_DAILY_SALES_SUMMARY`** — `business_date x store x department x category`
  (txn_count, unit_qty, gross/net sales, discount, private-label sales, loyalty txns)
- **`EDW.F_INVENTORY_VALUE`** — `snapshot_date x store x department`
  (sku_count, on_hand_units, cost_value, retail_value)

## Migration notes (Ab Initio -> PySpark)

Rough component mapping for the migration exercise:

| Ab Initio | PySpark equivalent |
|-----------|--------------------|
| INPUT FILE + DML | `spark.read` with an explicit `StructType` schema |
| REFORMAT + XFR | `select` / `withColumn` expressions |
| JOIN (unused port) | `df.join(..., "left")` + null-side split for rejects |
| SORT | (implicit; not required before a DataFrame `groupBy`) |
| ROLLUP + XFR | `groupBy(...).agg(...)` |
| OUTPUT FILE / `m_db load` | `df.write` to table / warehouse |
| Conduct>It plan | Airflow DAG / job scheduler |
