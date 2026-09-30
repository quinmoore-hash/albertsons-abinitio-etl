# albertsons-abinitio-etl

Batch ETL for Albertsons retail analytics, migrated from **Ab Initio** to
**PySpark**. The PySpark implementation lives in `spark/` (jobs, transforms,
schemas) with an Airflow DAG in `dags/`.

> **The Ab Initio artifacts (`mp/`, `xfr/`, `dml/`, `plan/`, `run/`, `dbc/`,
> `sand/`) are superseded by the PySpark package.** They are kept in place,
> unchanged, as the reference for parity verification and will be removed in
> a follow-up once the PySpark jobs are signed off in production. `sand/*.pset`
> and `dbc/edw.dbc` are still read by the PySpark config/JDBC layer.

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

## PySpark (current)

### Package layout

| Path | Replaces | Purpose |
|------|----------|---------|
| `spark/schemas.py` | `dml/*.dml` | Explicit `StructType` per record format (names, decimal precision, nullability) |
| `spark/common/config.py`, `pset.py` | `sand/*.pset` | `BatchConfig`: paths, `BUSINESS_DATE`, DQ thresholds |
| `spark/common/jdbc.py` | `dbc/edw.dbc`, `m_db load` | Oracle JDBC read/write (`append` / `truncate`) |
| `spark/common/notify.py` | `run/notify.ksh` | Failure email (`mailx`) + Slack webhook |
| `spark/transforms/pos_sales_cleanse.py` | `xfr/pos_sales_cleanse.xfr` | REFORMAT cleanse |
| `spark/transforms/dim_join.py` | `xfr/dim_join.xfr` | Broadcast dim join + unused-port rejects |
| `spark/transforms/sales_rollup.py` | `xfr/sales_rollup.xfr` | Daily sales ROLLUP |
| `spark/transforms/inventory_rollup.py` | `xfr/inventory_rollup.xfr` | Active-store filter + inventory ROLLUP |
| `spark/jobs/daily_pos_sales.py` | `mp/daily_pos_sales.mp`, `run/daily_pos_sales.ksh` | Job -> `EDW.F_DAILY_SALES_SUMMARY` |
| `spark/jobs/inventory_snapshot.py` | `mp/inventory_snapshot.mp`, `run/inventory_snapshot.ksh` | Job -> `EDW.F_INVENTORY_VALUE` |
| `spark/jobs/dq_check.py` | `run/dq_check.ksh` | Row-count / reject-% gate (exit 2 / 3 on failure) |
| `dags/nightly_batch.py` | `plan/nightly_batch.plan` | Airflow DAG `START -> daily_pos_sales -> inventory_snapshot -> dq_check -> END`, `30 2 * * *` |

### Setup

Requires Python 3.10+ and Java 17+.

```sh
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
```

### Running the jobs

Every job takes `BUSINESS_DATE` (`YYYYMMDD`, default: yesterday) and sources
`sand/project.pset` + `sand/sandbox.pset` from `--project-dir` (default: repo
root). Any pset parameter can be overridden with `--param NAME=VALUE`, and
extra pset files layered on with `--pset FILE`.

```sh
# files only (data/out/...), no EDW load
python -m spark.jobs.daily_pos_sales    20260713 --skip-load
python -m spark.jobs.inventory_snapshot 20260713 --skip-load

# DQ gate; the sample data is far below the production minimums
# (DQ_MIN_ROWCOUNT=1000, DQ_MAX_REJECT_PCT=2.0), so relax them for the demo
python -m spark.jobs.dq_check 20260713 --param DQ_MIN_ROWCOUNT=5 --param DQ_MAX_REJECT_PCT=20
python -m spark.jobs.dq_check 20260713 --notify   # production thresholds; fails with exit 2 + alert
```

The same scripts run under `spark-submit`, e.g.
`spark-submit spark/jobs/daily_pos_sales.py 20260713`.

Outputs are Spark output directories, each holding a single delimited part
file in the DML output format: `data/out/daily_sales_summary_<date>/`,
`data/out/reject_<date>/` (the join's unused port: rows whose UPC or store has
no dimension match; voids and zero-qty lines are dropped earlier by the
cleanse), and `data/out/inventory_value_<date>/`.

### EDW (Oracle) load

Without `--skip-load` the jobs load EDW over JDBC using `dbc/edw.dbc`
(`db_nodes`, `db_port`, `db_service`, schema `EDW`). Credentials come from the
environment exactly as in `dbc/edw.dbc.env`; provide the Oracle driver via
`EDW_JDBC_PACKAGES` (Maven coordinates) or `spark-submit --jars`.

```sh
export EDW_DB_USER=... EDW_DB_PASSWORD=...        # from the vault
export EDW_JDBC_PACKAGES=com.oracle.database.jdbc:ojdbc11:23.5.0.24.07
python -m spark.jobs.daily_pos_sales 20260713     # appends to EDW.F_DAILY_SALES_SUMMARY
python -m spark.jobs.inventory_snapshot 20260713  # truncate + load EDW.F_INVENTORY_VALUE
```

### Airflow

Point Airflow's `dags_folder` at `dags/` (or copy `dags/nightly_batch.py`).
Environment knobs: `PROJECT_DIR` (repo checkout), `SPARK_SUBMIT`,
`SPARK_SUBMIT_ARGS`, `NIGHTLY_BATCH_TZ`. Trigger params: `business_date`
(override) and `skip_load`. Any task failure calls the `run/notify.ksh`
equivalent (email `store-data-ops@albertsons.com`, Slack `#store-data-ops`).

### Tests

```sh
pytest                      # unit tests per transform + 20260713 parity vs tests/expected/
ruff check . && ruff format --check .
```

- `tests/test_parity_20260713.py` runs the full jobs on `data/in` + `data/serial`
  and compares byte-for-byte with `tests/expected/` (void line dropped, UPC
  `9999999999` in the reject file, store `4120` REMODEL excluded).
- `tests/test_nightly_batch_dag.py` is skipped unless `apache-airflow` is installed.
- `tests/test_edw_jdbc_integration.py` is skipped unless `EDW_IT_JDBC_URL`,
  `EDW_IT_USER`, `EDW_IT_PASSWORD`, `EDW_IT_JDBC_JAR` point at an Oracle
  schema with `F_DAILY_SALES_SUMMARY` / `F_INVENTORY_VALUE` tables.

## Running the legacy Ab Initio graphs (superseded)

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

Component mapping used by the migration:

| Ab Initio | PySpark equivalent |
|-----------|--------------------|
| INPUT FILE + DML | `spark.read` with an explicit `StructType` schema |
| REFORMAT + XFR | `select` / `withColumn` expressions |
| JOIN (unused port) | `df.join(..., "left")` + null-side split for rejects |
| SORT | (implicit; not required before a DataFrame `groupBy`) |
| ROLLUP + XFR | `groupBy(...).agg(...)` |
| OUTPUT FILE / `m_db load` | `df.write` to table / warehouse |
| Conduct>It plan | Airflow DAG / job scheduler |
