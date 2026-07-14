# PySpark migration of the Albertsons nightly Ab Initio batch

This directory re-implements the two legacy Ab Initio nightly pipelines as
PySpark jobs that produce **row/value-equivalent output** to the original
Co>Operating System graphs. The Ab Initio artifacts (`mp/`, `dml/`, `xfr/`,
`run/`, `plan/`) are intentionally left in place so both implementations can be
run and diffed side by side.

```
pyspark/
├── jobs/
│   ├── daily_pos_sales.py      # port of mp/daily_pos_sales.mp + XFRs
│   └── inventory_snapshot.py   # port of mp/inventory_snapshot.mp + XFR
├── common/
│   ├── schemas.py              # StructType schemas mirroring dml/*.dml
│   ├── spark_session.py        # SparkSession builder + config
│   └── io.py                   # delimited read/write + optional EDW JDBC load
├── dq/
│   └── dq_check.py             # port of run/dq_check.ksh (row/reject gate)
├── dags/
│   └── nightly_batch_dag.py    # Airflow DAG port of plan/nightly_batch.plan
├── tests/                      # pytest with a local SparkSession fixture
├── conftest.py
└── requirements.txt
```

> **Note on the directory name.** `pyspark/` is *not* a Python package (it has no
> `__init__.py`) so it never shadows the real `pyspark` library. It is treated as
> the source root: the jobs add it to `sys.path` and import `common.*`; the tests
> rely on the `conftest.py` here doing the same.

## Setup

```sh
cd pyspark
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt      # pyspark, pytest
```

Requires a JDK (Java 8/11/17) on the machine for Spark.

## Running the jobs

Each job takes `--business-date YYYYMMDD` and a `--data-dir` (defaults to
`$AB_DATA_DIR` or `./data`). Run from the repository root:

```sh
# Daily POS sales aggregation -> data/out/daily_sales_summary_20260713.dat
#                              -> data/out/reject_20260713.dat
spark-submit pyspark/jobs/daily_pos_sales.py --business-date 20260713 --data-dir data

# Inventory value snapshot     -> data/out/inventory_value_20260713.dat
spark-submit pyspark/jobs/inventory_snapshot.py --business-date 20260713 --data-dir data

# Data-quality gate (exit 0 pass / 2 low rowcount / 3 reject pct too high)
python pyspark/dq/dq_check.py --business-date 20260713 --data-dir data
```

`python pyspark/jobs/<job>.py ...` also works when `pyspark` is `pip`-installed
(a local `SparkSession` is created on demand).

Add `--no-load-db` to skip the EDW JDBC load unconditionally (the load is also
skipped automatically when no credentials are present, so local/demo runs work
out of the box).

Sample inputs for business date `20260713` ship under `data/in/` and dimension
lookups under `data/serial/`. The POS feed deliberately includes a voided line
and an unknown UPC (`9999999999`) to exercise the reject path.

## EDW load (JDBC) environment variables

The `m_db load` steps are ported to an optional JDBC write (`common/io.load_to_edw`).
The target DDL and real credentials are **not** in the repo; connection info is
read from environment variables analogous to `dbc/edw.dbc`:

| Env var           | Default                              | Notes                       |
|-------------------|--------------------------------------|-----------------------------|
| `EDW_DB_USER`     | *(unset)*                            | required to enable the load |
| `EDW_DB_PASSWORD` | *(unset)*                            | required to enable the load |
| `EDW_DB_HOST`     | `edw-scan.albertsons.internal`       | Oracle SCAN host            |
| `EDW_DB_PORT`     | `1521`                               |                             |
| `EDW_DB_SERVICE`  | `EDWPRD.albertsons.internal`         | Oracle service name         |
| `EDW_DB_SCHEMA`   | `EDW`                                | default schema              |

The Oracle JDBC driver must be on the Spark classpath, e.g.:

```sh
spark-submit --packages com.oracle.database.jdbc:ojdbc8:21.9.0.0 \
  pyspark/jobs/daily_pos_sales.py --business-date 20260713 --data-dir data
```

Load modes preserve the legacy behavior:

* `daily_pos_sales`  -> `EDW.F_DAILY_SALES_SUMMARY` in **append** mode.
* `inventory_snapshot` -> `EDW.F_INVENTORY_VALUE` in **truncate/overwrite** mode
  (`truncate=true`, so the table DDL is kept).

## Orchestration (Airflow)

`dags/nightly_batch_dag.py` ports `plan/nightly_batch.plan`:

* Dependency chain `daily_pos_sales >> inventory_snapshot >> dq_check`.
* Cron schedule `30 2 * * *`.
* `BUSINESS_DATE` parameterised via a schema-validated `Param`
  (`^\d{8}$`, so triggering users can't inject shell metacharacters into the
  `BashOperator` commands), defaulting to the run's logical date (`ds`) in
  `YYYYMMDD` — which for the `30 2 * * *` schedule is "yesterday" at execution.
* An `on_failure_callback` equivalent to `run/notify.ksh` (email +
  `SLACK_WEBHOOK_URL` post to `#store-data-ops`).

Set `PROJECT_DIR` (repo root), `AB_DATA_DIR`, and optionally `SPARK_SUBMIT`,
`ALERT_EMAIL`, `ALERT_SLACK_CHANNEL`, `SLACK_WEBHOOK_URL` in the Airflow
environment. Airflow itself is not required to run the jobs or tests (it is
commented out in `requirements.txt`).

## Data-quality thresholds

`dq/dq_check.py` mirrors `run/dq_check.ksh` and reads thresholds from the
environment (defaults from `sand/sandbox.pset`):

| Env var              | Default | Meaning                                    |
|----------------------|---------|--------------------------------------------|
| `DQ_MIN_ROWCOUNT`    | `1000`  | fail (exit 2) if summary rows below this   |
| `DQ_MAX_REJECT_PCT`  | `2.0`   | fail (exit 3) if `reject/(summary+reject)*100` above this |

> The shipped 11-line sample produces 9 summary rows, so the **production**
> `DQ_MIN_ROWCOUNT=1000` intentionally fails on the sample. The tests assert both
> the failing production thresholds and passing demo thresholds.

## Tests

```sh
cd pyspark
python -m pytest -q
```

The suite (local `SparkSession` fixture in `conftest.py`) covers:

* **cleanse** edge cases — voided/zero-qty drop, tender `CC`→`CREDIT`, null
  `unit_price`/`discount_amt` fill, `ext_price` recompute vs. keep, string trims.
* **join/reject** — unknown UPC and unknown store land in the reject file in the
  `pos_sales` format; matched rows continue.
* **rollup** aggregates — line-grain `txn_count`, `unit_qty`, gross/net,
  private-label sales, loyalty count.
* **inventory** — active-store filter and cost/retail value calculation.
* **DQ gate** — pass / below-min / reject-pct branches.
* **end-to-end parity** — the sample feeds reproduce hand-computed expected
  output byte-for-byte, including the dropped void line and the reject row.

## Ab Initio → PySpark component mapping

| Ab Initio artifact / component            | PySpark equivalent                                                   |
|-------------------------------------------|----------------------------------------------------------------------|
| `INPUT FILE` + `dml/*.dml`                | `common.io.read_delimited` with an explicit `StructType` (`schemas.py`) |
| `dml/*.dml` record format                 | `StructType` in `common/schemas.py` (exact field order/type, delimiter) |
| `REFORMAT` + `xfr/pos_sales_cleanse.xfr`  | `jobs.daily_pos_sales.cleanse` (`select` + `filter` + `withColumn`)  |
| `SORT`                                    | implicit before `groupBy`; explicit `orderBy` for deterministic output |
| `JOIN` (`xfr/dim_join.xfr`, `unused-port`)| `jobs.daily_pos_sales.join_dims` (`df.join(..., "left")` + null-side reject split) |
| `ROLLUP` + `xfr/sales_rollup.xfr`         | `jobs.daily_pos_sales.rollup_sales` (`groupBy(...).agg(...)`)        |
| `FILTER_BY_EXPRESSION`                    | `jobs.inventory_snapshot.filter_active` (`df.filter(...)`)           |
| `ROLLUP` + `xfr/inventory_rollup.xfr`     | `jobs.inventory_snapshot.rollup_inventory`                           |
| `OUTPUT FILE`                             | `common.io.write_delimited` (single named `.dat`, no quoting)        |
| `m_db load ... -mode append/truncate`     | `common.io.load_to_edw` (JDBC `append` / `truncate` overwrite)       |
| `run/dq_check.ksh`                        | `dq/dq_check.py`                                                      |
| `run/notify.ksh`                          | `notify_failure` callback in `dags/nightly_batch_dag.py`             |
| `plan/nightly_batch.plan` (Conduct>It)    | `dags/nightly_batch_dag.py` (Airflow DAG, cron `30 2 * * *`)         |
