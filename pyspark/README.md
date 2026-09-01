# PySpark port of the Albertsons Ab Initio batch

This package is a functional re-implementation of the Ab Initio graphs in
`mp/`, transforms in `xfr/` and the Conduct>It plan in `plan/`. The legacy
artifacts are left untouched so the two implementations can be diffed and run
side by side.

```
pyspark/
├── config.py              sand/project.pset + sand/sandbox.pset + dbc/edw.dbc(.env)
├── schemas.py             dml/*.dml  ->  StructType
├── io_utils.py            INPUT FILE / OUTPUT FILE / m_db load
├── daily_pos_sales.py     mp/daily_pos_sales.mp
├── inventory_snapshot.py  mp/inventory_snapshot.mp
├── dq_check.py            run/dq_check.ksh
├── notify.py              run/notify.ksh
├── dags/nightly_batch.py  plan/nightly_batch.plan
└── tests/                 parity tests on the sample data/in/ feeds
```

## Running locally

```sh
python -m venv .venv && . .venv/bin/activate
pip install -r pyspark/requirements.txt

export PROJECT_DIR=$(pwd)
export SKIP_DB_LOAD=1                 # demo runs: skip the Oracle load
cd pyspark

spark-submit --master 'local[*]' daily_pos_sales.py 20260713
spark-submit --master 'local[*]' inventory_snapshot.py 20260713
python dq_check.py 20260713           # DQ_MIN_ROWCOUNT=1000 fails on sample data
```

Outputs land in `data/out/` exactly where the `.ksh` scripts wrote them.

To load the EDW for real, drop `SKIP_DB_LOAD`, export `EDW_DB_USER` /
`EDW_DB_PASSWORD` (injected from the vault by the scheduler, as
`dbc/edw.dbc.env` did) and pass the Oracle driver:
`spark-submit --jars /opt/jdbc/ojdbc8.jar ...`. The JDBC URL is built from
`dbc/edw.dbc`: `jdbc:oracle:thin:@//edw-scan.albertsons.internal:1521/EDWPRD.albertsons.internal`.

Run the tests with:

```sh
cd pyspark && pytest tests -q
```

> The package directory is named `pyspark/` (as requested). It deliberately has
> no `__init__.py`, so `import pyspark` still resolves to the installed library
> — but always run scripts and tests from inside `pyspark/` (or with that
> directory on `PYTHONPATH`), never with the repository root first on the path.

## Airflow

`dags/nightly_batch.py` reproduces the plan's chain
`daily_pos_sales >> inventory_snapshot >> dq_check`, on the same `30 2 * * *`
schedule, with `notify.alert_store_data_ops` wired as `on_failure_callback`
(email + Slack, exactly like `run/notify.ksh`). `BUSINESS_DATE` is a DAG param;
when unset it defaults to the run's logical date formatted `YYYYMMDD`.

## Component mapping

| Ab Initio | PySpark |
|-----------|---------|
| `INPUT FILE` + `.dml` | `spark.read.csv` with an explicit `StructType` (`schemas.py`) |
| `REFORMAT` + `pos_sales_cleanse.xfr` | `daily_pos_sales.cleanse` |
| `select = is_valid_line` | `filter(void_flag != 'Y' and qty != 0)` |
| `FILTER_BY_EXPRESSION active_only` | `inventory_snapshot.active_only` |
| `SORT` | dropped (implicit before `groupBy`; final `orderBy` on the rollup key preserves output order) |
| `JOIN` + `dim_join.xfr` + unused port | `join(..., "left")` split into matched / reject frames |
| `ROLLUP` + `sales_rollup.xfr` | `groupBy(...).agg(...)` |
| `OUTPUT FILE` (serial) | `io_utils.write_serial_file` (single flat file, no header) |
| `m_db load -mode append` | `df.write.format("jdbc").mode("append")` |
| `m_db load -mode truncate` | `df.write.format("jdbc").mode("overwrite").option("truncate", true)` |
| `run/dq_check.ksh` | `dq_check.py` (same exit codes 2 / 3) |
| `plan/nightly_batch.plan` | `dags/nightly_batch.py` |
| `run/notify.ksh` | `notify.alert_store_data_ops` |

## Parity notes

* **Delimiters.** The `pos_sales.dml` header comment says "pipe-delimited", but
  its field specs (`decimal(",")`) and the sample data are comma-delimited; so
  are the two dimension files. The inventory feed and all target files are
  pipe-delimited. The reject file is written with the `pos_sales.dml` record
  format, so it stays comma-delimited like the input feed.
* **Types.** Ab Initio `decimal("")` is exact, so amounts are `DecimalType`,
  never floats — this keeps `decimal_round(x, 2)` (HALF_UP) semantics.
* **Rounding points** match the transforms exactly: `ext_price` is rounded per
  line, the rollup sums are rounded once in `finalize` and `net_sales` is
  rounded from the unrounded gross/discount sums.
* **`ext_price` reconciliation.** The feed value is kept only when
  `round(qty * unit_price, 2)` equals it; otherwise the recomputed value wins.
  The comparison uses the raw `unit_price` (as the XFR does) while the
  recomputation uses the null-filled one.
* **`txn_count`** is the line-grain count, not `count(distinct transaction_id)`
  — the legacy transform's documented approximation is preserved.
* **Reject routing.** Any line whose UPC misses `product_dim` or whose store
  misses `store_dim` goes to `data/out/reject_<BUSINESS_DATE>.dat`; the
  remaining lines flow to the rollup.
* **Load modes.** Sales append, inventory truncate/overwrite.
* Outputs must byte-match the legacy `.dat` files for the sample business date
  `20260713`; `tests/` asserts the expected rows and the exact first line of
  each output file, including that the voided line (`T0501`) is dropped and the
  unknown UPC `9999999999` lands in the reject path.
