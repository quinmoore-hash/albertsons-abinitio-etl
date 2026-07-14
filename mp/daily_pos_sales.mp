;;; ============================================================
;;; daily_pos_sales.mp
;;; Ab Initio GDE graph (checked-in text form).
;;; Nightly aggregation of raw POS lines into the daily sales fact.
;;;
;;;   INPUT FILE (pos)  ---> REFORMAT (cleanse) ---> SORT (join key)
;;;                                                     |
;;;   INPUT FILE (product_dim) --> [lookup in1] ------->|
;;;   INPUT FILE (store_dim)   --> [lookup in2] ------> JOIN (dim_join)
;;;                                                        |
;;;                              unused --> OUTPUT FILE (reject)
;;;                                                        |
;;;                                                     SORT (rollup key)
;;;                                                        |
;;;                                                     ROLLUP (sales_rollup)
;;;                                                        |
;;;                                    OUTPUT FILE (daily_sales_summary)
;;; ============================================================

graph daily_pos_sales
  parameter AI_SERIAL          = "${AI_SERIAL}"
  parameter POS_INPUT_FILE     = "${AI_SERIAL}/pos_sales_${BUSINESS_DATE}.dat"
  parameter PRODUCT_DIM_FILE   = "${AI_SERIAL}/product_dim.dat"
  parameter STORE_DIM_FILE     = "${AI_SERIAL}/store_dim.dat"
  parameter SUMMARY_OUT_FILE   = "${AI_SERIAL}/daily_sales_summary_${BUSINESS_DATE}.dat"
  parameter REJECT_OUT_FILE    = "${AI_SERIAL}/reject_${BUSINESS_DATE}.dat"

  component INPUT_FILE read_pos
    layout       = serial
    file         = "${POS_INPUT_FILE}"
    record_format = "$PROJECT_DIR/dml/pos_sales.dml"

  component INPUT_FILE read_product_dim
    layout       = serial
    file         = "${PRODUCT_DIM_FILE}"
    record_format = "$PROJECT_DIR/dml/product_dim.dml"

  component INPUT_FILE read_store_dim
    layout       = serial
    file         = "${STORE_DIM_FILE}"
    record_format = "$PROJECT_DIR/dml/store_dim.dml"

  component REFORMAT cleanse
    transform    = "$PROJECT_DIR/xfr/pos_sales_cleanse.xfr"
    select       = is_valid_line
    reject-threshold = "Never Abort"

  component SORT sort_for_join
    key          = { upc }

  component JOIN dim_join
    join-type    = inner
    key          = in0.upc = in1.upc, in0.store_id = in2.store_id
    transform    = "$PROJECT_DIR/xfr/dim_join.xfr"
    unused-port  = enabled

  component SORT sort_for_rollup
    key          = { business_date; store_id; department; category }

  component ROLLUP sales_rollup
    key          = { business_date; store_id; department; category }
    transform    = "$PROJECT_DIR/xfr/sales_rollup.xfr"

  component OUTPUT_FILE write_summary
    layout       = serial
    file         = "${SUMMARY_OUT_FILE}"
    record_format = "$PROJECT_DIR/dml/daily_sales_summary.dml"

  component OUTPUT_FILE write_reject
    layout       = serial
    file         = "${REJECT_OUT_FILE}"
    record_format = "$PROJECT_DIR/dml/pos_sales.dml"

  flow read_pos.out            -> cleanse.in
  flow cleanse.out             -> sort_for_join.in
  flow sort_for_join.out       -> dim_join.in0
  flow read_product_dim.out    -> dim_join.in1
  flow read_store_dim.out      -> dim_join.in2
  flow dim_join.unused0        -> write_reject.in
  flow dim_join.out            -> sort_for_rollup.in
  flow sort_for_rollup.out     -> sales_rollup.in
  flow sales_rollup.out        -> write_summary.in
end graph
