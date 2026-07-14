;;; ============================================================
;;; inventory_snapshot.mp
;;; Ab Initio GDE graph (checked-in text form).
;;; Loads the nightly on-hand inventory extract, filters to active
;;; stores, and computes on-hand retail value per store/department.
;;;
;;;   INPUT FILE (inv) --> FILTER_BY_EXPRESSION (active) --> SORT
;;;                                                            |
;;;                                                         ROLLUP
;;;                                                            |
;;;                                              OUTPUT FILE (inv_value)
;;; ============================================================

graph inventory_snapshot
  parameter AI_SERIAL        = "${AI_SERIAL}"
  parameter INV_INPUT_FILE   = "${AI_SERIAL}/inventory_${BUSINESS_DATE}.dat"
  parameter INV_OUT_FILE     = "${AI_SERIAL}/inventory_value_${BUSINESS_DATE}.dat"

  component INPUT_FILE read_inventory
    layout        = serial
    file          = "${INV_INPUT_FILE}"
    record_format = "$PROJECT_DIR/dml/inventory.dml"

  component FILTER_BY_EXPRESSION active_only
    select_expr   = "in.on_hand_qty >= 0 && in.store_status == \"OPEN\""

  component SORT sort_inv
    key           = { store_id; department }

  component ROLLUP inv_value
    key           = { store_id; department }
    transform     = "$PROJECT_DIR/xfr/inventory_rollup.xfr"

  component OUTPUT_FILE write_inv
    layout        = serial
    file          = "${INV_OUT_FILE}"
    record_format = "$PROJECT_DIR/dml/inventory_value.dml"

  flow read_inventory.out -> active_only.in
  flow active_only.out    -> sort_inv.in
  flow sort_inv.out       -> inv_value.in
  flow inv_value.out      -> write_inv.in
end graph
