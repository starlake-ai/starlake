source ./env.sh

$SL_BIN_DIR/starlake.sh transform --name sales_kpi.byseller_kpi --interactive table
