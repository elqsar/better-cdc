#!/usr/bin/env bash
#
# E2E Performance Test Script for CDC
# Generates INSERT/UPDATE/DELETE operations with configurable ratios.
#
# Usage: ./scripts/e2e-load-test.sh [OPTIONS]
#

set -euo pipefail

# Default configuration
TOTAL_OPS=100000
BATCH_MODE=false
BATCH_SIZE=10
PG_HOST="localhost"
PG_PORT="5432"
PG_USER="postgres"
PG_PASSWORD="postgres"
PG_DB="postgres"

# Operation ratios (must sum to 100)
INSERT_RATIO=60
UPDATE_RATIO=30
DELETE_RATIO=10

# Table ratios: orders get 2/3, accounts get 1/3
ORDER_RATIO=67

# Tracking arrays
declare -a ACCOUNT_IDS=()
declare -a ORDER_IDS=()

# Counters
INSERTS=0
UPDATES=0
DELETES=0
TOTAL_DONE=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    cat << EOF
E2E Performance Test Script for CDC

Usage: $0 [OPTIONS]

Options:
  -n, --operations NUM   Total operations to perform (default: $TOTAL_OPS)
  -b, --batch            Enable batch mode (multiple ops per transaction)
  -s, --batch-size NUM   Operations per batch in batch mode (default: $BATCH_SIZE)
  -H, --host HOST        PostgreSQL host (default: $PG_HOST)
  -p, --port PORT        PostgreSQL port (default: $PG_PORT)
  -U, --user USER        PostgreSQL user (default: $PG_USER)
  -d, --database DB      PostgreSQL database (default: $PG_DB)
  --help                 Show this help message

Operation Distribution:
  - INSERT: ${INSERT_RATIO}%
  - UPDATE: ${UPDATE_RATIO}%
  - DELETE: ${DELETE_RATIO}%

Table Distribution:
  - orders: ${ORDER_RATIO}%
  - accounts: $((100 - ORDER_RATIO))%

Examples:
  $0                     # Run with defaults (100k ops, single mode)
  $0 -n 10000            # Run 10k operations
  $0 -n 50000 -b         # Run 50k ops in batch mode
  $0 -H db.example.com   # Connect to remote host
EOF
    exit 0
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--operations)
            TOTAL_OPS="$2"
            shift 2
            ;;
        -b|--batch)
            BATCH_MODE=true
            shift
            ;;
        -s|--batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        -H|--host)
            PG_HOST="$2"
            shift 2
            ;;
        -p|--port)
            PG_PORT="$2"
            shift 2
            ;;
        -U|--user)
            PG_USER="$2"
            shift 2
            ;;
        -d|--database)
            PG_DB="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Build psql connection string
export PGPASSWORD="$PG_PASSWORD"
PSQL_CMD="psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -t -A"

# Execute SQL and return result
exec_sql() {
    $PSQL_CMD -c "$1" 2>/dev/null
}

# Execute SQL batch
exec_sql_batch() {
    $PSQL_CMD << EOF
$1
EOF
}

# Generate random email
random_email() {
    echo "user_${RANDOM}_${RANDOM}@loadtest.com"
}

# Generate random cents (100 to 99999)
random_cents() {
    echo $((RANDOM % 99900 + 100))
}

# Pick random status for accounts
random_account_status() {
    local statuses=("active" "inactive" "suspended" "pending")
    echo "${statuses[$((RANDOM % ${#statuses[@]}))]}"
}

# Pick random status for orders
random_order_status() {
    local statuses=("pending" "processing" "shipped" "delivered" "cancelled")
    echo "${statuses[$((RANDOM % ${#statuses[@]}))]}"
}

# Get random account ID
random_account_id() {
    if [[ ${#ACCOUNT_IDS[@]} -eq 0 ]]; then
        echo ""
        return
    fi
    echo "${ACCOUNT_IDS[$((RANDOM % ${#ACCOUNT_IDS[@]}))]}"
}

# Get random order ID
random_order_id() {
    if [[ ${#ORDER_IDS[@]} -eq 0 ]]; then
        echo ""
        return
    fi
    echo "${ORDER_IDS[$((RANDOM % ${#ORDER_IDS[@]}))]}"
}

# Remove account ID from tracking array
remove_account_id() {
    local val=$1
    local new_arr=()
    for item in "${ACCOUNT_IDS[@]}"; do
        [[ "$item" != "$val" ]] && new_arr+=("$item")
    done
    ACCOUNT_IDS=(${new_arr[@]+"${new_arr[@]}"})
}

# Remove order ID from tracking array
remove_order_id() {
    local val=$1
    local new_arr=()
    for item in "${ORDER_IDS[@]}"; do
        [[ "$item" != "$val" ]] && new_arr+=("$item")
    done
    ORDER_IDS=(${new_arr[@]+"${new_arr[@]}"})
}

# INSERT operation
do_insert() {
    local table_roll=$((RANDOM % 100))

    if [[ $table_roll -lt $ORDER_RATIO ]] && [[ ${#ACCOUNT_IDS[@]} -gt 0 ]]; then
        # Insert into orders (need existing account)
        local account_id
        account_id=$(random_account_id)
        local cents
        cents=$(random_cents)
        local status
        status=$(random_order_status)

        local sql="INSERT INTO orders (account_id, total_cents, status) VALUES ($account_id, $cents, '$status') RETURNING id;"
        local new_id
        new_id=$(exec_sql "$sql" | tr -d '[:space:]')

        if [[ -n "$new_id" ]]; then
            ORDER_IDS+=("$new_id")
        fi
    else
        # Insert into accounts
        local email
        email=$(random_email)
        local status
        status=$(random_account_status)

        local sql="INSERT INTO accounts (email, status) VALUES ('$email', '$status') RETURNING id;"
        local new_id
        new_id=$(exec_sql "$sql" | tr -d '[:space:]')

        if [[ -n "$new_id" ]]; then
            ACCOUNT_IDS+=("$new_id")
        fi
    fi

    ((INSERTS++))
}

# UPDATE operation
do_update() {
    local table_roll=$((RANDOM % 100))

    if [[ $table_roll -lt $ORDER_RATIO ]] && [[ ${#ORDER_IDS[@]} -gt 0 ]]; then
        # Update orders
        local order_id
        order_id=$(random_order_id)
        local cents
        cents=$(random_cents)
        local status
        status=$(random_order_status)

        exec_sql "UPDATE orders SET total_cents = $cents, status = '$status', updated_at = now() WHERE id = $order_id;" > /dev/null
    elif [[ ${#ACCOUNT_IDS[@]} -gt 0 ]]; then
        # Update accounts
        local account_id
        account_id=$(random_account_id)
        local status
        status=$(random_account_status)

        exec_sql "UPDATE accounts SET status = '$status', updated_at = now() WHERE id = $account_id;" > /dev/null
    else
        # No records to update, do insert instead
        do_insert
        return
    fi

    ((UPDATES++))
}

# DELETE operation
do_delete() {
    local table_roll=$((RANDOM % 100))

    if [[ $table_roll -lt $ORDER_RATIO ]] && [[ ${#ORDER_IDS[@]} -gt 0 ]]; then
        # Delete from orders (safe, no FK dependencies)
        local order_id
        order_id=$(random_order_id)

        exec_sql "DELETE FROM orders WHERE id = $order_id;" > /dev/null
        remove_order_id "$order_id"
    elif [[ ${#ACCOUNT_IDS[@]} -gt 0 ]]; then
        # Delete from accounts - first delete related orders
        local account_id
        account_id=$(random_account_id)

        # Delete orders for this account first (FK constraint)
        exec_sql "DELETE FROM orders WHERE account_id = $account_id;" > /dev/null
        # Remove those order IDs from tracking (we don't track by account, so just proceed)

        exec_sql "DELETE FROM accounts WHERE id = $account_id;" > /dev/null
        remove_account_id "$account_id"
    else
        # No records to delete, do insert instead
        do_insert
        return
    fi

    ((DELETES++))
}

# Select operation based on ratio
select_operation() {
    local roll=$((RANDOM % 100))

    if [[ $roll -lt $INSERT_RATIO ]]; then
        do_insert
    elif [[ $roll -lt $((INSERT_RATIO + UPDATE_RATIO)) ]]; then
        do_update
    else
        do_delete
    fi
}

# Generate batch SQL - writes SQL to temp file and returns count info
# Args: max_ops sql_file
# Returns: "ops_count:inserts:updates:deletes" via stdout
generate_batch_sql() {
    local max_ops=$1
    local sql_file=$2
    local ops_in_batch=0
    local batch_inserts=0
    local batch_updates=0
    local batch_deletes=0

    echo "BEGIN;" > "$sql_file"

    while [[ $ops_in_batch -lt $max_ops ]]; do
        local roll=$((RANDOM % 100))
        local table_roll=$((RANDOM % 100))

        if [[ $roll -lt $INSERT_RATIO ]]; then
            # INSERT
            if [[ $table_roll -lt $ORDER_RATIO ]] && [[ ${#ACCOUNT_IDS[@]} -gt 0 ]]; then
                local account_id
                account_id=$(random_account_id)
                local cents
                cents=$(random_cents)
                local status
                status=$(random_order_status)
                echo "INSERT INTO orders (account_id, total_cents, status) VALUES ($account_id, $cents, '$status');" >> "$sql_file"
            else
                local email
                email=$(random_email)
                local status
                status=$(random_account_status)
                echo "INSERT INTO accounts (email, status) VALUES ('$email', '$status');" >> "$sql_file"
            fi
            ((batch_inserts++))
        elif [[ $roll -lt $((INSERT_RATIO + UPDATE_RATIO)) ]]; then
            # UPDATE
            if [[ $table_roll -lt $ORDER_RATIO ]] && [[ ${#ORDER_IDS[@]} -gt 0 ]]; then
                local order_id
                order_id=$(random_order_id)
                local cents
                cents=$(random_cents)
                local status
                status=$(random_order_status)
                echo "UPDATE orders SET total_cents = $cents, status = '$status', updated_at = now() WHERE id = $order_id;" >> "$sql_file"
                ((batch_updates++))
            elif [[ ${#ACCOUNT_IDS[@]} -gt 0 ]]; then
                local account_id
                account_id=$(random_account_id)
                local status
                status=$(random_account_status)
                echo "UPDATE accounts SET status = '$status', updated_at = now() WHERE id = $account_id;" >> "$sql_file"
                ((batch_updates++))
            else
                local email
                email=$(random_email)
                local status
                status=$(random_account_status)
                echo "INSERT INTO accounts (email, status) VALUES ('$email', '$status');" >> "$sql_file"
                ((batch_inserts++))
            fi
        else
            # DELETE
            if [[ $table_roll -lt $ORDER_RATIO ]] && [[ ${#ORDER_IDS[@]} -gt 0 ]]; then
                local order_id
                order_id=$(random_order_id)
                echo "DELETE FROM orders WHERE id = $order_id;" >> "$sql_file"
                ((batch_deletes++))
            elif [[ ${#ACCOUNT_IDS[@]} -gt 1 ]]; then
                # Keep at least one account for order inserts
                local account_id
                account_id=$(random_account_id)
                echo "DELETE FROM orders WHERE account_id = $account_id;" >> "$sql_file"
                echo "DELETE FROM accounts WHERE id = $account_id;" >> "$sql_file"
                ((batch_deletes++))
            else
                local email
                email=$(random_email)
                local status
                status=$(random_account_status)
                echo "INSERT INTO accounts (email, status) VALUES ('$email', '$status');" >> "$sql_file"
                ((batch_inserts++))
            fi
        fi

        ((ops_in_batch++))
    done

    echo "COMMIT;" >> "$sql_file"
    echo "$ops_in_batch:$batch_inserts:$batch_updates:$batch_deletes"
}

# Pre-seed accounts for FK constraints
preseed_accounts() {
    log_info "Pre-seeding accounts for FK constraints..."

    local seed_count=100
    local sql="BEGIN;"

    for i in $(seq 1 $seed_count); do
        local email
        email=$(random_email)
        sql+=$'\n'"INSERT INTO accounts (email, status) VALUES ('$email', 'active');"
    done
    sql+=$'\n'"COMMIT;"

    exec_sql_batch "$sql" > /dev/null

    # Fetch inserted account IDs
    local ids
    ids=$(exec_sql "SELECT id FROM accounts ORDER BY id DESC LIMIT $seed_count;")
    while IFS= read -r id; do
        [[ -n "$id" ]] && ACCOUNT_IDS+=("$id")
    done <<< "$ids"

    log_info "Pre-seeded ${#ACCOUNT_IDS[@]} accounts"
}

# Print progress
print_progress() {
    local pct=$((TOTAL_DONE * 100 / TOTAL_OPS))
    local elapsed=$((SECONDS - START_TIME))
    local ops_per_sec=0
    [[ $elapsed -gt 0 ]] && ops_per_sec=$((TOTAL_DONE / elapsed))

    printf "\r[%3d%%] %d/%d ops | I:%d U:%d D:%d | %d ops/sec | accounts:%d orders:%d    " \
        "$pct" "$TOTAL_DONE" "$TOTAL_OPS" "$INSERTS" "$UPDATES" "$DELETES" \
        "$ops_per_sec" "${#ACCOUNT_IDS[@]}" "${#ORDER_IDS[@]}"
}

# Main execution
main() {
    log_info "Starting E2E Load Test"
    log_info "Configuration:"
    log_info "  Total operations: $TOTAL_OPS"
    log_info "  Batch mode: $BATCH_MODE"
    [[ "$BATCH_MODE" == "true" ]] && log_info "  Batch size: $BATCH_SIZE"
    log_info "  PostgreSQL: $PG_HOST:$PG_PORT/$PG_DB"
    log_info "  Ratios: INSERT=${INSERT_RATIO}% UPDATE=${UPDATE_RATIO}% DELETE=${DELETE_RATIO}%"
    echo

    # Test connection
    if ! exec_sql "SELECT 1;" > /dev/null 2>&1; then
        log_error "Failed to connect to PostgreSQL at $PG_HOST:$PG_PORT"
        log_error "Make sure docker compose is running: docker compose up -d"
        exit 1
    fi
    log_info "Connected to PostgreSQL"

    # Pre-seed
    preseed_accounts

    # Also load any existing IDs from DB
    log_info "Loading existing record IDs..."
    local existing_accounts
    existing_accounts=$(exec_sql "SELECT id FROM accounts;")
    while IFS= read -r id; do
        [[ -n "$id" ]] && ! [[ " ${ACCOUNT_IDS[*]} " =~ " $id " ]] && ACCOUNT_IDS+=("$id")
    done <<< "$existing_accounts"

    local existing_orders
    existing_orders=$(exec_sql "SELECT id FROM orders;")
    while IFS= read -r id; do
        [[ -n "$id" ]] && ORDER_IDS+=("$id")
    done <<< "$existing_orders"

    log_info "Loaded ${#ACCOUNT_IDS[@]} accounts, ${#ORDER_IDS[@]} orders"
    echo

    START_TIME=$SECONDS

    if [[ "$BATCH_MODE" == "true" ]]; then
        log_info "Running in BATCH mode..."
        local sql_file
        sql_file=$(mktemp)
        trap "rm -f $sql_file" EXIT

        while [[ $TOTAL_DONE -lt $TOTAL_OPS ]]; do
            # Calculate how many ops for this batch
            local remaining=$((TOTAL_OPS - TOTAL_DONE))
            local batch_count=$BATCH_SIZE
            [[ $remaining -lt $batch_count ]] && batch_count=$remaining

            # Generate batch - returns counts, writes SQL to temp file
            local counts
            counts=$(generate_batch_sql "$batch_count" "$sql_file")

            # Parse counts (ops:inserts:updates:deletes)
            local ops_count ins_count upd_count del_count
            IFS=':' read -r ops_count ins_count upd_count del_count <<< "$counts"

            # Execute the batch from file
            $PSQL_CMD < "$sql_file" > /dev/null 2>&1 || true

            # Update counters in parent shell
            ((TOTAL_DONE += ops_count))
            ((INSERTS += ins_count))
            ((UPDATES += upd_count))
            ((DELETES += del_count))

            # Refresh IDs periodically (every 10 batches)
            if [[ $((TOTAL_DONE % (BATCH_SIZE * 10))) -eq 0 ]]; then
                # Quick refresh of account IDs for FK
                local new_accounts
                new_accounts=$(exec_sql "SELECT id FROM accounts ORDER BY id DESC LIMIT 50;")
                while IFS= read -r id; do
                    [[ -n "$id" ]] && ! [[ " ${ACCOUNT_IDS[*]} " =~ " $id " ]] && ACCOUNT_IDS+=("$id")
                done <<< "$new_accounts"
            fi

            print_progress
        done
    else
        log_info "Running in SINGLE operation mode..."
        # Calculate progress interval (at least every 1% or every 50 ops, whichever is smaller)
        local progress_interval=$((TOTAL_OPS / 100))
        [[ $progress_interval -lt 1 ]] && progress_interval=1
        [[ $progress_interval -gt 50 ]] && progress_interval=50

        while [[ $TOTAL_DONE -lt $TOTAL_OPS ]]; do
            select_operation
            ((TOTAL_DONE++))

            # Print progress at calculated interval
            [[ $((TOTAL_DONE % progress_interval)) -eq 0 ]] && print_progress
        done
        # Final progress update
        print_progress
    fi

    echo
    echo

    local elapsed=$((SECONDS - START_TIME))
    local ops_per_sec=0
    [[ $elapsed -gt 0 ]] && ops_per_sec=$((TOTAL_DONE / elapsed))

    log_info "Load test completed!"
    echo
    echo "=== Summary ==="
    echo "Total operations: $TOTAL_DONE"
    echo "  INSERTs: $INSERTS ($((INSERTS * 100 / TOTAL_DONE))%)"
    echo "  UPDATEs: $UPDATES ($((UPDATES * 100 / TOTAL_DONE))%)"
    echo "  DELETEs: $DELETES ($((DELETES * 100 / TOTAL_DONE))%)"
    echo "Duration: ${elapsed}s"
    echo "Throughput: $ops_per_sec ops/sec"
    echo "Final state: ${#ACCOUNT_IDS[@]} accounts, ${#ORDER_IDS[@]} orders tracked"
}

main
