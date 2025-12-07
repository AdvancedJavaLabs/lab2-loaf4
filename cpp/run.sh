#!/bin/bash
set -e

print_info() { echo -e "$1"; }

TEXT_FILE="${1}"
SENTENCES_PER_SECTION="${2}"
NUM_WORKERS="${3}"
TOP_WORDS="${4}"

check_dependencies() {    
    # RabbitMQ
    if ! systemctl is-active --quiet rabbitmq-server; then
        print_info "Starting rabbitmq"
        sudo systemctl start rabbitmq-server || {
            print_info "unable to start rabbitmq"
            exit 1
        }
    fi
}

compile_project() {
    rm -f aggregator.log producer.log
    for ((i=1; i<=NUM_WORKERS; i++)); do
        rm -f worker_$i.log
    done
    rm -f processed_text.txt report.txt sorted_text.txt

    make clean
    make -j4
}

start_workers() {
    pkill -f "./worker" || true
    sleep 1
    
    for ((i=1; i<=NUM_WORKERS; i++)); do
        print_info "Start worker $i..."
        ./worker "$TOP_WORDS" > "worker_$i.log" 2>&1 &
    done
}

start_aggregator() {
    pkill -f "./aggregator" || true
    sleep 1
    
    ./aggregator "$TOP_WORDS" > "aggregator.log" 2>&1 &
    local pid=$!
    echo $pid
}

start_producer() {
    pkill -f "./producer" 2>/dev/null || true
    sleep 1

    print_info "Start producer..."
    ./producer "$TEXT_FILE" "$SENTENCES_PER_SECTION" > "producer.log" 2>&1 &
}

cleanup() {
    pkill -f "./worker" 2>/dev/null || true
    pkill -f "./aggregator" 2>/dev/null || true
    pkill -f "./producer" 2>/dev/null || true
    
    sleep 2
    
    pkill -9 -f "./worker" 2>/dev/null || true
    pkill -9 -f "./aggregator" 2>/dev/null || true
    pkill -9 -f "./producer" 2>/dev/null || true
}

main() {
    trap cleanup EXIT INT TERM
    
    check_dependencies
    compile_project
    start=`date +%s%3N`
    start_workers
    process_pid=$(start_aggregator)
    start_producer

    while kill -0 $process_pid 2>/dev/null; do
        sleep 0.1
    done
    end=`date +%s%3N`
    runtime=$((end-start))
    echo $runtime
}

main "$@"