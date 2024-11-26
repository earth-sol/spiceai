#!/usr/bin/env bash

# Spice Test Runner
# Handles initialization, runtime management, and testing for Spice application

set -euo pipefail
IFS=$'\n\t'

# Default configuration
SPICE_PORT=8090
SPICE_READY_ENDPOINT="http://localhost:${SPICE_PORT}/v1/ready"
MAX_RETRY_TIME=60  # Maximum time to wait for Spice in seconds
LOG_FILE="spice.log"
CONFIG_FILE="spicepod.yaml"
DEFAULT_CONFIG_SOURCE="./test/models/spicepod.yml"
declare -a TEST_FILES=(./test/models/*.exp)

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# Show usage information
show_usage() {
    cat << EOF
Usage: $(basename "$0") [options] [test_files...]

Options:
    -h, --help          Show this help message

Arguments overide which `.exp` test files to run (default: All '.exp' in 'test/models/')

Examples:
    $(basename "$0")
    $(basename "$0") ./test/no_models/test1.exp ./test/models2/custom_test.exp
EOF
}

# Parse command line arguments
parse_args() {
    local -a test_files
    test_files=("${TEST_FILES[@]}")

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                show_usage
                exit 0
                ;;
            -*)
                log_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                # Clear default test files on first positional argument
                if [[ ${#test_files[@]} -eq ${#TEST_FILES[@]} ]]; then
                    test_files=()
                fi

                if [ -f "$1" ]; then
                    test_files+=("$1")
                else
                    log_error "Test file not found: $1"
                    show_usage
                    exit 1
                fi
                ;;
        esac
        shift
    done

    # Return array using declare
    declare -p test_files
}

# Error handler
cleanup() {
    log_info "Cleaning up..."
    killall spice 2>/dev/null || true
    if [[ -f "${LOG_FILE}" ]]; then
        log_info "Spice logs:"
        cat "${LOG_FILE}"
    fi
}
trap cleanup EXIT

# Check if required environment variables are set
check_environment() {
    local missing_vars=()
    [[ -z "${GITHUB_TOKEN:-}" ]] && missing_vars+=("GITHUB_TOKEN")
    [[ -z "${SPICE_OPENAI_API_KEY:-}" ]] && missing_vars+=("SPICE_OPENAI_API_KEY")

    if [[ ${#missing_vars[@]} -ne 0 ]]; then
        log_error "Missing required environment variables: ${missing_vars[*]}"
        exit 1
    fi
}

init_spice() {
    local config_source="$1"
    log_info "Initializing Spice configuration from: ${config_source}"
    cp "${config_source}" "./${CONFIG_FILE}"
}

start_spice() {
    log_info "Starting Spice runtime..."
    spice run &> "${LOG_FILE}" &
    local spice_pid=$!
    log_info "Spice started with PID: ${spice_pid}"
}

wait_for_spice() {
    log_info "Waiting for Spice to be ready..."
    local retry_count=0
    while [[ "$(curl -s ${SPICE_READY_ENDPOINT})" != "ready" ]]; do
        if ((retry_count >= MAX_RETRY_TIME)); then
            log_error "Timeout waiting for Spice to be ready"
            exit 1
        fi
        sleep 1
        ((retry_count++))
        if ((retry_count % 10 == 0)); then
            log_warn "Still waiting for Spice... (${retry_count}s)"
        fi
    done
    log_info "Spice is ready!"
}

install_expect() {
    local os_type=$(uname -s | tr '[:upper:]' '[:lower:]')
    log_info "Installing expect for ${os_type}..."

    case "${os_type}" in
        "linux")
            sudo apt-get update
            sudo apt-get install -y expect
            ;;
        "darwin")
            if ! brew list expect &>/dev/null; then
                log_info "Installing expect..."
                brew install expect
            fi
            ;;
        *)
            log_error "Unsupported OS: ${os_type}"
            exit 1
            ;;
    esac
}

# Run tests
run_tests() {
    local passed=0
    local failed=0

    for test_cmd in "$@"; do
        log_info "Running test: ${test_cmd}"
        if eval "${test_cmd}"; then
            log_info "Test passed: ${test_cmd}"
            ((passed++))
        else
            log_error "Test failed: ${test_cmd}"
            ((failed++))
        fi
    done

    if ((failed > 0)); then
        log_error "Tests completed with failures: ${failed} failed, ${passed} passed"
        return 1
    fi

    log_info "All ${passed} tests passed successfully"
    return 0
}

main() {
    rm -f "${LOG_FILE}"
    if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
        show_usage
        exit 0
    fi

    local config_source
    TEST_FILES=$(parse_args "$@")

    log_info "Starting Spice test runner..."
    check_environment
    init_spice $DEFAULT_CONFIG_SOURCE
    start_spice
    wait_for_spice
    install_expect
    run_tests $TEST_FILES

    log_info "All tests completed successfully!"
}

main "$@"
