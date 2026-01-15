#!/bin/bash

# Carrier-Grade Distributed Telecommunication System
# Main Execution Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  DISTRIBUTED TELECOM SYSTEM${NC}"
echo -e "${BLUE}  Capstone Project - ICS 2403${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Create necessary directories
echo -e "${YELLOW}Creating necessary directories...${NC}"
mkdir -p logs/{edge,core,cloud}
mkdir -p results/{graphs,tables,reports}
mkdir -p shared/utils shared/protocols
mkdir -p edge/services core/services cloud/services
echo -e "${GREEN}✓ Directories created${NC}"
echo ""

# Install dependencies
echo -e "${YELLOW}Installing Python dependencies...${NC}"
pip install -q --break-system-packages -r requirements.txt 2>/dev/null || pip install -q -r requirements.txt
echo -e "${GREEN}✓ Dependencies installed${NC}"
echo ""

# Create __init__.py files
echo -e "${YELLOW}Setting up Python modules...${NC}"
touch shared/__init__.py
touch shared/utils/__init__.py
touch shared/protocols/__init__.py
touch edge/__init__.py
touch edge/services/__init__.py
touch core/__init__.py
touch core/services/__init__.py
touch cloud/__init__.py
touch cloud/services/__init__.py
touch tests/__init__.py
touch tests/performance/__init__.py
echo -e "${GREEN}✓ Python modules configured${NC}"
echo ""

# Function to show menu
show_menu() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Select an option:${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo "1. Run Main System Demo"
    echo "2. Run Performance Evaluation"
    echo "3. Run All Tests (System + Performance)"
    echo "4. Quick Test (Reduced workload)"
    echo "5. View Previous Results"
    echo "6. Clean Results"
    echo "7. Exit"
    echo ""
    echo -n "Enter choice [1-7]: "
}

# Function to run main system
run_main_system() {
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}RUNNING MAIN SYSTEM${NC}"
    echo -e "${GREEN}========================================${NC}\n"
    
    python3 main.py 2>&1 | tee logs/main_system.log
    
    echo -e "\n${GREEN}✓ Main system execution completed${NC}"
    echo -e "${YELLOW}Logs saved to: logs/main_system.log${NC}"
}

# Function to run performance evaluation
run_performance_eval() {
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}RUNNING PERFORMANCE EVALUATION${NC}"
    echo -e "${GREEN}========================================${NC}\n"
    
    python3 tests/performance/evaluate_system.py 2>&1 | tee logs/performance_eval.log
    
    echo -e "\n${GREEN}✓ Performance evaluation completed${NC}"
    echo -e "${YELLOW}Logs saved to: logs/performance_eval.log${NC}"
    echo -e "${YELLOW}Results saved in: results/${NC}"
}

# Function to run all tests
run_all_tests() {
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}RUNNING COMPREHENSIVE TEST SUITE${NC}"
    echo -e "${GREEN}========================================${NC}\n"
    
    echo -e "${BLUE}Phase 1: Main System Test${NC}"
    run_main_system
    
    sleep 2
    
    echo -e "\n${BLUE}Phase 2: Performance Evaluation${NC}"
    run_performance_eval
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}ALL TESTS COMPLETED${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Function to run quick test
run_quick_test() {
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}RUNNING QUICK TEST${NC}"
    echo -e "${GREEN}========================================${NC}\n"
    
    python3 -c "
import sys
import os
sys.path.append(os.getcwd())

from edge.services.edge_node import EdgeNode
from core.services.core_node import CoreNode
from cloud.services.cloud_node import CloudNode
import time

print('Creating nodes...')
edge = EdgeNode('edge-quick', 'region-0', ['core-0'])
core = CoreNode('core-quick', 'region-0', ['edge-quick'], ['cloud-0'])
cloud = CloudNode('cloud-quick', 'dc-0')

print('Starting nodes...')
edge.start(num_workers=2)
core.start(num_workers=4)
cloud.start(num_workers=8)

time.sleep(2)

print('Processing test requests...')
for i in range(50):
    request = {'request_id': f'req-{i}', 'type': 'simple', 'operation': 'read'}
    edge.process_request(request)
    core.process_request(request)
    cloud.process_request(request)

print('\\nCollecting statistics...')
print('\\nEdge Node Stats:', edge.get_statistics())
print('\\nCore Node Stats:', core.get_statistics())
print('\\nCloud Node Stats:', cloud.get_statistics())

print('\\nStopping nodes...')
edge.stop()
core.stop()
cloud.stop()

print('\\n✓ Quick test completed!')
" 2>&1 | tee logs/quick_test.log
    
    echo -e "\n${GREEN}✓ Quick test completed${NC}"
    echo -e "${YELLOW}Logs saved to: logs/quick_test.log${NC}"
}

# Function to view results
view_results() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}AVAILABLE RESULTS${NC}"
    echo -e "${BLUE}========================================${NC}\n"
    
    if [ -f "results/system_statistics.json" ]; then
        echo -e "${GREEN}Main System Results:${NC}"
        echo "  - results/system_statistics.json"
        echo ""
    fi
    
    if [ -f "results/reports/performance_report.json" ]; then
        echo -e "${GREEN}Performance Evaluation Results:${NC}"
        echo "  - results/reports/performance_report.json"
        echo ""
    fi
    
    if [ -d "results/graphs" ] && [ "$(ls -A results/graphs 2>/dev/null)" ]; then
        echo -e "${GREEN}Generated Graphs:${NC}"
        ls -1 results/graphs/*.png 2>/dev/null | sed 's/^/  - /'
        echo ""
    fi
    
    if [ -d "results/tables" ] && [ "$(ls -A results/tables 2>/dev/null)" ]; then
        echo -e "${GREEN}Generated Tables:${NC}"
        ls -1 results/tables/*.csv 2>/dev/null | sed 's/^/  - /'
        echo ""
    fi
    
    if [ -d "logs" ] && [ "$(ls -A logs/*.log 2>/dev/null)" ]; then
        echo -e "${GREEN}Log Files:${NC}"
        ls -1 logs/*.log 2>/dev/null | sed 's/^/  - /'
        echo ""
    fi
    
    echo -e "${YELLOW}Press Enter to continue...${NC}"
    read
}

# Function to clean results
clean_results() {
    echo -e "\n${YELLOW}Cleaning results...${NC}"
    rm -rf results/graphs/* results/tables/* results/reports/* logs/*.log
    echo -e "${GREEN}✓ Results cleaned${NC}"
    sleep 1
}

# Main menu loop
while true; do
    show_menu
    read choice
    
    case $choice in
        1)
            run_main_system
            echo -e "\n${YELLOW}Press Enter to continue...${NC}"
            read
            ;;
        2)
            run_performance_eval
            echo -e "\n${YELLOW}Press Enter to continue...${NC}"
            read
            ;;
        3)
            run_all_tests
            echo -e "\n${YELLOW}Press Enter to continue...${NC}"
            read
            ;;
        4)
            run_quick_test
            echo -e "\n${YELLOW}Press Enter to continue...${NC}"
            read
            ;;
        5)
            view_results
            ;;
        6)
            clean_results
            ;;
        7)
            echo -e "\n${GREEN}Thank you for using the Distributed Telecom System!${NC}"
            echo -e "${BLUE}========================================${NC}\n"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option. Please try again.${NC}"
            sleep 1
            ;;
    esac
done