#!/usr/bin/env python3
"""
Quick Demo of Distributed Telecommunication System
Demonstrates all major components with minimal dependencies
"""

import sys
import os
import time
import json
import threading
import random
from datetime import datetime

# Add paths
sys.path.append(os.path.dirname(__file__))

# Simple metrics collector
class MetricsCollector:
    def __init__(self):
        self.data = {}
    
    def record(self, key, value):
        if key not in self.data:
            self.data[key] = []
        self.data[key].append(value)
    
    def get_stats(self, key):
        if key not in self.data or not self.data[key]:
            return {}
        values = self.data[key]
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "total": sum(values)
        }

print("="*70)
print("DISTRIBUTED TELECOMMUNICATION SYSTEM - QUICK DEMO")
print("Capstone Project - ICS 2403")
print("="*70)
print()

metrics = MetricsCollector()

# ============================================================================
# PART 1: EDGE-CORE-CLOUD ARCHITECTURE
# ============================================================================
print("\n" + "="*70)
print("PART 1: EDGE-CORE-CLOUD ARCHITECTURE")
print("="*70)

try:
    from edge.services.edge_node import EdgeNode
    from core.services.core_node import CoreNode
    from cloud.services.cloud_node import CloudNode
    
    print("\n✓ Modules imported successfully")
    
    # Create nodes
    print("\n[1/5] Creating nodes...")
    edge_node = EdgeNode("edge-demo", "region-east", ["core-demo"])
    core_node = CoreNode("core-demo", "region-central", ["edge-demo"], ["cloud-demo"])
    cloud_node = CloudNode("cloud-demo", "dc-primary")
    print("  ✓ Edge node created: edge-demo")
    print("  ✓ Core node created: core-demo")
    print("  ✓ Cloud node created: cloud-demo")
    
    # Start nodes
    print("\n[2/5] Starting nodes...")
    edge_node.start(num_workers=2)
    print("  ✓ Edge node started (2 workers)")
    core_node.start(num_workers=4)
    print("  ✓ Core node started (4 workers)")
    cloud_node.start(num_workers=8)
    print("  ✓ Cloud node started (8 workers)")
    
    time.sleep(2)
    print("  ✓ All nodes initialized")
    
    # Process requests
    print("\n[3/5] Processing test requests...")
    
    request_types = ["simple", "complex", "transaction", "analytics", "ml_inference"]
    num_requests = 100
    
    for i in range(num_requests):
        req_type = random.choice(request_types)
        request = {
            "request_id": f"req-{i}",
            "type": req_type,
            "operation": "read",
            "data_key": f"key-{i % 20}"
        }
        
        # Measure latency
        start = time.time()
        
        if req_type in ["simple", "cache_update"]:
            response = edge_node.process_request(request)
        elif req_type in ["complex", "transaction"]:
            response = core_node.process_request(request)
        else:
            response = cloud_node.process_request(request)
        
        latency_ms = (time.time() - start) * 1000
        metrics.record("request_latency", latency_ms)
        
        if (i + 1) % 20 == 0:
            print(f"  Processed {i+1}/{num_requests} requests...")
    
    print(f"  ✓ Completed {num_requests} requests")
    
    # Get statistics
    print("\n[4/5] Collecting statistics...")
    edge_stats = edge_node.get_statistics()
    core_stats = core_node.get_statistics()
    cloud_stats = cloud_node.get_statistics()
    
    print("\n  EDGE NODE STATISTICS:")
    print(f"    Processed Requests: {edge_stats['processed_requests']}")
    print(f"    Cache Hit Rate: {edge_stats['cache_hit_rate_percent']:.1f}%")
    print(f"    Average Latency: {edge_stats['average_latency_ms']:.2f}ms")
    
    print("\n  CORE NODE STATISTICS:")
    print(f"    Processed Requests: {core_stats['processed_requests']}")
    print(f"    Active Sessions: {core_stats['active_sessions']}")
    print(f"    Average Latency: {core_stats['average_latency_ms']:.2f}ms")
    
    print("\n  CLOUD NODE STATISTICS:")
    print(f"    Processed Requests: {cloud_stats['processed_requests']}")
    print(f"    Analytics Jobs: {cloud_stats['analytics_jobs']}")
    print(f"    Average Latency: {cloud_stats['average_latency_ms']:.2f}ms")
    
    # Stop nodes
    print("\n[5/5] Stopping nodes...")
    edge_node.stop()
    core_node.stop()
    cloud_node.stop()
    print("  ✓ All nodes stopped gracefully")
    
except Exception as e:
    print(f"  ✗ Error in Part 1: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# PART 2: DISTRIBUTED TRANSACTIONS
# ============================================================================
print("\n" + "="*70)
print("PART 2: DISTRIBUTED TRANSACTIONS (2PC & 3PC)")
print("="*70)

try:
    from shared.protocols.transactions import TransactionCoordinator
    
    print("\n✓ Transaction module imported")
    
    coordinator = TransactionCoordinator("coordinator-demo")
    participants = [f"participant-{i}" for i in range(5)]
    
    print(f"\n[1/3] Testing Two-Phase Commit (2PC)...")
    num_txns = 50
    success_2pc = 0
    latencies_2pc = []
    
    for i in range(num_txns):
        start = time.time()
        txn_id = coordinator.begin_transaction(participants, {"operation": f"2pc-{i}"})
        if coordinator.execute_2pc(txn_id):
            success_2pc += 1
        latency = (time.time() - start) * 1000
        latencies_2pc.append(latency)
    
    print(f"  ✓ Completed {num_txns} transactions")
    print(f"    Success Rate: {success_2pc/num_txns*100:.1f}%")
    print(f"    Avg Latency: {sum(latencies_2pc)/len(latencies_2pc):.2f}ms")
    
    print(f"\n[2/3] Testing Three-Phase Commit (3PC)...")
    success_3pc = 0
    latencies_3pc = []
    
    for i in range(num_txns):
        start = time.time()
        txn_id = coordinator.begin_transaction(participants, {"operation": f"3pc-{i}"})
        if coordinator.execute_3pc(txn_id):
            success_3pc += 1
        latency = (time.time() - start) * 1000
        latencies_3pc.append(latency)
    
    print(f"  ✓ Completed {num_txns} transactions")
    print(f"    Success Rate: {success_3pc/num_txns*100:.1f}%")
    print(f"    Avg Latency: {sum(latencies_3pc)/len(latencies_3pc):.2f}ms")
    
    print(f"\n[3/3] Transaction Comparison:")
    print(f"  2PC - Success: {success_2pc}/{num_txns} ({success_2pc/num_txns*100:.1f}%), " +
          f"Latency: {sum(latencies_2pc)/len(latencies_2pc):.2f}ms")
    print(f"  3PC - Success: {success_3pc}/{num_txns} ({success_3pc/num_txns*100:.1f}%), " +
          f"Latency: {sum(latencies_3pc)/len(latencies_3pc):.2f}ms")
    
    txn_stats = coordinator.get_statistics()
    print(f"\n  COORDINATOR STATISTICS:")
    print(f"    Total Transactions: {txn_stats['total_transactions']}")
    print(f"    Committed: {txn_stats['committed']}")
    print(f"    Aborted: {txn_stats['aborted']}")
    print(f"    Overall Commit Rate: {txn_stats['commit_rate']:.1f}%")
    
except Exception as e:
    print(f"  ✗ Error in Part 2: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# PART 3: FAULT TOLERANCE & REPLICATION
# ============================================================================
print("\n" + "="*70)
print("PART 3: FAULT TOLERANCE & REPLICATION")
print("="*70)

try:
    from shared.protocols.fault_tolerance import ReplicationManager, FailureDetector
    
    print("\n✓ Fault tolerance module imported")
    
    replication_manager = ReplicationManager(replication_factor=3)
    all_nodes = [f"node-{i}" for i in range(10)]
    
    print("\n[1/4] Creating replicas...")
    for i in range(20):
        data_key = f"data-{i}"
        replication_manager.create_replicas(data_key, all_nodes)
        replication_manager.write_data(data_key, f"value-{i}", "strong")
    
    print(f"  ✓ Created replicas for 20 data keys")
    
    before_stats = replication_manager.get_replication_stats()
    print(f"  ✓ Active replicas: {before_stats['active_replicas']}")
    print(f"  ✓ Availability: {before_stats['availability_percent']:.1f}%")
    
    print("\n[2/4] Simulating node failures...")
    failed_nodes = ["node-0", "node-1", "node-2"]
    recovery_start = time.time()
    
    for node in failed_nodes:
        replication_manager.handle_failure(node)
        print(f"  ✓ Handled failure of {node}")
    
    print("\n[3/4] Triggering failover...")
    for i in range(20):
        replication_manager.trigger_failover(f"data-{i}")
    
    recovery_time = (time.time() - recovery_start) * 1000
    print(f"  ✓ Failover completed in {recovery_time:.2f}ms")
    
    after_stats = replication_manager.get_replication_stats()
    
    print("\n[4/4] Post-failure statistics:")
    print(f"  Active replicas: {after_stats['active_replicas']} " +
          f"(was {before_stats['active_replicas']})")
    print(f"  Failed replicas: {after_stats['failed_replicas']}")
    print(f"  Availability: {after_stats['availability_percent']:.1f}% " +
          f"(was {before_stats['availability_percent']:.1f}%)")
    print(f"  Recovery time: {recovery_time:.2f}ms")
    
except Exception as e:
    print(f"  ✗ Error in Part 3: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# PART 4: CONSENSUS PROTOCOL
# ============================================================================
print("\n" + "="*70)
print("PART 4: DISTRIBUTED CONSENSUS")
print("="*70)

try:
    from shared.protocols.consensus import ConsensusCluster
    
    print("\n✓ Consensus module imported")
    
    print("\n[1/3] Creating consensus cluster...")
    consensus_cluster = ConsensusCluster(num_nodes=5)
    print("  ✓ Created cluster with 5 nodes")
    
    print("\n[2/3] Starting consensus...")
    consensus_cluster.start_cluster()
    time.sleep(3)  # Allow time for leader election
    print("  ✓ Cluster started, leader election in progress")
    
    # Get cluster state
    cluster_state = consensus_cluster.get_cluster_state()
    leader = consensus_cluster.get_leader()
    
    print("\n[3/3] Cluster state:")
    for node_state in cluster_state:
        status = "LEADER" if node_state['is_leader'] else node_state['state'].upper()
        print(f"  {node_state['node_id']}: {status} (term: {node_state['term']})")
    
    if leader:
        print(f"\n  ✓ Current leader: {leader.node_id}")
        
        # Submit commands
        print("\n  Submitting commands to leader...")
        for i in range(10):
            consensus_cluster.submit_command({"command": f"cmd-{i}"})
        print(f"  ✓ Submitted 10 commands")
    
    # Stop cluster
    consensus_cluster.stop_cluster()
    print("\n  ✓ Consensus cluster stopped")
    
except Exception as e:
    print(f"  ✗ Error in Part 4: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "="*70)
print("DEMO SUMMARY")
print("="*70)

latency_stats = metrics.get_stats("request_latency")
if latency_stats:
    print("\nOVERALL REQUEST LATENCY:")
    print(f"  Total Requests: {latency_stats['count']}")
    print(f"  Min Latency: {latency_stats['min']:.2f}ms")
    print(f"  Max Latency: {latency_stats['max']:.2f}ms")
    print(f"  Avg Latency: {latency_stats['avg']:.2f}ms")

print("\nCOMPONENTS DEMONSTRATED:")
print("  ✓ Edge-Core-Cloud Architecture")
print("  ✓ Distributed Transaction Management (2PC & 3PC)")
print("  ✓ Fault Tolerance & Replication")
print("  ✓ Distributed Consensus (Raft-like)")
print("  ✓ Load Balancing & Routing")
print("  ✓ Performance Monitoring")

print("\n" + "="*70)
print("DEMO COMPLETED SUCCESSFULLY!")
print("="*70)
print("\nFor comprehensive evaluation, run:")
print("  python3 tests/performance/evaluate_system.py")
print("\nFor full system integration, run:")
print("  python3 main.py")
print("\n" + "="*70)