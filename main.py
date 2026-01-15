"""
Main System Orchestrator
Integrates edge, core, and cloud nodes with all distributed protocols
"""

import sys
import os
import time
import logging
import json
from typing import Dict, List, Any

# Add shared modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'edge'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'cloud'))

from shared.protocols.consensus import ConsensusCluster
from shared.protocols.transactions import TransactionCoordinator
from shared.protocols.fault_tolerance import ReplicationManager, FailureDetector
from shared.utils.common import PerformanceMetrics, Logger

from edge.services.edge_node import EdgeNode
from core.services.core_node import CoreNode
from cloud.services.cloud_node import CloudNode

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DistributedTelecomSystem:
    """Main orchestrator for the distributed telecommunication system"""
    
    def __init__(self):
        # System components
        self.edge_nodes: List[EdgeNode] = []
        self.core_nodes: List[CoreNode] = []
        self.cloud_nodes: List[CloudNode] = []
        
        # Distributed protocols
        self.consensus_cluster = None
        self.transaction_coordinator = None
        self.replication_manager = None
        self.failure_detector = None
        
        # Performance monitoring
        self.metrics = PerformanceMetrics()
        
        # System state
        self.running = False
        
        logger.info("Distributed Telecom System initialized")
    
    def setup_infrastructure(self, config: Dict[str, Any]):
        """Setup edge-core-cloud infrastructure"""
        logger.info("Setting up infrastructure...")
        
        # Create edge nodes
        num_edge = config.get("num_edge_nodes", 3)
        core_node_ids = [f"core-{i}" for i in range(config.get("num_core_nodes", 2))]
        
        for i in range(num_edge):
            edge_node = EdgeNode(
                node_id=f"edge-{i}",
                region=f"region-{i % 3}",
                core_nodes=core_node_ids
            )
            self.edge_nodes.append(edge_node)
        
        logger.info(f"Created {num_edge} edge nodes")
        
        # Create core nodes
        num_core = config.get("num_core_nodes", 2)
        edge_node_ids = [f"edge-{i}" for i in range(num_edge)]
        cloud_node_ids = [f"cloud-{i}" for i in range(config.get("num_cloud_nodes", 2))]
        
        for i in range(num_core):
            core_node = CoreNode(
                node_id=f"core-{i}",
                region=f"region-{i % 2}",
                edge_nodes=edge_node_ids,
                cloud_nodes=cloud_node_ids
            )
            self.core_nodes.append(core_node)
        
        logger.info(f"Created {num_core} core nodes")
        
        # Create cloud nodes
        num_cloud = config.get("num_cloud_nodes", 2)
        
        for i in range(num_cloud):
            cloud_node = CloudNode(
                node_id=f"cloud-{i}",
                data_center=f"dc-{i % 2}"
            )
            self.cloud_nodes.append(cloud_node)
        
        logger.info(f"Created {num_cloud} cloud nodes")
        
        # Initialize distributed protocols
        self._setup_protocols(config)
    
    def _setup_protocols(self, config: Dict[str, Any]):
        """Setup distributed protocols"""
        logger.info("Setting up distributed protocols...")
        
        # Consensus
        num_consensus_nodes = config.get("num_consensus_nodes", 5)
        self.consensus_cluster = ConsensusCluster(num_consensus_nodes)
        
        # Transactions
        self.transaction_coordinator = TransactionCoordinator("coordinator-main")
        
        # Replication
        replication_factor = config.get("replication_factor", 3)
        self.replication_manager = ReplicationManager(replication_factor)
        
        # Failure detection
        self.failure_detector = FailureDetector(timeout=5.0)
        
        logger.info("Distributed protocols initialized")
    
    def start_system(self):
        """Start all system components"""
        logger.info("Starting distributed system...")
        
        # Start consensus cluster
        if self.consensus_cluster:
            self.consensus_cluster.start_cluster()
        
        # Start edge nodes
        for edge_node in self.edge_nodes:
            edge_node.start(num_workers=4)
        
        # Start core nodes
        for core_node in self.core_nodes:
            core_node.start(num_workers=8)
        
        # Start cloud nodes
        for cloud_node in self.cloud_nodes:
            cloud_node.start(num_workers=16)
        
        self.running = True
        logger.info("Distributed system started successfully")
    
    def stop_system(self):
        """Stop all system components"""
        logger.info("Stopping distributed system...")
        
        self.running = False
        
        # Stop all nodes
        for edge_node in self.edge_nodes:
            edge_node.stop()
        
        for core_node in self.core_nodes:
            core_node.stop()
        
        for cloud_node in self.cloud_nodes:
            cloud_node.stop()
        
        # Stop consensus cluster
        if self.consensus_cluster:
            self.consensus_cluster.stop_cluster()
        
        logger.info("Distributed system stopped")
    
    def run_transactions_test(self, num_transactions: int = 100):
        """Test distributed transactions"""
        logger.info(f"Running {num_transactions} transaction tests...")
        
        participants = [f"participant-{i}" for i in range(5)]
        
        for i in range(num_transactions):
            txn_id = self.transaction_coordinator.begin_transaction(
                participants=participants,
                operations={"operation": f"txn-{i}"}
            )
            
            # Alternate between 2PC and 3PC
            if i % 2 == 0:
                self.transaction_coordinator.execute_2pc(txn_id)
            else:
                self.transaction_coordinator.execute_3pc(txn_id)
        
        stats = self.transaction_coordinator.get_statistics()
        logger.info(f"Transaction test completed: {stats}")
        return stats
    
    def run_replication_test(self):
        """Test data replication"""
        logger.info("Running replication tests...")
        
        all_nodes = [n.node_id for n in (self.edge_nodes + self.core_nodes + self.cloud_nodes)]
        
        # Create replicas for test data
        for i in range(10):
            data_key = f"data-{i}"
            self.replication_manager.create_replicas(data_key, all_nodes)
            
            # Write data with different consistency levels
            consistency = ["strong", "quorum", "eventual"][i % 3]
            self.replication_manager.write_data(data_key, f"value-{i}", consistency)
        
        stats = self.replication_manager.get_replication_stats()
        logger.info(f"Replication test completed: {stats}")
        return stats
    
    def simulate_failures(self):
        """Simulate node failures"""
        logger.info("Simulating node failures...")
        
        # Simulate edge node failure
        if self.edge_nodes:
            failed_node = self.edge_nodes[0].node_id
            logger.info(f"Simulating failure of {failed_node}")
            self.replication_manager.handle_failure(failed_node)
            
            # Trigger failover
            self.replication_manager.trigger_failover("data-0")
        
        stats = self.replication_manager.get_replication_stats()
        logger.info(f"After failure handling: {stats}")
        return stats
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        stats = {
            "timestamp": time.time(),
            "edge_nodes": [],
            "core_nodes": [],
            "cloud_nodes": [],
            "consensus": {},
            "transactions": {},
            "replication": {}
        }
        
        # Collect edge node stats
        for edge_node in self.edge_nodes:
            stats["edge_nodes"].append(edge_node.get_statistics())
        
        # Collect core node stats
        for core_node in self.core_nodes:
            stats["core_nodes"].append(core_node.get_statistics())
        
        # Collect cloud node stats
        for cloud_node in self.cloud_nodes:
            stats["cloud_nodes"].append(cloud_node.get_statistics())
        
        # Collect protocol stats
        if self.consensus_cluster:
            stats["consensus"] = {
                "cluster_state": self.consensus_cluster.get_cluster_state()
            }
        
        if self.transaction_coordinator:
            stats["transactions"] = self.transaction_coordinator.get_statistics()
        
        if self.replication_manager:
            stats["replication"] = self.replication_manager.get_replication_stats()
        
        return stats

def main():
    """Main entry point"""
    # Configuration
    config = {
        "num_edge_nodes": 3,
        "num_core_nodes": 2,
        "num_cloud_nodes": 2,
        "num_consensus_nodes": 5,
        "replication_factor": 3
    }
    
    # Create system
    system = DistributedTelecomSystem()
    
    try:
        # Setup and start
        system.setup_infrastructure(config)
        system.start_system()
        
        logger.info("System running. Waiting for initialization...")
        time.sleep(5)
        
        # Run tests
        logger.info("\n" + "="*50)
        logger.info("RUNNING DISTRIBUTED TRANSACTIONS TEST")
        logger.info("="*50)
        txn_stats = system.run_transactions_test(100)
        
        logger.info("\n" + "="*50)
        logger.info("RUNNING REPLICATION TEST")
        logger.info("="*50)
        rep_stats = system.run_replication_test()
        
        logger.info("\n" + "="*50)
        logger.info("SIMULATING FAILURES")
        logger.info("="*50)
        failure_stats = system.simulate_failures()
        
        # Collect final statistics
        logger.info("\n" + "="*50)
        logger.info("COLLECTING FINAL STATISTICS")
        logger.info("="*50)
        final_stats = system.get_system_statistics()
        
        # Save results
        with open('results/system_statistics.json', 'w') as f:
            json.dump(final_stats, f, indent=2)
        
        logger.info("Results saved to results/system_statistics.json")
        
        # Keep running for a bit
        logger.info("\nSystem running... Press Ctrl+C to stop")
        time.sleep(30)
        
    except KeyboardInterrupt:
        logger.info("\nReceived interrupt signal")
    
    finally:
        # Cleanup
        logger.info("\nShutting down system...")
        system.stop_system()
        logger.info("Shutdown complete")

if __name__ == "__main__":
    main()