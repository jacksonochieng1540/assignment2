"""
Fault Tolerance and Replication Manager
Handles crash, omission, and Byzantine failures with replication strategies
"""

import time
import random
import threading
import logging
from enum import Enum
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass
from collections import defaultdict

logger = logging.getLogger(__name__)

class FailureType(Enum):
    NONE = "none"
    CRASH = "crash"
    OMISSION = "omission"
    BYZANTINE = "byzantine"
    NETWORK = "network"

class ReplicaState(Enum):
    ACTIVE = "active"
    STANDBY = "standby"
    FAILED = "failed"
    RECOVERING = "recovering"

@dataclass
class Replica:
    replica_id: str
    node_type: str
    state: ReplicaState
    last_heartbeat: float
    failure_count: int = 0
    data_version: int = 0

class FailureDetector:
    """Detects and monitors node failures"""
    
    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout
        self.suspected_nodes: Set[str] = set()
        self.heartbeats: Dict[str, float] = {}
        self.lock = threading.Lock()
        
    def record_heartbeat(self, node_id: str):
        """Record heartbeat from node"""
        with self.lock:
            self.heartbeats[node_id] = time.time()
            if node_id in self.suspected_nodes:
                self.suspected_nodes.remove(node_id)
                logger.info(f"Node {node_id} recovered")
    
    def check_failures(self) -> Set[str]:
        """Check for failed nodes"""
        with self.lock:
            current_time = time.time()
            failed_nodes = set()
            
            for node_id, last_heartbeat in self.heartbeats.items():
                if current_time - last_heartbeat > self.timeout:
                    if node_id not in self.suspected_nodes:
                        self.suspected_nodes.add(node_id)
                        failed_nodes.add(node_id)
                        logger.warning(f"Node {node_id} suspected of failure")
            
            return failed_nodes
    
    def is_suspected(self, node_id: str) -> bool:
        """Check if node is suspected"""
        with self.lock:
            return node_id in self.suspected_nodes

class ReplicationManager:
    """Manages data replication across nodes"""
    
    def __init__(self, replication_factor: int = 3):
        self.replication_factor = replication_factor
        self.replicas: Dict[str, List[Replica]] = defaultdict(list)
        self.data_store: Dict[str, Any] = {}
        self.version_vector: Dict[str, int] = {}
        self.lock = threading.Lock()
        self.failure_detector = FailureDetector()
        
        logger.info(f"Replication manager initialized with factor {replication_factor}")
    
    def create_replicas(self, data_key: str, nodes: List[str]) -> bool:
        """Create replicas for data across nodes"""
        with self.lock:
            if len(nodes) < self.replication_factor:
                logger.error(f"Not enough nodes for replication factor {self.replication_factor}")
                return False
            
            # Select nodes for replication
            replica_nodes = nodes[:self.replication_factor]
            
            for i, node_id in enumerate(replica_nodes):
                replica = Replica(
                    replica_id=f"{data_key}-replica-{i}",
                    node_type="primary" if i == 0 else "secondary",
                    state=ReplicaState.ACTIVE,
                    last_heartbeat=time.time(),
                    data_version=0
                )
                self.replicas[data_key].append(replica)
            
            self.version_vector[data_key] = 0
            logger.info(f"Created {len(replica_nodes)} replicas for {data_key}")
            return True
    
    def write_data(self, data_key: str, value: Any, 
                   consistency: str = "strong") -> bool:
        """Write data with specified consistency"""
        with self.lock:
            if data_key not in self.replicas:
                logger.error(f"No replicas found for {data_key}")
                return False
            
            # Increment version
            self.version_vector[data_key] += 1
            version = self.version_vector[data_key]
            
            # Store data
            self.data_store[data_key] = {
                "value": value,
                "version": version,
                "timestamp": time.time()
            }
            
            # Replicate to active replicas
            active_replicas = [r for r in self.replicas[data_key] 
                             if r.state == ReplicaState.ACTIVE]
            
            if consistency == "strong":
                # Wait for all replicas
                required_acks = len(active_replicas)
            elif consistency == "quorum":
                # Wait for majority
                required_acks = (len(active_replicas) // 2) + 1
            else:  # eventual
                required_acks = 1
            
            successful_writes = 0
            for replica in active_replicas:
                if self._replicate_to_node(replica, data_key, value, version):
                    successful_writes += 1
                    replica.data_version = version
            
            success = successful_writes >= required_acks
            logger.info(f"Write to {data_key}: {successful_writes}/{required_acks} successful")
            return success
    
    def read_data(self, data_key: str, consistency: str = "strong") -> Optional[Any]:
        """Read data with specified consistency"""
        with self.lock:
            if data_key not in self.replicas:
                logger.error(f"No replicas found for {data_key}")
                return None
            
            active_replicas = [r for r in self.replicas[data_key] 
                             if r.state == ReplicaState.ACTIVE]
            
            if not active_replicas:
                logger.error(f"No active replicas for {data_key}")
                return None
            
            if consistency == "strong":
                # Read from all replicas and check consistency
                values = []
                for replica in active_replicas:
                    val = self._read_from_node(replica, data_key)
                    if val:
                        values.append(val)
                
                if values and all(v["version"] == values[0]["version"] for v in values):
                    return values[0]["value"]
                else:
                    logger.warning(f"Inconsistent replicas detected for {data_key}")
                    return None
            else:
                # Read from any replica
                data = self.data_store.get(data_key)
                return data["value"] if data else None
    
    def _replicate_to_node(self, replica: Replica, data_key: str, 
                          value: Any, version: int) -> bool:
        """Replicate data to a specific node"""
        # Simulate network delay
        time.sleep(0.001)
        
        # Simulate failure (5% failure rate)
        if random.random() < 0.05:
            replica.failure_count += 1
            logger.debug(f"Replication to {replica.replica_id} failed")
            return False
        
        replica.last_heartbeat = time.time()
        return True
    
    def _read_from_node(self, replica: Replica, data_key: str) -> Optional[Dict]:
        """Read data from a specific node"""
        # Simulate network delay
        time.sleep(0.001)
        
        data = self.data_store.get(data_key)
        if data:
            replica.last_heartbeat = time.time()
        return data
    
    def handle_failure(self, failed_node: str) -> bool:
        """Handle node failure and trigger recovery"""
        with self.lock:
            logger.warning(f"Handling failure of node {failed_node}")
            
            # Mark replicas on failed node as failed
            for data_key, replica_list in self.replicas.items():
                for replica in replica_list:
                    if failed_node in replica.replica_id:
                        replica.state = ReplicaState.FAILED
                        logger.info(f"Marked {replica.replica_id} as failed")
            
            return True
    
    def trigger_failover(self, data_key: str) -> bool:
        """Trigger failover to standby replica"""
        with self.lock:
            if data_key not in self.replicas:
                return False
            
            failed_replicas = [r for r in self.replicas[data_key] 
                             if r.state == ReplicaState.FAILED]
            standby_replicas = [r for r in self.replicas[data_key] 
                              if r.state == ReplicaState.STANDBY]
            
            for _ in failed_replicas:
                if standby_replicas:
                    standby = standby_replicas.pop(0)
                    standby.state = ReplicaState.ACTIVE
                    logger.info(f"Promoted {standby.replica_id} to active")
            
            return True
    
    def get_replication_stats(self) -> Dict[str, Any]:
        """Get replication statistics"""
        with self.lock:
            total_replicas = sum(len(replicas) for replicas in self.replicas.values())
            active_replicas = sum(
                sum(1 for r in replicas if r.state == ReplicaState.ACTIVE)
                for replicas in self.replicas.values()
            )
            failed_replicas = sum(
                sum(1 for r in replicas if r.state == ReplicaState.FAILED)
                for replicas in self.replicas.values()
            )
            
            return {
                "total_data_keys": len(self.replicas),
                "total_replicas": total_replicas,
                "active_replicas": active_replicas,
                "failed_replicas": failed_replicas,
                "replication_factor": self.replication_factor,
                "availability_percent": (active_replicas / total_replicas * 100 
                                        if total_replicas > 0 else 0)
            }

class ByzantineDetector:
    """Detects Byzantine failures"""
    
    def __init__(self, threshold: int = 2):
        self.threshold = threshold
        self.suspicious_behavior: Dict[str, int] = defaultdict(int)
        self.lock = threading.Lock()
    
    def report_suspicious(self, node_id: str, behavior: str):
        """Report suspicious behavior"""
        with self.lock:
            self.suspicious_behavior[node_id] += 1
            logger.warning(f"Suspicious behavior from {node_id}: {behavior}")
            
            if self.suspicious_behavior[node_id] >= self.threshold:
                logger.error(f"Node {node_id} suspected of Byzantine failure")
    
    def is_byzantine(self, node_id: str) -> bool:
        """Check if node exhibits Byzantine behavior"""
        with self.lock:
            return self.suspicious_behavior.get(node_id, 0) >= self.threshold