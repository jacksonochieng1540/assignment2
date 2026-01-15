"""
Distributed Consensus Protocol Implementation
Implements Raft-like consensus for distributed coordination
"""

import time
import random
import threading
import logging
from enum import Enum
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)

class ConsensusNode:
    """Implements distributed consensus protocol"""
    
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers
        self.state = NodeState.FOLLOWER
        
        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Timing
        self.election_timeout = self._reset_election_timeout()
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 0.5
        
        # Thread control
        self.running = False
        self.lock = threading.Lock()
        self.election_thread = None
        self.heartbeat_thread = None
        
        logger.info(f"Consensus node {node_id} initialized with peers: {peers}")
    
    def _reset_election_timeout(self) -> float:
        """Reset election timeout with random jitter"""
        return random.uniform(1.5, 3.0)
    
    def start(self):
        """Start consensus protocol"""
        self.running = True
        self.election_thread = threading.Thread(target=self._election_timer)
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_timer)
        self.election_thread.start()
        self.heartbeat_thread.start()
        logger.info(f"Node {self.node_id} started")
    
    def stop(self):
        """Stop consensus protocol"""
        self.running = False
        if self.election_thread:
            self.election_thread.join()
        if self.heartbeat_thread:
            self.heartbeat_thread.join()
        logger.info(f"Node {self.node_id} stopped")
    
    def _election_timer(self):
        """Monitor election timeout"""
        while self.running:
            time.sleep(0.1)
            
            with self.lock:
                if self.state == NodeState.LEADER:
                    continue
                
                elapsed = time.time() - self.last_heartbeat
                if elapsed > self.election_timeout:
                    self._start_election()
    
    def _heartbeat_timer(self):
        """Send periodic heartbeats if leader"""
        while self.running:
            time.sleep(self.heartbeat_interval)
            
            with self.lock:
                if self.state == NodeState.LEADER:
                    self._send_heartbeat()
    
    def _start_election(self):
        """Start leader election"""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self._reset_election_timeout()
        
        votes_received = 1  # Vote for self
        votes_needed = len(self.peers) // 2 + 1
        
        logger.info(f"Node {self.node_id} starting election for term {self.current_term}")
        
        # Request votes from peers (simulated)
        for peer in self.peers:
            if random.random() > 0.3:  # Simulate 70% response rate
                votes_received += 1
        
        if votes_received >= votes_needed:
            self._become_leader()
        else:
            self.state = NodeState.FOLLOWER
            logger.info(f"Node {self.node_id} lost election")
    
    def _become_leader(self):
        """Transition to leader state"""
        self.state = NodeState.LEADER
        
        # Initialize leader state
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = 0
        
        logger.info(f"Node {self.node_id} became leader for term {self.current_term}")
    
    def _send_heartbeat(self):
        """Send heartbeat to all followers"""
        logger.debug(f"Leader {self.node_id} sending heartbeat")
        # In real implementation, send AppendEntries RPC
        pass
    
    def append_entry(self, command: Dict[str, Any]) -> bool:
        """Append entry to log (only leader can do this)"""
        with self.lock:
            if self.state != NodeState.LEADER:
                return False
            
            entry = LogEntry(
                term=self.current_term,
                index=len(self.log),
                command=command
            )
            self.log.append(entry)
            
            logger.info(f"Leader {self.node_id} appended entry: {entry}")
            return True
    
    def receive_heartbeat(self, term: int, leader_id: str):
        """Process heartbeat from leader"""
        with self.lock:
            if term >= self.current_term:
                self.current_term = term
                self.state = NodeState.FOLLOWER
                self.last_heartbeat = time.time()
                logger.debug(f"Node {self.node_id} received heartbeat from {leader_id}")
    
    def get_state(self) -> Dict[str, Any]:
        """Get current node state"""
        with self.lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "term": self.current_term,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "is_leader": self.state == NodeState.LEADER
            }

class ConsensusCluster:
    """Manages cluster of consensus nodes"""
    
    def __init__(self, num_nodes: int = 5):
        self.nodes: List[ConsensusNode] = []
        node_ids = [f"node-{i}" for i in range(num_nodes)]
        
        # Create nodes
        for i, node_id in enumerate(node_ids):
            peers = [nid for nid in node_ids if nid != node_id]
            node = ConsensusNode(node_id, peers)
            self.nodes.append(node)
        
        logger.info(f"Consensus cluster created with {num_nodes} nodes")
    
    def start_cluster(self):
        """Start all nodes in cluster"""
        for node in self.nodes:
            node.start()
        logger.info("Cluster started")
    
    def stop_cluster(self):
        """Stop all nodes in cluster"""
        for node in self.nodes:
            node.stop()
        logger.info("Cluster stopped")
    
    def get_leader(self) -> Optional[ConsensusNode]:
        """Get current leader node"""
        for node in self.nodes:
            if node.state == NodeState.LEADER:
                return node
        return None
    
    def get_cluster_state(self) -> List[Dict[str, Any]]:
        """Get state of all nodes"""
        return [node.get_state() for node in self.nodes]
    
    def submit_command(self, command: Dict[str, Any]) -> bool:
        """Submit command to cluster"""
        leader = self.get_leader()
        if leader:
            return leader.append_entry(command)
        return False