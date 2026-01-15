"""
Core Node Service
Handles business logic, routing, and coordination between edge and cloud
"""

import time
import threading
import logging
import random
from typing import Dict, List, Any, Optional
from collections import defaultdict
import psutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CoreNode:
    """Core network node for business logic and routing"""
    
    def __init__(self, node_id: str, region: str, 
                 edge_nodes: List[str], cloud_nodes: List[str]):
        self.node_id = node_id
        self.region = region
        self.edge_nodes = edge_nodes
        self.cloud_nodes = cloud_nodes
        
        # Session management
        self.active_sessions: Dict[str, Dict[str, Any]] = {}
        self.session_count = 0
        
        # Request routing
        self.route_table: Dict[str, str] = {}
        self.processed_requests = 0
        self.routed_to_cloud = 0
        self.routed_to_edge = 0
        
        # Load balancing
        self.load_distribution: Dict[str, int] = defaultdict(int)
        
        # Performance metrics
        self.total_latency = 0.0
        self.request_count = 0
        self.lock = threading.Lock()
        
        # Resource monitoring
        self.cpu_usage = []
        self.memory_usage = []
        
        # Service state
        self.running = False
        self.worker_threads = []
        
        logger.info(f"Core node {node_id} initialized in region {region}")
    
    def start(self, num_workers: int = 8):
        """Start core node service"""
        self.running = True
        
        # Start worker threads
        for i in range(num_workers):
            thread = threading.Thread(target=self._worker_loop, args=(i,))
            thread.start()
            self.worker_threads.append(thread)
        
        # Start session management thread
        session_thread = threading.Thread(target=self._session_manager)
        session_thread.start()
        self.worker_threads.append(session_thread)
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=self._monitor_resources)
        monitor_thread.start()
        self.worker_threads.append(monitor_thread)
        
        logger.info(f"Core node {self.node_id} started with {num_workers} workers")
    
    def stop(self):
        """Stop core node service"""
        self.running = False
        for thread in self.worker_threads:
            thread.join()
        logger.info(f"Core node {self.node_id} stopped")
    
    def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process request at core node"""
        start_time = time.time()
        request_id = request.get("request_id", f"req-{time.time()}")
        
        try:
            # Create or update session
            session_id = request.get("session_id")
            if session_id:
                self._update_session(session_id, request)
            else:
                session_id = self._create_session(request)
            
            # Determine routing
            request_type = request.get("type", "simple")
            
            if request_type in ["analytics", "ml_inference", "big_data"]:
                # Route to cloud
                response = self._route_to_cloud(request)
                with self.lock:
                    self.routed_to_cloud += 1
            elif request_type in ["cache_update", "edge_data"]:
                # Route to edge
                response = self._route_to_edge(request)
                with self.lock:
                    self.routed_to_edge += 1
            else:
                # Process at core
                response = self._process_at_core(request)
            
            latency = time.time() - start_time
            
            with self.lock:
                self.processed_requests += 1
                self.total_latency += latency
                self.request_count += 1
            
            response["session_id"] = session_id
            response["latency_ms"] = latency * 1000
            return response
            
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            return {
                "request_id": request_id,
                "status": "error",
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }
    
    def _process_at_core(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process request at core node"""
        # Simulate core processing
        processing_delay = random.uniform(0.02, 0.08)
        time.sleep(processing_delay)
        
        operation = request.get("operation", "process")
        
        return {
            "status": "success",
            "data": {
                "result": f"Processed {operation} at core",
                "node": self.node_id,
                "timestamp": time.time()
            },
            "source": "core_node"
        }
    
    def _route_to_cloud(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Route request to cloud node"""
        # Select cloud node based on load
        cloud_node = self._select_cloud_node()
        
        # Update load distribution
        with self.lock:
            self.load_distribution[cloud_node] += 1
        
        # Simulate network delay to cloud
        network_delay = random.uniform(0.02, 0.05)
        time.sleep(network_delay)
        
        logger.debug(f"Routed request to cloud node {cloud_node}")
        
        return {
            "status": "success",
            "data": {
                "result": f"Processed on cloud node {cloud_node}",
                "routed_from": self.node_id
            },
            "source": "cloud_node"
        }
    
    def _route_to_edge(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Route request to edge node"""
        # Select edge node
        edge_node = random.choice(self.edge_nodes)
        
        # Simulate network delay to edge
        network_delay = random.uniform(0.005, 0.015)
        time.sleep(network_delay)
        
        logger.debug(f"Routed request to edge node {edge_node}")
        
        return {
            "status": "success",
            "data": {
                "result": f"Processed on edge node {edge_node}",
                "routed_from": self.node_id
            },
            "source": "edge_node"
        }
    
    def _select_cloud_node(self) -> str:
        """Select cloud node with least load"""
        with self.lock:
            if not self.cloud_nodes:
                return "cloud-default"
            
            # Simple load-based selection
            min_load = float('inf')
            selected_node = self.cloud_nodes[0]
            
            for node in self.cloud_nodes:
                load = self.load_distribution.get(node, 0)
                if load < min_load:
                    min_load = load
                    selected_node = node
            
            return selected_node
    
    def _create_session(self, request: Dict[str, Any]) -> str:
        """Create new session"""
        session_id = f"session-{time.time()}-{random.randint(1000, 9999)}"
        
        with self.lock:
            self.active_sessions[session_id] = {
                "created_at": time.time(),
                "last_activity": time.time(),
                "request_count": 1,
                "user_id": request.get("user_id", "anonymous")
            }
            self.session_count += 1
        
        logger.debug(f"Created session {session_id}")
        return session_id
    
    def _update_session(self, session_id: str, request: Dict[str, Any]):
        """Update existing session"""
        with self.lock:
            if session_id in self.active_sessions:
                session = self.active_sessions[session_id]
                session["last_activity"] = time.time()
                session["request_count"] += 1
    
    def _session_manager(self):
        """Manage active sessions and cleanup expired ones"""
        session_timeout = 300  # 5 minutes
        
        while self.running:
            try:
                current_time = time.time()
                expired_sessions = []
                
                with self.lock:
                    for session_id, session in self.active_sessions.items():
                        if current_time - session["last_activity"] > session_timeout:
                            expired_sessions.append(session_id)
                    
                    for session_id in expired_sessions:
                        del self.active_sessions[session_id]
                        logger.debug(f"Expired session {session_id}")
                
            except Exception as e:
                logger.error(f"Session manager error: {e}")
            
            time.sleep(10)
    
    def _worker_loop(self, worker_id: int):
        """Worker thread loop"""
        logger.info(f"Worker {worker_id} started on core node {self.node_id}")
        
        while self.running:
            try:
                # Simulate processing work
                time.sleep(0.01)
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
    
    def _monitor_resources(self):
        """Monitor CPU and memory usage"""
        while self.running:
            try:
                cpu = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory().percent
                
                with self.lock:
                    self.cpu_usage.append(cpu)
                    self.memory_usage.append(memory)
                    
                    # Keep only last 100 measurements
                    if len(self.cpu_usage) > 100:
                        self.cpu_usage.pop(0)
                    if len(self.memory_usage) > 100:
                        self.memory_usage.pop(0)
                
            except Exception as e:
                logger.error(f"Resource monitoring error: {e}")
            
            time.sleep(5)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get core node statistics"""
        with self.lock:
            avg_latency = (self.total_latency / self.request_count 
                          if self.request_count > 0 else 0)
            
            avg_cpu = sum(self.cpu_usage) / len(self.cpu_usage) if self.cpu_usage else 0
            avg_memory = sum(self.memory_usage) / len(self.memory_usage) if self.memory_usage else 0
            
            return {
                "node_id": self.node_id,
                "region": self.region,
                "processed_requests": self.processed_requests,
                "routed_to_cloud": self.routed_to_cloud,
                "routed_to_edge": self.routed_to_edge,
                "active_sessions": len(self.active_sessions),
                "total_sessions": self.session_count,
                "average_latency_ms": avg_latency * 1000,
                "average_cpu_percent": avg_cpu,
                "average_memory_percent": avg_memory,
                "load_distribution": dict(self.load_distribution)
            }