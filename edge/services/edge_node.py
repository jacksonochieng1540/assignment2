"""
Edge Node Service
Handles user requests with ultra-low latency, local processing, and caching
"""

import time
import threading
import logging
import random
from typing import Dict, List, Any, Optional
from collections import deque
import psutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EdgeNode:
    """Edge node for low-latency user request processing"""
    
    def __init__(self, node_id: str, region: str, core_nodes: List[str]):
        self.node_id = node_id
        self.region = region
        self.core_nodes = core_nodes
        
        # Local cache for frequently accessed data
        self.cache: Dict[str, Any] = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Request queue
        self.request_queue = deque(maxlen=1000)
        self.processed_requests = 0
        self.failed_requests = 0
        
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
        
        logger.info(f"Edge node {node_id} initialized in region {region}")
    
    def start(self, num_workers: int = 4):
        """Start edge node service"""
        self.running = True
        
        # Start worker threads
        for i in range(num_workers):
            thread = threading.Thread(target=self._worker_loop, args=(i,))
            thread.start()
            self.worker_threads.append(thread)
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=self._monitor_resources)
        monitor_thread.start()
        self.worker_threads.append(monitor_thread)
        
        logger.info(f"Edge node {self.node_id} started with {num_workers} workers")
    
    def stop(self):
        """Stop edge node service"""
        self.running = False
        for thread in self.worker_threads:
            thread.join()
        logger.info(f"Edge node {self.node_id} stopped")
    
    def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming user request"""
        start_time = time.time()
        request_id = request.get("request_id", f"req-{time.time()}")
        
        try:
            # Add to queue
            with self.lock:
                self.request_queue.append(request)
            
            # Check cache first
            data_key = request.get("data_key")
            if data_key and data_key in self.cache:
                with self.lock:
                    self.cache_hits += 1
                
                latency = time.time() - start_time
                logger.debug(f"Cache hit for {data_key} - {latency*1000:.2f}ms")
                
                return {
                    "request_id": request_id,
                    "status": "success",
                    "data": self.cache[data_key],
                    "source": "edge_cache",
                    "latency_ms": latency * 1000
                }
            
            # Cache miss - process locally or forward to core
            with self.lock:
                self.cache_misses += 1
            
            # Simulate local processing
            processing_delay = random.uniform(0.01, 0.05)
            time.sleep(processing_delay)
            
            # Determine if we need to forward to core
            request_type = request.get("type", "simple")
            
            if request_type == "complex" or request_type == "transaction":
                # Forward to core node
                response = self._forward_to_core(request)
            else:
                # Process locally
                response = self._process_locally(request)
            
            latency = time.time() - start_time
            
            with self.lock:
                self.processed_requests += 1
                self.total_latency += latency
                self.request_count += 1
            
            # Update cache if applicable
            if data_key and response.get("status") == "success":
                self.cache[data_key] = response.get("data")
            
            response["latency_ms"] = latency * 1000
            return response
            
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            with self.lock:
                self.failed_requests += 1
            
            return {
                "request_id": request_id,
                "status": "error",
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }
    
    def _process_locally(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process request locally on edge node"""
        # Simulate local processing
        operation = request.get("operation", "read")
        
        result = {
            "status": "success",
            "data": {
                "result": f"Processed {operation} on edge",
                "node": self.node_id,
                "timestamp": time.time()
            },
            "source": "edge_local"
        }
        
        return result
    
    def _forward_to_core(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Forward request to core node"""
        # Select core node (simple round-robin)
        core_node = random.choice(self.core_nodes)
        
        # Simulate network delay to core
        network_delay = random.uniform(0.005, 0.015)
        time.sleep(network_delay)
        
        logger.debug(f"Forwarded request to core node {core_node}")
        
        return {
            "status": "success",
            "data": {
                "result": f"Processed on core node {core_node}",
                "forwarded_from": self.node_id
            },
            "source": "core_node"
        }
    
    def _worker_loop(self, worker_id: int):
        """Worker thread loop"""
        logger.info(f"Worker {worker_id} started on edge node {self.node_id}")
        
        while self.running:
            try:
                # Process requests from queue
                if self.request_queue:
                    with self.lock:
                        if self.request_queue:
                            request = self.request_queue.popleft()
                        else:
                            request = None
                    
                    if request:
                        self.process_request(request)
                else:
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
        """Get edge node statistics"""
        with self.lock:
            total_requests = self.cache_hits + self.cache_misses
            cache_hit_rate = (self.cache_hits / total_requests * 100 
                            if total_requests > 0 else 0)
            
            avg_latency = (self.total_latency / self.request_count 
                          if self.request_count > 0 else 0)
            
            avg_cpu = sum(self.cpu_usage) / len(self.cpu_usage) if self.cpu_usage else 0
            avg_memory = sum(self.memory_usage) / len(self.memory_usage) if self.memory_usage else 0
            
            return {
                "node_id": self.node_id,
                "region": self.region,
                "processed_requests": self.processed_requests,
                "failed_requests": self.failed_requests,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "cache_hit_rate_percent": cache_hit_rate,
                "cache_size": len(self.cache),
                "queue_size": len(self.request_queue),
                "average_latency_ms": avg_latency * 1000,
                "average_cpu_percent": avg_cpu,
                "average_memory_percent": avg_memory
            }
    
    def clear_cache(self):
        """Clear edge cache"""
        with self.lock:
            self.cache.clear()
        logger.info(f"Cache cleared on edge node {self.node_id}")