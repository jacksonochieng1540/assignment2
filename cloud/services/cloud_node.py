"""
Cloud Node Service
Handles compute-intensive operations, analytics, and storage
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

class CloudNode:
    """Cloud node for compute-intensive operations"""
    
    def __init__(self, node_id: str, data_center: str):
        self.node_id = node_id
        self.data_center = data_center
        
        # Storage
        self.data_store: Dict[str, Any] = {}
        self.storage_size = 0
        
        # Analytics
        self.analytics_jobs = 0
        self.ml_inferences = 0
        
        # Performance metrics
        self.processed_requests = 0
        self.total_latency = 0.0
        self.request_count = 0
        self.lock = threading.Lock()
        
        # Resource monitoring
        self.cpu_usage = []
        self.memory_usage = []
        
        # Service state
        self.running = False
        self.worker_threads = []
        
        logger.info(f"Cloud node {node_id} initialized in data center {data_center}")
    
    def start(self, num_workers: int = 16):
        """Start cloud node service"""
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
        
        logger.info(f"Cloud node {self.node_id} started with {num_workers} workers")
    
    def stop(self):
        """Stop cloud node service"""
        self.running = False
        for thread in self.worker_threads:
            thread.join()
        logger.info(f"Cloud node {self.node_id} stopped")
    
    def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process request at cloud node"""
        start_time = time.time()
        request_id = request.get("request_id", f"req-{time.time()}")
        
        try:
            request_type = request.get("type", "storage")
            
            if request_type == "analytics":
                response = self._run_analytics(request)
            elif request_type == "ml_inference":
                response = self._run_ml_inference(request)
            elif request_type == "big_data":
                response = self._process_big_data(request)
            elif request_type == "storage":
                response = self._handle_storage(request)
            else:
                response = self._generic_processing(request)
            
            latency = time.time() - start_time
            
            with self.lock:
                self.processed_requests += 1
                self.total_latency += latency
                self.request_count += 1
            
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
    
    def _run_analytics(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Run analytics job"""
        # Simulate intensive computation
        processing_delay = random.uniform(0.1, 0.5)
        time.sleep(processing_delay)
        
        with self.lock:
            self.analytics_jobs += 1
        
        return {
            "status": "success",
            "data": {
                "result": "Analytics completed",
                "metrics": {
                    "records_processed": random.randint(10000, 100000),
                    "insights_generated": random.randint(5, 20)
                },
                "node": self.node_id
            },
            "source": "cloud_analytics"
        }
    
    def _run_ml_inference(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Run ML inference"""
        # Simulate ML inference
        processing_delay = random.uniform(0.05, 0.2)
        time.sleep(processing_delay)
        
        with self.lock:
            self.ml_inferences += 1
        
        model_name = request.get("model", "default_model")
        
        return {
            "status": "success",
            "data": {
                "result": "Inference completed",
                "model": model_name,
                "prediction": random.uniform(0, 1),
                "confidence": random.uniform(0.7, 0.99),
                "node": self.node_id
            },
            "source": "cloud_ml"
        }
    
    def _process_big_data(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process big data request"""
        # Simulate big data processing
        processing_delay = random.uniform(0.2, 0.8)
        time.sleep(processing_delay)
        
        return {
            "status": "success",
            "data": {
                "result": "Big data processing completed",
                "data_size_gb": random.uniform(10, 1000),
                "processing_time_s": processing_delay,
                "node": self.node_id
            },
            "source": "cloud_bigdata"
        }
    
    def _handle_storage(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle storage operations"""
        operation = request.get("operation", "read")
        data_key = request.get("data_key")
        
        if operation == "write":
            data = request.get("data")
            with self.lock:
                self.data_store[data_key] = {
                    "data": data,
                    "timestamp": time.time(),
                    "version": random.randint(1, 100)
                }
                self.storage_size += 1
            
            return {
                "status": "success",
                "data": {
                    "result": "Data written",
                    "key": data_key,
                    "node": self.node_id
                },
                "source": "cloud_storage"
            }
        
        elif operation == "read":
            with self.lock:
                data = self.data_store.get(data_key)
            
            if data:
                return {
                    "status": "success",
                    "data": data,
                    "source": "cloud_storage"
                }
            else:
                return {
                    "status": "not_found",
                    "data": None,
                    "source": "cloud_storage"
                }
        
        else:
            return {
                "status": "error",
                "error": f"Unknown operation: {operation}"
            }
    
    def _generic_processing(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Generic processing"""
        processing_delay = random.uniform(0.05, 0.15)
        time.sleep(processing_delay)
        
        return {
            "status": "success",
            "data": {
                "result": "Processing completed",
                "node": self.node_id,
                "timestamp": time.time()
            },
            "source": "cloud_generic"
        }
    
    def _worker_loop(self, worker_id: int):
        """Worker thread loop"""
        logger.info(f"Worker {worker_id} started on cloud node {self.node_id}")
        
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
        """Get cloud node statistics"""
        with self.lock:
            avg_latency = (self.total_latency / self.request_count 
                          if self.request_count > 0 else 0)
            
            avg_cpu = sum(self.cpu_usage) / len(self.cpu_usage) if self.cpu_usage else 0
            avg_memory = sum(self.memory_usage) / len(self.memory_usage) if self.memory_usage else 0
            
            return {
                "node_id": self.node_id,
                "data_center": self.data_center,
                "processed_requests": self.processed_requests,
                "analytics_jobs": self.analytics_jobs,
                "ml_inferences": self.ml_inferences,
                "storage_size": self.storage_size,
                "average_latency_ms": avg_latency * 1000,
                "average_cpu_percent": avg_cpu,
                "average_memory_percent": avg_memory
            }