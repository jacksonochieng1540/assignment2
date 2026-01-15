"""
Shared Utilities for Distributed Telecom System
Provides common functionality across edge, core, and cloud nodes
"""

import time
import logging
import json
import hashlib
from datetime import datetime
from typing import Dict, Any, List
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class NodeIdentifier:
    """Generates unique node identifiers"""
    
    @staticmethod
    def generate_id(node_type: str, region: str = "default") -> str:
        """Generate unique node ID"""
        timestamp = str(time.time())
        data = f"{node_type}-{region}-{timestamp}"
        return hashlib.md5(data.encode()).hexdigest()[:12]

class MessageFormatter:
    """Formats messages for inter-node communication"""
    
    @staticmethod
    def create_message(msg_type: str, payload: Dict[str, Any], 
                      source: str, destination: str) -> Dict[str, Any]:
        """Create standardized message format"""
        return {
            "message_id": NodeIdentifier.generate_id("msg", ""),
            "type": msg_type,
            "timestamp": datetime.now().isoformat(),
            "source": source,
            "destination": destination,
            "payload": payload,
            "checksum": MessageFormatter._calculate_checksum(payload)
        }
    
    @staticmethod
    def _calculate_checksum(payload: Dict[str, Any]) -> str:
        """Calculate message checksum for integrity"""
        data_str = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()[:16]
    
    @staticmethod
    def verify_message(message: Dict[str, Any]) -> bool:
        """Verify message integrity"""
        stored_checksum = message.get("checksum", "")
        calculated = MessageFormatter._calculate_checksum(message.get("payload", {}))
        return stored_checksum == calculated

class PerformanceMetrics:
    """Collects and manages performance metrics"""
    
    def __init__(self):
        self.metrics: Dict[str, List[float]] = {}
        self.lock = threading.Lock()
    
    def record_metric(self, metric_name: str, value: float):
        """Record a performance metric"""
        with self.lock:
            if metric_name not in self.metrics:
                self.metrics[metric_name] = []
            self.metrics[metric_name].append(value)
    
    def get_average(self, metric_name: str) -> float:
        """Get average of a metric"""
        with self.lock:
            values = self.metrics.get(metric_name, [])
            return sum(values) / len(values) if values else 0.0
    
    def get_percentile(self, metric_name: str, percentile: float) -> float:
        """Get percentile of a metric"""
        with self.lock:
            values = sorted(self.metrics.get(metric_name, []))
            if not values:
                return 0.0
            index = int(len(values) * percentile / 100)
            return values[min(index, len(values) - 1)]
    
    def get_all_metrics(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all metrics"""
        with self.lock:
            result = {}
            for name, values in self.metrics.items():
                if values:
                    result[name] = {
                        "count": len(values),
                        "average": sum(values) / len(values),
                        "min": min(values),
                        "max": max(values),
                        "p50": self._percentile(values, 50),
                        "p95": self._percentile(values, 95),
                        "p99": self._percentile(values, 99)
                    }
            return result
    
    @staticmethod
    def _percentile(values: List[float], percentile: float) -> float:
        """Calculate percentile"""
        sorted_vals = sorted(values)
        index = int(len(sorted_vals) * percentile / 100)
        return sorted_vals[min(index, len(sorted_vals) - 1)]

class Logger:
    """Enhanced logging with file rotation"""
    
    @staticmethod
    def get_logger(name: str, log_file: str = None) -> logging.Logger:
        """Get configured logger"""
        logger = logging.getLogger(name)
        
        if log_file:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

# Export all utilities
__all__ = [
    'NodeIdentifier',
    'MessageFormatter',
    'PerformanceMetrics',
    'Logger'
]