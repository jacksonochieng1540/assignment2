"""
Distributed Transaction Manager
Implements Two-Phase Commit (2PC) and Three-Phase Commit (3PC) protocols
"""

import time
import uuid
import threading
import logging
from enum import Enum
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

class TransactionState(Enum):
    INIT = "init"
    PREPARING = "preparing"
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"

class ParticipantState(Enum):
    READY = "ready"
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"

@dataclass
class Transaction:
    txn_id: str
    coordinator: str
    participants: List[str]
    operations: Dict[str, Any]
    state: TransactionState
    start_time: float
    timeout: float = 30.0
    
    def is_expired(self) -> bool:
        return time.time() - self.start_time > self.timeout

class TransactionCoordinator:
    """Coordinates distributed transactions using 2PC/3PC"""
    
    def __init__(self, coordinator_id: str):
        self.coordinator_id = coordinator_id
        self.transactions: Dict[str, Transaction] = {}
        self.lock = threading.Lock()
        self.commit_count = 0
        self.abort_count = 0
        self.total_latency = 0.0
        
        logger.info(f"Transaction coordinator {coordinator_id} initialized")
    
    def begin_transaction(self, participants: List[str], 
                         operations: Dict[str, Any]) -> str:
        """Begin a new distributed transaction"""
        txn_id = str(uuid.uuid4())
        
        transaction = Transaction(
            txn_id=txn_id,
            coordinator=self.coordinator_id,
            participants=participants,
            operations=operations,
            state=TransactionState.INIT,
            start_time=time.time()
        )
        
        with self.lock:
            self.transactions[txn_id] = transaction
        
        logger.info(f"Transaction {txn_id} started with {len(participants)} participants")
        return txn_id
    
    def execute_2pc(self, txn_id: str) -> bool:
        """Execute Two-Phase Commit protocol"""
        with self.lock:
            transaction = self.transactions.get(txn_id)
            if not transaction:
                logger.error(f"Transaction {txn_id} not found")
                return False
        
        start_time = time.time()
        
        # Phase 1: PREPARE
        logger.info(f"2PC Phase 1 (PREPARE) for transaction {txn_id}")
        transaction.state = TransactionState.PREPARING
        
        prepare_responses = self._send_prepare(transaction)
        
        # Check if all participants voted YES
        all_prepared = all(prepare_responses.values())
        
        if not all_prepared or transaction.is_expired():
            # Phase 2: ABORT
            logger.warning(f"Transaction {txn_id} aborting")
            transaction.state = TransactionState.ABORTING
            self._send_abort(transaction)
            transaction.state = TransactionState.ABORTED
            
            with self.lock:
                self.abort_count += 1
            
            return False
        
        # Phase 2: COMMIT
        logger.info(f"2PC Phase 2 (COMMIT) for transaction {txn_id}")
        transaction.state = TransactionState.COMMITTING
        self._send_commit(transaction)
        transaction.state = TransactionState.COMMITTED
        
        latency = time.time() - start_time
        with self.lock:
            self.commit_count += 1
            self.total_latency += latency
        
        logger.info(f"Transaction {txn_id} committed in {latency:.3f}s")
        return True
    
    def execute_3pc(self, txn_id: str) -> bool:
        """Execute Three-Phase Commit protocol"""
        with self.lock:
            transaction = self.transactions.get(txn_id)
            if not transaction:
                logger.error(f"Transaction {txn_id} not found")
                return False
        
        start_time = time.time()
        
        # Phase 1: CAN-COMMIT
        logger.info(f"3PC Phase 1 (CAN-COMMIT) for transaction {txn_id}")
        transaction.state = TransactionState.PREPARING
        
        can_commit_responses = self._send_can_commit(transaction)
        
        if not all(can_commit_responses.values()):
            logger.warning(f"Transaction {txn_id} aborting in phase 1")
            transaction.state = TransactionState.ABORTING
            self._send_abort(transaction)
            transaction.state = TransactionState.ABORTED
            
            with self.lock:
                self.abort_count += 1
            
            return False
        
        # Phase 2: PRE-COMMIT
        logger.info(f"3PC Phase 2 (PRE-COMMIT) for transaction {txn_id}")
        transaction.state = TransactionState.PREPARED
        
        pre_commit_responses = self._send_pre_commit(transaction)
        
        if not all(pre_commit_responses.values()) or transaction.is_expired():
            logger.warning(f"Transaction {txn_id} aborting in phase 2")
            transaction.state = TransactionState.ABORTING
            self._send_abort(transaction)
            transaction.state = TransactionState.ABORTED
            
            with self.lock:
                self.abort_count += 1
            
            return False
        
        # Phase 3: DO-COMMIT
        logger.info(f"3PC Phase 3 (DO-COMMIT) for transaction {txn_id}")
        transaction.state = TransactionState.COMMITTING
        self._send_do_commit(transaction)
        transaction.state = TransactionState.COMMITTED
        
        latency = time.time() - start_time
        with self.lock:
            self.commit_count += 1
            self.total_latency += latency
        
        logger.info(f"Transaction {txn_id} committed in {latency:.3f}s")
        return True
    
    def _send_prepare(self, transaction: Transaction) -> Dict[str, bool]:
        """Send PREPARE messages to participants"""
        responses = {}
        for participant in transaction.participants:
            # Simulate participant response (90% success rate)
            responses[participant] = time.time() % 1 > 0.1
            time.sleep(0.01)  # Simulate network delay
        return responses
    
    def _send_can_commit(self, transaction: Transaction) -> Dict[str, bool]:
        """Send CAN-COMMIT messages to participants"""
        responses = {}
        for participant in transaction.participants:
            # Simulate participant response (95% success rate)
            responses[participant] = time.time() % 1 > 0.05
            time.sleep(0.01)
        return responses
    
    def _send_pre_commit(self, transaction: Transaction) -> Dict[str, bool]:
        """Send PRE-COMMIT messages to participants"""
        responses = {}
        for participant in transaction.participants:
            # Simulate participant response (98% success rate)
            responses[participant] = time.time() % 1 > 0.02
            time.sleep(0.01)
        return responses
    
    def _send_commit(self, transaction: Transaction):
        """Send COMMIT messages to participants"""
        for participant in transaction.participants:
            # Simulate commit operation
            time.sleep(0.005)
            logger.debug(f"Sent COMMIT to {participant}")
    
    def _send_do_commit(self, transaction: Transaction):
        """Send DO-COMMIT messages to participants"""
        for participant in transaction.participants:
            # Simulate commit operation
            time.sleep(0.005)
            logger.debug(f"Sent DO-COMMIT to {participant}")
    
    def _send_abort(self, transaction: Transaction):
        """Send ABORT messages to participants"""
        for participant in transaction.participants:
            # Simulate abort operation
            time.sleep(0.005)
            logger.debug(f"Sent ABORT to {participant}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get transaction statistics"""
        with self.lock:
            total = self.commit_count + self.abort_count
            avg_latency = (self.total_latency / self.commit_count 
                          if self.commit_count > 0 else 0)
            
            return {
                "total_transactions": total,
                "committed": self.commit_count,
                "aborted": self.abort_count,
                "commit_rate": (self.commit_count / total * 100 
                               if total > 0 else 0),
                "average_latency_ms": avg_latency * 1000,
                "active_transactions": len(self.transactions)
            }

class TransactionParticipant:
    """Participant in distributed transactions"""
    
    def __init__(self, participant_id: str):
        self.participant_id = participant_id
        self.state = ParticipantState.READY
        self.current_txn: Optional[str] = None
        self.lock = threading.Lock()
        
        logger.info(f"Transaction participant {participant_id} initialized")
    
    def prepare(self, txn_id: str) -> bool:
        """Handle PREPARE request"""
        with self.lock:
            self.current_txn = txn_id
            self.state = ParticipantState.PREPARED
            # Simulate resource locking
            time.sleep(0.01)
            return True
    
    def commit(self, txn_id: str) -> bool:
        """Handle COMMIT request"""
        with self.lock:
            if self.current_txn != txn_id:
                return False
            self.state = ParticipantState.COMMITTED
            self.current_txn = None
            return True
    
    def abort(self, txn_id: str) -> bool:
        """Handle ABORT request"""
        with self.lock:
            if self.current_txn != txn_id:
                return False
            self.state = ParticipantState.ABORTED
            self.current_txn = None
            return True