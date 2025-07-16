"""
Partial Synchrony and Network Models

Implements the network model framework from Chapters 4-6 of 
"Foundations of Distributed Consensus and Blockchains" including
synchronous, asynchronous, and partial synchrony models.
"""

from typing import Dict, List, Set, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import time
import random
import heapq
from abc import ABC, abstractmethod


class NetworkModel(Enum):
    """Different network timing models."""
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"
    PARTIAL_SYNCHRONY = "partial_synchrony"


class MessageType(Enum):
    """Types of messages in consensus protocols."""
    ECHO = "echo"
    READY = "ready"
    VOTE = "vote"
    PROPOSE = "propose"
    PREPARE = "prepare"
    COMMIT = "commit"
    VIEW_CHANGE = "view_change"


@dataclass
class NetworkMessage:
    """A message in the network with timing information."""
    sender_id: str
    receiver_id: str
    message_type: MessageType
    content: Any
    timestamp: float
    round_number: int
    delivery_time: Optional[float] = None
    size_bits: int = 0
    
    def __post_init__(self):
        if self.size_bits == 0:
            # Estimate message size
            self.size_bits = len(str(self.content).encode('utf-8')) * 8


@dataclass
class NetworkParameters:
    """Parameters defining network behavior."""
    # Synchronous model parameters
    max_delay: float = 1.0  # Δ in synchronous model
    
    # Asynchronous model parameters
    min_delay: float = 0.1
    avg_delay: float = 0.5
    max_async_delay: float = 10.0
    
    # Partial synchrony parameters
    global_stabilization_time: Optional[float] = None  # GST
    unknown_bound: bool = True  # Whether Δ is unknown
    
    # Network quality parameters
    message_loss_rate: float = 0.0
    duplicate_rate: float = 0.0
    reorder_rate: float = 0.0


class NetworkSimulator:
    """
    Simulates different network models for consensus protocols.
    
    Implements the network models from Chapters 4-6.
    """
    
    def __init__(self, model: NetworkModel, parameters: NetworkParameters):
        self.model = model
        self.parameters = parameters
        self.current_time = 0.0
        self.message_queue: List[Tuple[float, NetworkMessage]] = []
        self.delivered_messages: List[NetworkMessage] = []
        self.node_clocks: Dict[str, float] = {}
        self.round_number = 0
        
        # Partial synchrony state
        self.gst_reached = False
        self.gst_time = parameters.global_stabilization_time
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_delivered': 0,
            'messages_lost': 0,
            'total_delay': 0.0,
            'max_observed_delay': 0.0
        }
    
    def send_message(self, sender_id: str, receiver_id: str, 
                    message_type: MessageType, content: Any) -> None:
        """Send a message through the network."""
        message = NetworkMessage(
            sender_id=sender_id,
            receiver_id=receiver_id,
            message_type=message_type,
            content=content,
            timestamp=self.current_time,
            round_number=self.round_number
        )
        
        self.stats['messages_sent'] += 1
        
        # Check for message loss
        if random.random() < self.parameters.message_loss_rate:
            self.stats['messages_lost'] += 1
            return
        
        # Calculate delivery time based on network model
        delivery_time = self._calculate_delivery_time(message)
        message.delivery_time = delivery_time
        
        # Add to message queue
        heapq.heappush(self.message_queue, (delivery_time, message))
        
        # Handle duplicates
        if random.random() < self.parameters.duplicate_rate:
            duplicate_delay = delivery_time + random.uniform(0.1, 1.0)
            duplicate_message = NetworkMessage(
                sender_id=sender_id,
                receiver_id=receiver_id,
                message_type=message_type,
                content=content,
                timestamp=self.current_time,
                round_number=self.round_number,
                delivery_time=duplicate_delay,
                size_bits=message.size_bits
            )
            heapq.heappush(self.message_queue, (duplicate_delay, duplicate_message))
    
    def _calculate_delivery_time(self, message: NetworkMessage) -> float:
        """Calculate when a message should be delivered based on network model."""
        if self.model == NetworkModel.SYNCHRONOUS:
            # In synchronous model, all messages delivered within Δ
            delay = random.uniform(0, self.parameters.max_delay)
            return self.current_time + delay
        
        elif self.model == NetworkModel.ASYNCHRONOUS:
            # In asynchronous model, arbitrary but finite delays
            delay = random.exponential(self.parameters.avg_delay)
            delay = min(delay, self.parameters.max_async_delay)
            delay = max(delay, self.parameters.min_delay)
            return self.current_time + delay
        
        elif self.model == NetworkModel.PARTIAL_SYNCHRONY:
            # Before GST: asynchronous behavior
            # After GST: synchronous behavior with known/unknown Δ
            if not self.gst_reached and (self.gst_time is None or self.current_time < self.gst_time):
                # Pre-GST: asynchronous delays
                delay = random.exponential(self.parameters.avg_delay * 2)
                delay = min(delay, self.parameters.max_async_delay)
                delay = max(delay, self.parameters.min_delay)
            else:
                # Post-GST: synchronous delays
                if not self.gst_reached and self.gst_time is not None:
                    self.gst_reached = True
                
                if self.parameters.unknown_bound:
                    # Δ unknown but exists
                    delay = random.uniform(0, self.parameters.max_delay * 1.5)
                else:
                    # Δ known
                    delay = random.uniform(0, self.parameters.max_delay)
            
            return self.current_time + delay
        
        else:
            raise ValueError(f"Unknown network model: {self.model}")
    
    def advance_time(self, target_time: Optional[float] = None) -> List[NetworkMessage]:
        """
        Advance simulation time and deliver messages.
        
        Returns messages delivered during this time advancement.
        """
        if target_time is None:
            # Advance to next message delivery
            if not self.message_queue:
                return []
            target_time = self.message_queue[0][0]
        
        delivered = []
        
        # Deliver all messages up to target time
        while self.message_queue and self.message_queue[0][0] <= target_time:
            delivery_time, message = heapq.heappop(self.message_queue)
            
            # Handle message reordering
            if random.random() < self.parameters.reorder_rate and self.message_queue:
                # Reorder by swapping with next message
                next_time, next_message = heapq.heappop(self.message_queue)
                heapq.heappush(self.message_queue, (delivery_time + 0.01, next_message))
                message = next_message
                delivery_time = next_time
            
            # Update statistics
            actual_delay = delivery_time - message.timestamp
            self.stats['total_delay'] += actual_delay
            self.stats['max_observed_delay'] = max(self.stats['max_observed_delay'], actual_delay)
            self.stats['messages_delivered'] += 1
            
            # Deliver message
            self.delivered_messages.append(message)
            delivered.append(message)
            
            # Update receiver's clock (for asynchronous model)
            if message.receiver_id not in self.node_clocks:
                self.node_clocks[message.receiver_id] = 0.0
            self.node_clocks[message.receiver_id] = max(
                self.node_clocks[message.receiver_id],
                delivery_time
            )
        
        self.current_time = target_time
        return delivered
    
    def start_new_round(self) -> None:
        """Start a new round in the protocol."""
        self.round_number += 1
        
        if self.model == NetworkModel.SYNCHRONOUS:
            # In synchronous model, advance time by Δ for each round
            self.advance_time(self.current_time + self.parameters.max_delay)
    
    def get_network_delay_bound(self) -> Optional[float]:
        """Get the current network delay bound (if known)."""
        if self.model == NetworkModel.SYNCHRONOUS:
            return self.parameters.max_delay
        elif self.model == NetworkModel.PARTIAL_SYNCHRONY:
            if self.gst_reached and not self.parameters.unknown_bound:
                return self.parameters.max_delay
        return None
    
    def is_synchronous_now(self) -> bool:
        """Check if network is currently behaving synchronously."""
        if self.model == NetworkModel.SYNCHRONOUS:
            return True
        elif self.model == NetworkModel.PARTIAL_SYNCHRONY:
            return self.gst_reached
        return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get network simulation statistics."""
        stats = self.stats.copy()
        stats['current_time'] = self.current_time
        stats['round_number'] = self.round_number
        stats['gst_reached'] = self.gst_reached
        stats['pending_messages'] = len(self.message_queue)
        
        if stats['messages_delivered'] > 0:
            stats['avg_delay'] = stats['total_delay'] / stats['messages_delivered']
            stats['delivery_rate'] = stats['messages_delivered'] / stats['messages_sent']
        else:
            stats['avg_delay'] = 0.0
            stats['delivery_rate'] = 0.0
        
        return stats


class PartialSynchronyOracle:
    """
    Oracle for partial synchrony model that can trigger GST.
    
    Implements the GST (Global Stabilization Time) concept from Chapter 6.
    """
    
    def __init__(self, network: NetworkSimulator):
        self.network = network
        self.gst_triggered = False
        self.gst_trigger_conditions: List[Callable[[], bool]] = []
    
    def add_gst_trigger(self, condition: Callable[[], bool]) -> None:
        """Add a condition that can trigger GST."""
        self.gst_trigger_conditions.append(condition)
    
    def check_gst_trigger(self) -> bool:
        """Check if GST should be triggered."""
        if self.gst_triggered:
            return False
        
        for condition in self.gst_trigger_conditions:
            if condition():
                self.trigger_gst()
                return True
        
        return False
    
    def trigger_gst(self) -> None:
        """Manually trigger GST."""
        if not self.gst_triggered:
            self.gst_triggered = True
            self.network.gst_reached = True
            self.network.gst_time = self.network.current_time
    
    def time_until_gst(self) -> Optional[float]:
        """Get time until GST (if known)."""
        if self.network.gst_time is not None and not self.gst_triggered:
            return max(0, self.network.gst_time - self.network.current_time)
        return None


class AdversarialNetworkScheduler:
    """
    Adversarial message scheduler for theoretical analysis.
    
    Implements worst-case message delivery patterns to test protocol robustness.
    """
    
    def __init__(self, network: NetworkSimulator):
        self.network = network
        self.adversarial_strategies: List[str] = []
        self.message_buffer: List[NetworkMessage] = []
    
    def add_strategy(self, strategy: str) -> None:
        """Add an adversarial strategy."""
        valid_strategies = [
            "delay_honest_nodes",
            "prioritize_byzantine_messages", 
            "create_network_partition",
            "delay_until_timeout",
            "reorder_by_content"
        ]
        
        if strategy not in valid_strategies:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        self.adversarial_strategies.append(strategy)
    
    def intercept_message(self, message: NetworkMessage) -> bool:
        """
        Intercept a message for adversarial scheduling.
        
        Returns True if message was intercepted, False if delivered normally.
        """
        if not self.adversarial_strategies:
            return False
        
        # Apply adversarial strategies
        for strategy in self.adversarial_strategies:
            if self._apply_strategy(strategy, message):
                self.message_buffer.append(message)
                return True
        
        return False
    
    def _apply_strategy(self, strategy: str, message: NetworkMessage) -> bool:
        """Apply a specific adversarial strategy to a message."""
        if strategy == "delay_honest_nodes":
            # Delay messages from honest nodes
            if not self._is_byzantine_node(message.sender_id):
                return True
        
        elif strategy == "prioritize_byzantine_messages":
            # Only delay if this is an honest node and there are Byzantine messages pending
            if not self._is_byzantine_node(message.sender_id):
                byzantine_pending = any(
                    self._is_byzantine_node(m.sender_id) for m in self.message_buffer
                )
                return byzantine_pending
        
        elif strategy == "create_network_partition":
            # Create artificial network partitions
            partition_a = {"node_0", "node_1", "node_2"}
            partition_b = {"node_3", "node_4", "node_5"}
            
            if (message.sender_id in partition_a and message.receiver_id in partition_b) or \
               (message.sender_id in partition_b and message.receiver_id in partition_a):
                return True
        
        elif strategy == "delay_until_timeout":
            # Delay critical messages until just before timeout
            if message.message_type in [MessageType.VOTE, MessageType.COMMIT]:
                return True
        
        elif strategy == "reorder_by_content":
            # Reorder messages to create worst-case scenarios
            return True
        
        return False
    
    def _is_byzantine_node(self, node_id: str) -> bool:
        """Check if a node is Byzantine (simplified heuristic)."""
        # Simple heuristic: assume nodes with higher IDs are more likely Byzantine
        try:
            node_num = int(node_id.split('_')[-1])
            return node_num >= 3  # Assume last 1/3 of nodes are Byzantine
        except:
            return False
    
    def release_messages(self, count: Optional[int] = None) -> List[NetworkMessage]:
        """Release buffered messages according to adversarial strategy."""
        if count is None:
            count = len(self.message_buffer)
        
        released = []
        for _ in range(min(count, len(self.message_buffer))):
            # Release oldest message (FIFO for simplicity)
            message = self.message_buffer.pop(0)
            released.append(message)
            
            # Re-inject into network with additional delay
            heapq.heappush(
                self.network.message_queue,
                (self.network.current_time + 0.1, message)
            )
        
        return released


class SynchronyAssumptionTracker:
    """
    Tracks violations of synchrony assumptions during protocol execution.
    
    Helps analyze when protocols fail due to timing assumptions being violated.
    """
    
    def __init__(self, expected_model: NetworkModel, network: NetworkSimulator):
        self.expected_model = expected_model
        self.network = network
        self.violations: List[Dict[str, Any]] = []
        self.monitoring_active = True
    
    def check_synchrony_violation(self, message: NetworkMessage) -> Optional[Dict[str, Any]]:
        """Check if a message violates synchrony assumptions."""
        if not self.monitoring_active:
            return None
        
        violation = None
        actual_delay = message.delivery_time - message.timestamp
        
        if self.expected_model == NetworkModel.SYNCHRONOUS:
            # Check if delay exceeds Δ
            max_allowed_delay = self.network.parameters.max_delay
            if actual_delay > max_allowed_delay:
                violation = {
                    "type": "synchrony_violation",
                    "expected_max_delay": max_allowed_delay,
                    "actual_delay": actual_delay,
                    "excess_delay": actual_delay - max_allowed_delay,
                    "message": message,
                    "time": self.network.current_time
                }
        
        elif self.expected_model == NetworkModel.PARTIAL_SYNCHRONY:
            # Check if post-GST delays exceed bound
            if self.network.gst_reached and not self.network.parameters.unknown_bound:
                max_allowed_delay = self.network.parameters.max_delay
                if actual_delay > max_allowed_delay:
                    violation = {
                        "type": "post_gst_violation",
                        "expected_max_delay": max_allowed_delay,
                        "actual_delay": actual_delay,
                        "gst_time": self.network.gst_time,
                        "message": message,
                        "time": self.network.current_time
                    }
        
        if violation:
            self.violations.append(violation)
        
        return violation
    
    def get_violation_summary(self) -> Dict[str, Any]:
        """Get summary of synchrony violations."""
        if not self.violations:
            return {"total_violations": 0}
        
        violation_types = {}
        max_excess = 0.0
        total_excess = 0.0
        
        for violation in self.violations:
            v_type = violation["type"]
            violation_types[v_type] = violation_types.get(v_type, 0) + 1
            
            if "excess_delay" in violation:
                excess = violation["excess_delay"]
                max_excess = max(max_excess, excess)
                total_excess += excess
        
        return {
            "total_violations": len(self.violations),
            "violation_types": violation_types,
            "max_excess_delay": max_excess,
            "avg_excess_delay": total_excess / len(self.violations),
            "violations": self.violations
        }


# Helper functions for theoretical analysis

def analyze_flp_impossibility_conditions(network: NetworkSimulator) -> Dict[str, Any]:
    """
    Analyze if current network conditions trigger FLP impossibility.
    
    From Chapter 5: FLP impossibility applies in pure asynchronous model.
    """
    analysis = {
        "model": network.model.value,
        "flp_applies": False,
        "reasons": [],
        "escape_mechanisms": []
    }
    
    if network.model == NetworkModel.ASYNCHRONOUS:
        analysis["flp_applies"] = True
        analysis["reasons"].append("Pure asynchronous model - FLP impossibility applies")
        analysis["escape_mechanisms"].extend([
            "Add synchrony assumptions (partial synchrony)",
            "Use randomization",
            "Weaken agreement (approximate consensus)",
            "Use failure detectors"
        ])
    
    elif network.model == NetworkModel.PARTIAL_SYNCHRONY:
        if not network.gst_reached:
            analysis["flp_applies"] = True
            analysis["reasons"].append("Pre-GST phase behaves asynchronously")
        else:
            analysis["reasons"].append("Post-GST phase has synchrony - FLP escaped")
        
        analysis["escape_mechanisms"].append("GST eventually provides synchrony")
    
    else:  # SYNCHRONOUS
        analysis["reasons"].append("Synchronous model - FLP does not apply")
        analysis["escape_mechanisms"].append("Built-in synchrony assumptions")
    
    return analysis


def calculate_theoretical_bounds(network_model: NetworkModel, n: int, f: int) -> Dict[str, Any]:
    """
    Calculate theoretical bounds for different network models.
    
    Based on results from Chapters 4-6.
    """
    bounds = {
        "model": network_model.value,
        "n": n,
        "f": f
    }
    
    if network_model == NetworkModel.SYNCHRONOUS:
        bounds.update({
            "round_complexity": {
                "deterministic_lower": f + 1,
                "deterministic_upper": f + 1,  # Dolev-Strong achieves this
                "randomized_expected": "O(1)"  # With high probability
            },
            "communication_complexity": {
                "lower_bound": f * f,  # Dolev-Reischuk bound
                "upper_bound": "O(n^2 * f)"  # Dolev-Strong
            },
            "fault_tolerance": {
                "max_faults": n - 1,
                "note": "Can tolerate any number of faults in synchronous model"
            }
        })
    
    elif network_model == NetworkModel.ASYNCHRONOUS:
        bounds.update({
            "round_complexity": {
                "deterministic": "Impossible (FLP)",
                "randomized_expected": "O(1)",
                "note": "Expected rounds with randomization"
            },
            "communication_complexity": {
                "lower_bound": "Ω(n^2)",
                "upper_bound": "O(n^2)"
            },
            "fault_tolerance": {
                "max_faults": (n - 1) // 3,
                "note": "At most f < n/3 faults for safety and liveness"
            }
        })
    
    elif network_model == NetworkModel.PARTIAL_SYNCHRONY:
        bounds.update({
            "round_complexity": {
                "deterministic_post_gst": f + 1,
                "randomized_expected": "O(1)",
                "note": "Bounds apply after GST"
            },
            "communication_complexity": {
                "lower_bound": "Ω(n^2)",
                "upper_bound": "O(n^2 * f)"
            },
            "fault_tolerance": {
                "max_faults": (n - 1) // 3,
                "note": "f < n/3 required for both safety and liveness"
            }
        })
    
    return bounds
