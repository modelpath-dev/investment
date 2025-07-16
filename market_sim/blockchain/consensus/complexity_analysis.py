"""
Communication and Round Complexity Analysis

Implements theoretical analysis tools from Chapters 10 and 11 of 
"Foundations of Distributed Consensus and Blockchains" for measuring
protocol efficiency and proving lower bounds.
"""

from typing import Dict, List, Set, Optional, Any, Tuple, Callable
from dataclasses import dataclass
from enum import Enum
import time
import statistics
from abc import ABC, abstractmethod


class ComplexityMetric(Enum):
    """Types of complexity metrics to measure."""
    ROUND_COMPLEXITY = "round_complexity"
    COMMUNICATION_COMPLEXITY = "communication_complexity"
    MESSAGE_COMPLEXITY = "message_complexity"


@dataclass
class ComplexityMeasurement:
    """A complexity measurement for a protocol execution."""
    metric: ComplexityMetric
    value: int
    protocol_name: str
    num_nodes: int
    num_faults: int
    execution_time: float
    additional_data: Dict[str, Any]


class ComplexityAnalyzer:
    """
    Analyzes complexity metrics for consensus protocols.
    
    Implements measurement techniques for the theoretical results
    in Chapters 10 and 11.
    """
    
    def __init__(self):
        self.measurements: List[ComplexityMeasurement] = []
        self.active_measurements: Dict[str, Dict[str, Any]] = {}
    
    def start_measurement(self, execution_id: str, protocol_name: str, 
                         num_nodes: int, num_faults: int) -> None:
        """Start measuring complexity for a protocol execution."""
        self.active_measurements[execution_id] = {
            'protocol_name': protocol_name,
            'num_nodes': num_nodes,
            'num_faults': num_faults,
            'start_time': time.time(),
            'round_count': 0,
            'messages_sent': 0,
            'total_bits_sent': 0,
            'round_start_time': time.time(),
            'messages_per_round': [],
            'bits_per_round': []
        }
    
    def record_round_start(self, execution_id: str) -> None:
        """Record the start of a new round."""
        if execution_id not in self.active_measurements:
            return
        
        measurement = self.active_measurements[execution_id]
        measurement['round_count'] += 1
        measurement['round_start_time'] = time.time()
        measurement['messages_per_round'].append(0)
        measurement['bits_per_round'].append(0)
    
    def record_message_sent(self, execution_id: str, message_size_bits: int) -> None:
        """Record a message being sent."""
        if execution_id not in self.active_measurements:
            return
        
        measurement = self.active_measurements[execution_id]
        measurement['messages_sent'] += 1
        measurement['total_bits_sent'] += message_size_bits
        
        # Update current round statistics
        if measurement['messages_per_round']:
            measurement['messages_per_round'][-1] += 1
            measurement['bits_per_round'][-1] += message_size_bits
    
    def finish_measurement(self, execution_id: str, 
                          additional_data: Optional[Dict[str, Any]] = None) -> None:
        """Finish measuring and record the results."""
        if execution_id not in self.active_measurements:
            return
        
        measurement = self.active_measurements[execution_id]
        end_time = time.time()
        execution_time = end_time - measurement['start_time']
        
        # Record all complexity metrics
        metrics = [
            ComplexityMeasurement(
                metric=ComplexityMetric.ROUND_COMPLEXITY,
                value=measurement['round_count'],
                protocol_name=measurement['protocol_name'],
                num_nodes=measurement['num_nodes'],
                num_faults=measurement['num_faults'],
                execution_time=execution_time,
                additional_data=additional_data or {}
            ),
            ComplexityMeasurement(
                metric=ComplexityMetric.MESSAGE_COMPLEXITY,
                value=measurement['messages_sent'],
                protocol_name=measurement['protocol_name'],
                num_nodes=measurement['num_nodes'],
                num_faults=measurement['num_faults'],
                execution_time=execution_time,
                additional_data=additional_data or {}
            ),
            ComplexityMeasurement(
                metric=ComplexityMetric.COMMUNICATION_COMPLEXITY,
                value=measurement['total_bits_sent'],
                protocol_name=measurement['protocol_name'],
                num_nodes=measurement['num_nodes'],
                num_faults=measurement['num_faults'],
                execution_time=execution_time,
                additional_data=additional_data or {}
            )
        ]
        
        self.measurements.extend(metrics)
        del self.active_measurements[execution_id]
    
    def get_measurements(self, metric: ComplexityMetric, 
                        protocol_name: Optional[str] = None) -> List[ComplexityMeasurement]:
        """Get measurements for a specific metric and optionally protocol."""
        results = [m for m in self.measurements if m.metric == metric]
        if protocol_name:
            results = [m for m in results if m.protocol_name == protocol_name]
        return results
    
    def analyze_round_complexity(self, protocol_name: str) -> Dict[str, Any]:
        """
        Analyze round complexity for a protocol.
        
        Implements analysis techniques from Chapter 10.
        """
        measurements = self.get_measurements(ComplexityMetric.ROUND_COMPLEXITY, protocol_name)
        
        if not measurements:
            return {"error": "No measurements found"}
        
        round_counts = [m.value for m in measurements]
        fault_counts = [m.num_faults for m in measurements]
        node_counts = [m.num_nodes for m in measurements]
        
        analysis = {
            "protocol_name": protocol_name,
            "total_executions": len(measurements),
            "round_complexity": {
                "min": min(round_counts),
                "max": max(round_counts),
                "avg": statistics.mean(round_counts),
                "median": statistics.median(round_counts),
                "std_dev": statistics.stdev(round_counts) if len(round_counts) > 1 else 0
            },
            "fault_tolerance": {
                "min_faults": min(fault_counts),
                "max_faults": max(fault_counts),
                "avg_faults": statistics.mean(fault_counts)
            },
            "scalability": {
                "min_nodes": min(node_counts),
                "max_nodes": max(node_counts),
                "avg_nodes": statistics.mean(node_counts)
            }
        }
        
        # Check theoretical bounds
        analysis.update(self._check_round_complexity_bounds(measurements))
        
        return analysis
    
    def analyze_communication_complexity(self, protocol_name: str) -> Dict[str, Any]:
        """
        Analyze communication complexity for a protocol.
        
        Implements analysis techniques from Chapter 11.
        """
        comm_measurements = self.get_measurements(ComplexityMetric.COMMUNICATION_COMPLEXITY, protocol_name)
        msg_measurements = self.get_measurements(ComplexityMetric.MESSAGE_COMPLEXITY, protocol_name)
        
        if not comm_measurements or not msg_measurements:
            return {"error": "No measurements found"}
        
        bits_sent = [m.value for m in comm_measurements]
        messages_sent = [m.value for m in msg_measurements]
        node_counts = [m.num_nodes for m in comm_measurements]
        fault_counts = [m.num_faults for m in comm_measurements]
        
        analysis = {
            "protocol_name": protocol_name,
            "total_executions": len(comm_measurements),
            "communication_complexity": {
                "min_bits": min(bits_sent),
                "max_bits": max(bits_sent),
                "avg_bits": statistics.mean(bits_sent),
                "median_bits": statistics.median(bits_sent),
                "std_dev_bits": statistics.stdev(bits_sent) if len(bits_sent) > 1 else 0
            },
            "message_complexity": {
                "min_messages": min(messages_sent),
                "max_messages": max(messages_sent),
                "avg_messages": statistics.mean(messages_sent),
                "median_messages": statistics.median(messages_sent),
                "std_dev_messages": statistics.stdev(messages_sent) if len(messages_sent) > 1 else 0
            },
            "efficiency_metrics": {
                "avg_bits_per_message": statistics.mean([b/m for b, m in zip(bits_sent, messages_sent) if m > 0]),
                "bits_per_node": [b/n for b, n in zip(bits_sent, node_counts)],
                "messages_per_node": [m/n for m, n in zip(messages_sent, node_counts)]
            }
        }
        
        # Check theoretical bounds
        analysis.update(self._check_communication_complexity_bounds(comm_measurements))
        
        return analysis
    
    def _check_round_complexity_bounds(self, measurements: List[ComplexityMeasurement]) -> Dict[str, Any]:
        """
        Check if measurements satisfy theoretical round complexity bounds.
        
        Based on results from Chapter 10:
        - Deterministic protocols: ≥ f + 1 rounds
        - Randomized protocols: different bounds based on failure probability
        """
        bounds_analysis = {
            "theoretical_bounds": {},
            "violations": []
        }
        
        for measurement in measurements:
            f = measurement.num_faults
            rounds = measurement.value
            
            # Check deterministic lower bound (Theorem 10)
            deterministic_lower_bound = f + 1
            bounds_analysis["theoretical_bounds"]["deterministic_lower_bound"] = deterministic_lower_bound
            
            if rounds < deterministic_lower_bound:
                bounds_analysis["violations"].append({
                    "type": "deterministic_lower_bound",
                    "expected_min": deterministic_lower_bound,
                    "actual": rounds,
                    "measurement": measurement
                })
            
            # Check randomized lower bound (Theorem 11)
            n = measurement.num_nodes
            if n > 0:
                randomized_lower_bound = (2 * n) // (n - f) - 1
                bounds_analysis["theoretical_bounds"]["randomized_lower_bound"] = randomized_lower_bound
                
                if rounds < randomized_lower_bound:
                    bounds_analysis["violations"].append({
                        "type": "randomized_lower_bound",
                        "expected_min": randomized_lower_bound,
                        "actual": rounds,
                        "measurement": measurement
                    })
        
        return bounds_analysis
    
    def _check_communication_complexity_bounds(self, measurements: List[ComplexityMeasurement]) -> Dict[str, Any]:
        """
        Check if measurements satisfy theoretical communication complexity bounds.
        
        Based on results from Chapter 11:
        - Deterministic protocols: ≥ ⌊f/2⌋² bits (Theorem 12)
        """
        bounds_analysis = {
            "theoretical_bounds": {},
            "violations": []
        }
        
        for measurement in measurements:
            f = measurement.num_faults
            bits = measurement.value
            
            # Check Dolev-Reischuk lower bound (Theorem 12)
            dolev_reischuk_bound = (f // 2) ** 2
            bounds_analysis["theoretical_bounds"]["dolev_reischuk_bound"] = dolev_reischuk_bound
            
            if bits < dolev_reischuk_bound:
                bounds_analysis["violations"].append({
                    "type": "dolev_reischuk_bound",
                    "expected_min": dolev_reischuk_bound,
                    "actual": bits,
                    "measurement": measurement
                })
        
        return bounds_analysis
    
    def compare_protocols(self, protocol_names: List[str], 
                         metric: ComplexityMetric) -> Dict[str, Any]:
        """Compare multiple protocols on a specific complexity metric."""
        comparison = {
            "metric": metric.value,
            "protocols": {},
            "ranking": []
        }
        
        for protocol_name in protocol_names:
            measurements = self.get_measurements(metric, protocol_name)
            if measurements:
                values = [m.value for m in measurements]
                comparison["protocols"][protocol_name] = {
                    "measurements": len(measurements),
                    "avg": statistics.mean(values),
                    "min": min(values),
                    "max": max(values),
                    "std_dev": statistics.stdev(values) if len(values) > 1 else 0
                }
        
        # Rank protocols by average performance (lower is better)
        ranking = sorted(
            comparison["protocols"].items(),
            key=lambda x: x[1]["avg"]
        )
        comparison["ranking"] = [{"protocol": name, **stats} for name, stats in ranking]
        
        return comparison


class ProtocolComplexityWrapper:
    """
    Wrapper for instrumenting existing consensus protocols with complexity analysis.
    
    Automatically measures complexity during protocol execution.
    """
    
    def __init__(self, protocol_name: str, analyzer: ComplexityAnalyzer):
        self.protocol_name = protocol_name
        self.analyzer = analyzer
        self.execution_counter = 0
    
    def wrap_protocol_execution(self, protocol_func: Callable, 
                              num_nodes: int, num_faults: int, 
                              *args, **kwargs) -> Tuple[Any, str]:
        """
        Wrap a protocol execution function with complexity measurement.
        
        Returns (result, execution_id)
        """
        self.execution_counter += 1
        execution_id = f"{self.protocol_name}_{self.execution_counter}"
        
        # Start measurement
        self.analyzer.start_measurement(execution_id, self.protocol_name, num_nodes, num_faults)
        
        try:
            # Execute protocol
            result = protocol_func(*args, **kwargs)
            
            # Finish measurement
            additional_data = {
                "success": True,
                "result_type": type(result).__name__
            }
            if hasattr(result, '__dict__'):
                additional_data.update(result.__dict__)
            
            self.analyzer.finish_measurement(execution_id, additional_data)
            
            return result, execution_id
            
        except Exception as e:
            # Finish measurement with error
            self.analyzer.finish_measurement(execution_id, {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            })
            raise
    
    def create_instrumented_message_sender(self, execution_id: str):
        """Create a message sender function that automatically measures complexity."""
        def send_message(message: Any, message_size_bits: Optional[int] = None):
            if message_size_bits is None:
                # Estimate message size
                if hasattr(message, 'serialize'):
                    message_size_bits = len(message.serialize()) * 8
                else:
                    message_size_bits = len(str(message).encode('utf-8')) * 8
            
            self.analyzer.record_message_sent(execution_id, message_size_bits)
        
        return send_message
    
    def create_instrumented_round_notifier(self, execution_id: str):
        """Create a round notification function that automatically measures rounds."""
        def notify_round_start():
            self.analyzer.record_round_start(execution_id)
        
        return notify_round_start


# Theoretical bounds verification functions

def verify_dolev_strong_optimality(analyzer: ComplexityAnalyzer) -> Dict[str, Any]:
    """
    Verify that Dolev-Strong protocol achieves optimal round complexity.
    
    From Chapter 9: Dolev-Strong achieves f+1 rounds, which is optimal for deterministic protocols.
    """
    measurements = analyzer.get_measurements(ComplexityMetric.ROUND_COMPLEXITY, "dolev_strong")
    
    if not measurements:
        return {"error": "No Dolev-Strong measurements found"}
    
    verification = {
        "protocol": "dolev_strong",
        "theoretical_optimality": "f + 1 rounds (optimal for deterministic)",
        "measurements": len(measurements),
        "optimal_executions": 0,
        "suboptimal_executions": 0,
        "violations": []
    }
    
    for measurement in measurements:
        f = measurement.num_faults
        optimal_rounds = f + 1
        actual_rounds = measurement.value
        
        if actual_rounds == optimal_rounds:
            verification["optimal_executions"] += 1
        else:
            verification["suboptimal_executions"] += 1
            verification["violations"].append({
                "expected": optimal_rounds,
                "actual": actual_rounds,
                "difference": actual_rounds - optimal_rounds,
                "measurement": measurement
            })
    
    verification["optimality_rate"] = verification["optimal_executions"] / len(measurements)
    
    return verification


def analyze_randomized_vs_deterministic(analyzer: ComplexityAnalyzer) -> Dict[str, Any]:
    """
    Analyze the advantage of randomized protocols over deterministic ones.
    
    From Chapter 10: Randomized protocols can achieve constant expected round complexity.
    """
    deterministic_protocols = ["dolev_strong", "byzantine_broadcast"]
    randomized_protocols = ["randomized_consensus", "streamlet"]
    
    analysis = {
        "deterministic": {},
        "randomized": {},
        "comparison": {}
    }
    
    # Analyze deterministic protocols
    det_rounds = []
    for protocol in deterministic_protocols:
        measurements = analyzer.get_measurements(ComplexityMetric.ROUND_COMPLEXITY, protocol)
        if measurements:
            rounds = [m.value for m in measurements]
            analysis["deterministic"][protocol] = {
                "avg_rounds": statistics.mean(rounds),
                "measurements": len(measurements)
            }
            det_rounds.extend(rounds)
    
    # Analyze randomized protocols  
    rand_rounds = []
    for protocol in randomized_protocols:
        measurements = analyzer.get_measurements(ComplexityMetric.ROUND_COMPLEXITY, protocol)
        if measurements:
            rounds = [m.value for m in measurements]
            analysis["randomized"][protocol] = {
                "avg_rounds": statistics.mean(rounds),
                "measurements": len(measurements)
            }
            rand_rounds.extend(rounds)
    
    # Compare
    if det_rounds and rand_rounds:
        analysis["comparison"] = {
            "deterministic_avg": statistics.mean(det_rounds),
            "randomized_avg": statistics.mean(rand_rounds),
            "improvement_factor": statistics.mean(det_rounds) / statistics.mean(rand_rounds) if rand_rounds else 0,
            "advantage": "randomized" if statistics.mean(rand_rounds) < statistics.mean(det_rounds) else "deterministic"
        }
    
    return analysis
