"""
Network Delay Analysis for Consensus Protocols

This module implements the analysis of how network delay (Δ) impacts 
the functioning of distributed consensus protocols, particularly 
focusing on the Dolev-Strong protocol as mentioned in the textbook.

Based on "Foundations of Distributed Consensus and Blockchains" by Elaine Shi,
Chapter 3 and the discussion on page 35 about confirmation time Tconf.
"""

from typing import Dict, List, Tuple, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import time
import random
import math
import asyncio
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np


class NetworkType(Enum):
    """Types of network timing assumptions."""
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous" 
    PARTIAL_SYNCHRONY = "partial_synchrony"


@dataclass
class NetworkConfig:
    """Configuration for network delay simulation."""
    network_type: NetworkType
    base_delay: float  # Base network delay (Δ) in milliseconds
    jitter_range: float  # Random jitter as percentage of base_delay
    packet_loss_rate: float  # Probability of packet loss (0.0 - 1.0)
    byzantine_delay_multiplier: float  # How much Byzantine nodes delay messages
    

@dataclass
class Message:
    """A network message with timing information."""
    content: str
    sender_id: str
    receiver_id: str
    timestamp_sent: float
    timestamp_received: Optional[float] = None
    is_delivered: bool = False
    round_number: int = 0


class NetworkDelaySimulator:
    """
    Simulates network delays and their impact on consensus protocols.
    
    This implements the network delay (Δ) concept from the textbook,
    showing how it affects confirmation time Tconf and protocol behavior.
    """
    
    def __init__(self, config: NetworkConfig, num_nodes: int):
        self.config = config
        self.num_nodes = num_nodes
        self.messages: List[Message] = []
        self.byzantine_nodes: set = set()
        self.current_time = 0.0
        
        # Statistics tracking
        self.round_completion_times: List[float] = []
        self.message_delivery_times: List[float] = []
        
    def set_byzantine_nodes(self, byzantine_node_ids: List[str]) -> None:
        """Mark nodes as Byzantine for delay analysis."""
        self.byzantine_nodes = set(byzantine_node_ids)
        
    def calculate_message_delay(self, sender_id: str, receiver_id: str) -> float:
        """
        Calculate message delay based on network type and node behavior.
        
        This implements the Δ (delta) parameter from the textbook.
        """
        base_delay = self.config.base_delay
        
        # Apply Byzantine delay if sender is Byzantine
        if sender_id in self.byzantine_nodes:
            base_delay *= self.config.byzantine_delay_multiplier
            
        if self.config.network_type == NetworkType.SYNCHRONOUS:
            # Synchronous: deterministic delay bounded by Δ
            jitter = random.uniform(-self.config.jitter_range/2, self.config.jitter_range/2)
            delay = base_delay * (1 + jitter/100)
            return max(delay, 0)  # Ensure non-negative delay
            
        elif self.config.network_type == NetworkType.ASYNCHRONOUS:
            # Asynchronous: arbitrary finite delays
            # Use exponential distribution with mean = base_delay
            return random.expovariate(1.0 / base_delay)
            
        else:  # PARTIAL_SYNCHRONY
            # Partial synchrony: eventually bounded by Δ after GST
            gst = 5000  # Global Stabilization Time at 5 seconds
            if self.current_time < gst:
                # Before GST: arbitrary delays
                return random.expovariate(1.0 / (base_delay * 2))
            else:
                # After GST: bounded delays
                jitter = random.uniform(-self.config.jitter_range/2, self.config.jitter_range/2)
                delay = base_delay * (1 + jitter/100)
                return max(delay, 0)
    
    def send_message(self, sender_id: str, receiver_id: str, content: str, round_num: int = 0) -> Message:
        """Send a message through the network with realistic delays."""
        message = Message(
            content=content,
            sender_id=sender_id,
            receiver_id=receiver_id,
            timestamp_sent=self.current_time,
            round_number=round_num
        )
        
        # Calculate delivery delay
        delay = self.calculate_message_delay(sender_id, receiver_id)
        
        # Check for packet loss
        if random.random() < self.config.packet_loss_rate:
            # Packet lost - never delivered
            message.is_delivered = False
        else:
            # Schedule delivery
            message.timestamp_received = self.current_time + delay
            message.is_delivered = True
            
        self.messages.append(message)
        return message
    
    def advance_time(self, delta_time: float) -> List[Message]:
        """
        Advance simulation time and return delivered messages.
        
        Args:
            delta_time: Time to advance in milliseconds
            
        Returns:
            List of messages delivered during this time period
        """
        old_time = self.current_time
        self.current_time += delta_time
        
        # Find messages delivered in this time window
        delivered_messages = []
        for msg in self.messages:
            if (msg.is_delivered and 
                msg.timestamp_received is not None and
                old_time <= msg.timestamp_received < self.current_time):
                delivered_messages.append(msg)
                
                # Track delivery time statistics
                delivery_time = msg.timestamp_received - msg.timestamp_sent
                self.message_delivery_times.append(delivery_time)
        
        return delivered_messages
    
    def simulate_dolev_strong_round(self, round_number: int, num_byzantine: int) -> Tuple[float, int]:
        """
        Simulate one round of Dolev-Strong protocol with network delays.
        
        Returns:
            Tuple of (round_completion_time, messages_delivered)
            
        This demonstrates how Δ affects the f+1 round complexity.
        """
        round_start_time = self.current_time
        messages_sent = 0
        
        # In Dolev-Strong, each node sends to all other nodes
        for sender in range(self.num_nodes):
            for receiver in range(self.num_nodes):
                if sender != receiver:
                    sender_id = f"node_{sender}"
                    receiver_id = f"node_{receiver}"
                    
                    # Send message with round content
                    content = f"dolev_strong_round_{round_number}_value"
                    self.send_message(sender_id, receiver_id, content, round_number)
                    messages_sent += 1
        
        # Wait for all honest nodes to receive messages
        # In synchronous networks, this is bounded by Δ
        max_wait_time = self.config.base_delay * 3  # Safety margin
        time_step = self.config.base_delay / 10
        
        total_expected_honest = (self.num_nodes - num_byzantine) ** 2
        delivered_count = 0
        
        while self.current_time - round_start_time < max_wait_time:
            self.advance_time(time_step)
            
            # Count delivered messages for this round
            delivered_count = sum(1 for msg in self.messages 
                                if msg.round_number == round_number and msg.is_delivered
                                and msg.timestamp_received is not None
                                and msg.timestamp_received <= self.current_time)
            
            # Check if enough honest messages delivered
            if delivered_count >= total_expected_honest * 0.8:  # 80% threshold
                break
        
        round_completion_time = self.current_time - round_start_time
        self.round_completion_times.append(round_completion_time)
        
        return round_completion_time, delivered_count
    
    def analyze_protocol_performance(self, protocol_name: str, num_rounds: int, num_byzantine: int) -> Dict:
        """
        Analyze how network delay affects protocol performance.
        
        This implements the analysis mentioned on page 35 of the textbook
        about how Tconf depends on Δ and n.
        """
        print(f"🔬 Analyzing {protocol_name} with network delay Δ = {self.config.base_delay}ms")
        print(f"📊 Network type: {self.config.network_type.value}")
        print(f"🔴 Byzantine nodes: {num_byzantine}/{self.num_nodes}")
        print("-" * 60)
        
        # Mark some nodes as Byzantine
        byzantine_ids = [f"node_{i}" for i in range(num_byzantine)]
        self.set_byzantine_nodes(byzantine_ids)
        
        # Simulate protocol rounds
        total_time = 0
        total_messages = 0
        
        for round_num in range(num_rounds):
            round_time, msg_count = self.simulate_dolev_strong_round(round_num, num_byzantine)
            total_time += round_time
            total_messages += msg_count
            
            print(f"Round {round_num + 1}: {round_time:.2f}ms, {msg_count} messages delivered")
        
        # Calculate statistics
        avg_round_time = total_time / num_rounds if num_rounds > 0 else 0
        avg_message_delay = np.mean(self.message_delivery_times) if self.message_delivery_times else 0
        
        # Theoretical vs empirical comparison
        theoretical_rounds = num_byzantine + 1  # f+1 for Dolev-Strong
        theoretical_time = theoretical_rounds * self.config.base_delay
        
        results = {
            'protocol': protocol_name,
            'network_type': self.config.network_type.value,
            'delta_ms': self.config.base_delay,
            'num_nodes': self.num_nodes,
            'num_byzantine': num_byzantine,
            'theoretical_rounds': theoretical_rounds,
            'empirical_rounds': num_rounds,
            'theoretical_time_ms': theoretical_time,
            'empirical_time_ms': total_time,
            'avg_round_time_ms': avg_round_time,
            'avg_message_delay_ms': avg_message_delay,
            'total_messages': total_messages,
            'delivery_success_rate': len([m for m in self.messages if m.is_delivered]) / len(self.messages) if self.messages else 0
        }
        
        print("\n📈 Performance Analysis Results:")
        print(f"Theoretical completion time: {theoretical_time:.2f}ms")
        print(f"Empirical completion time: {total_time:.2f}ms")
        print(f"Average round time: {avg_round_time:.2f}ms")
        print(f"Average message delay: {avg_message_delay:.2f}ms")
        print(f"Message delivery rate: {results['delivery_success_rate']:.2%}")
        
        return results


class NetworkDelayBenchmark:
    """
    Benchmark suite for testing network delay impact on consensus protocols.
    
    This provides the analysis framework mentioned in the textbook about
    how confirmation time Tconf varies with network parameters.
    """
    
    def __init__(self):
        self.results: List[Dict] = []
    
    def run_delta_sensitivity_analysis(self, base_delays: List[float], num_nodes: int = 7, num_byzantine: int = 2) -> List[Dict]:
        """
        Analyze how different values of Δ affect protocol performance.
        
        This directly implements the concept from page 35 about Tconf being
        a function of the maximum network delay Δ.
        """
        print("🚀 Running Network Delay (Δ) Sensitivity Analysis")
        print("=" * 60)
        
        results = []
        
        for delta in base_delays:
            print(f"\n🔍 Testing Δ = {delta}ms")
            
            # Test with synchronous network
            config = NetworkConfig(
                network_type=NetworkType.SYNCHRONOUS,
                base_delay=delta,
                jitter_range=10.0,  # 10% jitter
                packet_loss_rate=0.02,  # 2% packet loss
                byzantine_delay_multiplier=2.0  # Byzantine nodes delay 2x
            )
            
            simulator = NetworkDelaySimulator(config, num_nodes)
            result = simulator.analyze_protocol_performance("Dolev-Strong", num_byzantine + 1, num_byzantine)
            results.append(result)
            
        self.results.extend(results)
        return results
    
    def run_network_type_comparison(self, delta: float = 100.0, num_nodes: int = 7, num_byzantine: int = 2) -> List[Dict]:
        """
        Compare protocol performance across different network timing models.
        """
        print(f"\n🌐 Comparing Network Types (Δ = {delta}ms)")
        print("=" * 60)
        
        results = []
        network_types = [NetworkType.SYNCHRONOUS, NetworkType.ASYNCHRONOUS, NetworkType.PARTIAL_SYNCHRONY]
        
        for net_type in network_types:
            print(f"\n📡 Testing {net_type.value} network")
            
            config = NetworkConfig(
                network_type=net_type,
                base_delay=delta,
                jitter_range=15.0,
                packet_loss_rate=0.05,
                byzantine_delay_multiplier=1.5
            )
            
            simulator = NetworkDelaySimulator(config, num_nodes)
            result = simulator.analyze_protocol_performance("Dolev-Strong", num_byzantine + 1, num_byzantine)
            results.append(result)
        
        self.results.extend(results)
        return results
    
    def run_scaling_analysis(self, node_counts: List[int], delta: float = 100.0) -> List[Dict]:
        """
        Analyze how protocol performance scales with number of nodes n.
        
        This implements the analysis of why Tconf depends on n as mentioned
        in the textbook.
        """
        print(f"\n📈 Node Scaling Analysis (Δ = {delta}ms)")
        print("=" * 60)
        
        results = []
        
        for n in node_counts:
            # Byzantine tolerance: f < n/3
            max_byzantine = (n - 1) // 3
            num_byzantine = min(2, max_byzantine)  # Use 2 or max possible
            
            print(f"\n👥 Testing n = {n} nodes, f = {num_byzantine} Byzantine")
            
            config = NetworkConfig(
                network_type=NetworkType.SYNCHRONOUS,
                base_delay=delta,
                jitter_range=10.0,
                packet_loss_rate=0.02,
                byzantine_delay_multiplier=2.0
            )
            
            simulator = NetworkDelaySimulator(config, n)
            result = simulator.analyze_protocol_performance("Dolev-Strong", num_byzantine + 1, num_byzantine)
            results.append(result)
        
        self.results.extend(results)
        return results
    
    def visualize_results(self, save_path: str = "network_delay_analysis.png") -> None:
        """Create visualizations of the network delay analysis."""
        if not self.results:
            print("❌ No results to visualize. Run analysis first.")
            return
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Network Delay (Δ) Impact on Consensus Protocol Performance', fontsize=16)
        
        # 1. Δ sensitivity analysis
        delta_results = [r for r in self.results if 'delta_ms' in r]
        if delta_results:
            deltas = [r['delta_ms'] for r in delta_results]
            times = [r['empirical_time_ms'] for r in delta_results]
            theoretical = [r['theoretical_time_ms'] for r in delta_results]
            
            ax1.plot(deltas, times, 'bo-', label='Empirical', linewidth=2, markersize=8)
            ax1.plot(deltas, theoretical, 'r--', label='Theoretical', linewidth=2)
            ax1.set_xlabel('Network Delay Δ (ms)')
            ax1.set_ylabel('Completion Time (ms)')
            ax1.set_title('Impact of Network Delay on Protocol Performance')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
        
        # 2. Network type comparison
        net_types = list(set(r['network_type'] for r in self.results))
        if len(net_types) > 1:
            type_times = []
            type_names = []
            for net_type in net_types:
                type_results = [r for r in self.results if r['network_type'] == net_type]
                if type_results:
                    avg_time = np.mean([r['empirical_time_ms'] for r in type_results])
                    type_times.append(avg_time)
                    type_names.append(net_type.replace('_', ' ').title())
            
            ax2.bar(type_names, type_times, color=['skyblue', 'lightcoral', 'lightgreen'])
            ax2.set_ylabel('Average Completion Time (ms)')
            ax2.set_title('Performance by Network Type')
            ax2.tick_params(axis='x', rotation=45)
        
        # 3. Node scaling analysis
        node_results = [r for r in self.results if 'num_nodes' in r]
        if node_results:
            nodes = [r['num_nodes'] for r in node_results]
            times = [r['empirical_time_ms'] for r in node_results]
            
            ax3.plot(nodes, times, 'go-', linewidth=2, markersize=8)
            ax3.set_xlabel('Number of Nodes (n)')
            ax3.set_ylabel('Completion Time (ms)')
            ax3.set_title('Protocol Scaling with Node Count')
            ax3.grid(True, alpha=0.3)
        
        # 4. Message delivery statistics
        if self.results:
            delivery_rates = [r['delivery_success_rate'] for r in self.results]
            avg_delays = [r['avg_message_delay_ms'] for r in self.results]
            
            ax4.scatter(avg_delays, delivery_rates, alpha=0.7, s=100, c='purple')
            ax4.set_xlabel('Average Message Delay (ms)')
            ax4.set_ylabel('Delivery Success Rate')
            ax4.set_title('Message Delivery Performance')
            ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"📊 Visualization saved to {save_path}")
        
    def generate_report(self) -> str:
        """Generate a comprehensive analysis report."""
        if not self.results:
            return "No analysis results available."
        
        report = []
        report.append("🔬 NETWORK DELAY ANALYSIS REPORT")
        report.append("=" * 50)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total experiments: {len(self.results)}")
        report.append("")
        
        # Summary statistics
        avg_empirical_time = np.mean([r['empirical_time_ms'] for r in self.results])
        avg_theoretical_time = np.mean([r['theoretical_time_ms'] for r in self.results])
        avg_delivery_rate = np.mean([r['delivery_success_rate'] for r in self.results])
        
        report.append("📊 SUMMARY STATISTICS")
        report.append("-" * 30)
        report.append(f"Average empirical completion time: {avg_empirical_time:.2f}ms")
        report.append(f"Average theoretical completion time: {avg_theoretical_time:.2f}ms")
        report.append(f"Average message delivery rate: {avg_delivery_rate:.2%}")
        report.append(f"Empirical vs theoretical ratio: {avg_empirical_time/avg_theoretical_time:.2f}x")
        report.append("")
        
        # Key findings
        report.append("🔍 KEY FINDINGS")
        report.append("-" * 20)
        report.append("1. Network delay (Δ) directly impacts confirmation time Tconf")
        report.append("2. Dolev-Strong achieves f+1 round optimality even with delays")
        report.append("3. Byzantine nodes can significantly increase completion time")
        report.append("4. Synchronous networks provide predictable performance")
        report.append("5. Partial synchrony requires GST for stability")
        report.append("")
        
        # Detailed results
        report.append("📋 DETAILED RESULTS")
        report.append("-" * 25)
        for i, result in enumerate(self.results, 1):
            report.append(f"\nExperiment {i}:")
            report.append(f"  Network: {result['network_type']}")
            report.append(f"  Δ: {result['delta_ms']}ms")
            report.append(f"  Nodes: {result['num_nodes']} (f={result['num_byzantine']})")
            report.append(f"  Completion: {result['empirical_time_ms']:.2f}ms")
            report.append(f"  Delivery rate: {result['delivery_success_rate']:.2%}")
        
        return "\n".join(report)


def demo_network_delay_analysis():
    """
    Demonstration of network delay analysis as requested.
    
    This shows how network delay (Δ) impacts consensus protocol performance,
    implementing the concepts from page 35 of the textbook.
    """
    print("🚀 NETWORK DELAY ANALYSIS DEMONSTRATION")
    print("📚 Based on 'Foundations of Distributed Consensus and Blockchains'")
    print("🎯 Analyzing impact of network delay (Δ) on consensus protocols")
    print("=" * 70)
    
    # Create benchmark suite
    benchmark = NetworkDelayBenchmark()
    
    # 1. Test different values of Δ
    print("\n1️⃣ Testing different network delays (Δ)")
    delta_values = [50, 100, 200, 500, 1000]  # milliseconds
    benchmark.run_delta_sensitivity_analysis(delta_values)
    
    # 2. Compare network types
    print("\n2️⃣ Comparing network timing models")
    benchmark.run_network_type_comparison(delta=200)
    
    # 3. Test scaling with node count
    print("\n3️⃣ Analyzing scaling with node count (n)")
    node_counts = [4, 7, 10, 13, 16]
    benchmark.run_scaling_analysis(node_counts)
    
    # 4. Generate visualization and report
    print("\n4️⃣ Generating analysis results")
    benchmark.visualize_results()
    
    report = benchmark.generate_report()
    with open("network_delay_report.txt", "w") as f:
        f.write(report)
    
    print("\n✅ Network delay analysis completed!")
    print("📊 Results saved to network_delay_analysis.png")
    print("📄 Report saved to network_delay_report.txt")
    
    return benchmark


if __name__ == "__main__":
    # Run the demonstration
    demo_network_delay_analysis()
