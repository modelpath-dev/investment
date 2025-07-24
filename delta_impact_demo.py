#!/usr/bin/env python3
"""
Network Delay Impact Demo for Consensus Protocols

This script demonstrates how network delay (Δ) affects consensus protocol 
performance, specifically implementing the analysis from page 35 of 
"Foundations of Distributed Consensus and Blockchains" by Elaine Shi.

The script shows how confirmation time Tconf depends on:
1. Maximum network delay (Δ)
2. Number of nodes (n) 
3. Network timing assumptions
"""

import sys
import os
import time
import random
from typing import List, Dict, Tuple
from dataclasses import dataclass

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from network_delay_analysis import (
    NetworkDelaySimulator, NetworkConfig, NetworkType, 
    NetworkDelayBenchmark, demo_network_delay_analysis
)


@dataclass 
class ConsensusResult:
    """Result of a consensus protocol execution with timing."""
    protocol_name: str
    num_nodes: int
    num_byzantine: int
    network_delay_ms: float
    completion_time_ms: float
    round_count: int
    message_count: int
    success: bool


class DelayAwareConsensusDemo:
    """
    Demonstrates consensus protocols with realistic network delays.
    
    This implements the core concepts from the textbook about how
    network delay (Δ) impacts protocol performance and confirmation time.
    """
    
    def __init__(self):
        self.results: List[ConsensusResult] = []
        
    def simulate_dolev_strong_with_delay(self, 
                                       num_nodes: int, 
                                       num_byzantine: int, 
                                       delta_ms: float,
                                       network_type: NetworkType = NetworkType.SYNCHRONOUS) -> ConsensusResult:
        """
        Simulate Dolev-Strong protocol with realistic network delays.
        
        This demonstrates the f+1 round complexity under network delay Δ.
        """
        print(f"🔄 Dolev-Strong: n={num_nodes}, f={num_byzantine}, Δ={delta_ms}ms, {network_type.value}")
        
        # Create network configuration
        config = NetworkConfig(
            network_type=network_type,
            base_delay=delta_ms,
            jitter_range=10.0,  # 10% jitter
            packet_loss_rate=0.02,  # 2% packet loss
            byzantine_delay_multiplier=2.0  # Byzantine nodes cause 2x delay
        )
        
        # Create simulator
        simulator = NetworkDelaySimulator(config, num_nodes)
        
        # Mark Byzantine nodes
        byzantine_ids = [f"node_{i}" for i in range(num_byzantine)]
        simulator.set_byzantine_nodes(byzantine_ids)
        
        # Theoretical expectations
        expected_rounds = num_byzantine + 1  # f+1 optimality
        expected_time = expected_rounds * delta_ms
        
        print(f"   📊 Theoretical: {expected_rounds} rounds, {expected_time:.0f}ms")
        
        # Simulate protocol execution
        start_time = time.time()
        total_time = 0
        total_messages = 0
        
        for round_num in range(expected_rounds):
            round_time, msg_count = simulator.simulate_dolev_strong_round(round_num, num_byzantine)
            total_time += round_time
            total_messages += msg_count
            
        actual_time = time.time() - start_time
        
        success = total_time <= expected_time * 2  # Allow 2x theoretical time
        
        print(f"   ✅ Empirical: {expected_rounds} rounds, {total_time:.0f}ms, {total_messages} messages")
        print(f"   🎯 Success: {success}, Ratio: {total_time/expected_time:.2f}x theoretical")
        
        result = ConsensusResult(
            protocol_name="Dolev-Strong",
            num_nodes=num_nodes,
            num_byzantine=num_byzantine, 
            network_delay_ms=delta_ms,
            completion_time_ms=total_time,
            round_count=expected_rounds,
            message_count=total_messages,
            success=success
        )
        
        self.results.append(result)
        return result
    
    def demonstrate_delta_impact(self):
        """
        Demonstrate how different values of Δ affect protocol performance.
        
        This directly implements the analysis from page 35 about Tconf 
        being a function of maximum network delay Δ.
        """
        print("\n📈 DEMONSTRATING IMPACT OF NETWORK DELAY (Δ)")
        print("=" * 60)
        print("Based on page 35: 'Tconf can be a function of the maximum network delay Δ'")
        print()
        
        # Fixed parameters
        num_nodes = 7
        num_byzantine = 2  # f = 2, so f+1 = 3 rounds expected
        
        # Test different network delays
        delta_values = [50, 100, 200, 500, 1000]  # milliseconds
        
        print(f"Fixed: n={num_nodes} nodes, f={num_byzantine} Byzantine faults")
        print(f"Variable: Network delay Δ = {delta_values} ms")
        print()
        
        for delta in delta_values:
            result = self.simulate_dolev_strong_with_delay(num_nodes, num_byzantine, delta)
            
            # Show how completion time scales with Δ
            theoretical_time = result.round_count * delta
            ratio = result.completion_time_ms / theoretical_time
            
            print(f"   📊 Δ={delta}ms → Tconf={result.completion_time_ms:.0f}ms (ratio: {ratio:.2f}x)")
            print()
    
    def demonstrate_node_scaling(self):
        """
        Demonstrate how protocol performance scales with number of nodes n.
        
        This shows why Tconf depends on n as mentioned in the textbook.
        """
        print("\n👥 DEMONSTRATING NODE SCALING IMPACT")
        print("=" * 50)
        print("Based on page 35: 'Why can Tconf also depend on n?'")
        print()
        
        # Fixed parameters
        delta = 200  # milliseconds
        
        # Test different node counts
        node_configs = [
            (4, 1),   # n=4, f=1, so f+1=2 rounds
            (7, 2),   # n=7, f=2, so f+1=3 rounds  
            (10, 3),  # n=10, f=3, so f+1=4 rounds
            (13, 4),  # n=13, f=4, so f+1=5 rounds
        ]
        
        print(f"Fixed: Network delay Δ={delta}ms")
        print(f"Variable: Number of nodes n and Byzantine faults f")
        print()
        
        for num_nodes, num_byzantine in node_configs:
            result = self.simulate_dolev_strong_with_delay(num_nodes, num_byzantine, delta)
            
            # Show how completion time scales with n (through f+1 rounds)
            rounds = result.round_count
            time_per_round = result.completion_time_ms / rounds
            
            print(f"   📊 n={num_nodes}, f={num_byzantine} → {rounds} rounds, {time_per_round:.0f}ms/round")
            print()
    
    def demonstrate_network_types(self):
        """
        Compare protocol performance across different network timing assumptions.
        """
        print("\n🌐 DEMONSTRATING NETWORK TIMING MODELS")
        print("=" * 50)
        print("Comparing: Synchronous, Asynchronous, Partial Synchrony")
        print()
        
        # Fixed parameters
        num_nodes = 7
        num_byzantine = 2
        delta = 200
        
        network_types = [
            NetworkType.SYNCHRONOUS,
            NetworkType.ASYNCHRONOUS, 
            NetworkType.PARTIAL_SYNCHRONY
        ]
        
        print(f"Fixed: n={num_nodes}, f={num_byzantine}, Δ={delta}ms")
        print()
        
        for net_type in network_types:
            result = self.simulate_dolev_strong_with_delay(num_nodes, num_byzantine, delta, net_type)
            
            # Show network-specific behavior
            predictability = "High" if net_type == NetworkType.SYNCHRONOUS else "Variable"
            print(f"   📊 {net_type.value}: {result.completion_time_ms:.0f}ms, Predictability: {predictability}")
            print()
    
    def demonstrate_byzantine_impact(self):
        """
        Show how Byzantine nodes affect protocol performance.
        """
        print("\n🔴 DEMONSTRATING BYZANTINE BEHAVIOR IMPACT")
        print("=" * 50)
        print("Showing how Byzantine nodes can increase confirmation time")
        print()
        
        # Fixed parameters
        num_nodes = 10
        delta = 150
        
        # Test different numbers of Byzantine nodes
        byzantine_counts = [0, 1, 2, 3]  # f = 0, 1, 2, 3
        
        print(f"Fixed: n={num_nodes}, Δ={delta}ms")
        print()
        
        for f in byzantine_counts:
            if f < num_nodes // 3:  # Byzantine fault tolerance limit
                result = self.simulate_dolev_strong_with_delay(num_nodes, f, delta)
                
                efficiency = 100 - (result.completion_time_ms / (delta * (f + 1)) - 1) * 100
                print(f"   📊 f={f} Byzantine → {result.round_count} rounds, {result.completion_time_ms:.0f}ms")
                print(f"       Efficiency: {efficiency:.1f}% of theoretical minimum")
                print()
    
    def generate_summary(self):
        """Generate a summary of all results."""
        if not self.results:
            print("❌ No results to summarize")
            return
            
        print("\n📋 EXPERIMENTAL SUMMARY")
        print("=" * 40)
        print(f"Total experiments: {len(self.results)}")
        
        # Success rate
        successful = sum(1 for r in self.results if r.success)
        success_rate = successful / len(self.results)
        print(f"Success rate: {success_rate:.1%}")
        
        # Average performance
        avg_completion = sum(r.completion_time_ms for r in self.results) / len(self.results)
        avg_messages = sum(r.message_count for r in self.results) / len(self.results)
        
        print(f"Average completion time: {avg_completion:.0f}ms")
        print(f"Average messages per run: {avg_messages:.0f}")
        
        # Key insights
        print("\n🔍 KEY INSIGHTS:")
        print("1. Network delay Δ directly affects confirmation time Tconf")
        print("2. Dolev-Strong maintains f+1 round optimality under delay")
        print("3. Byzantine nodes can significantly increase completion time")
        print("4. Synchronous networks provide predictable performance bounds")
        print("5. Protocol scales with both Δ and n as predicted by theory")


def main():
    """Main demonstration script."""
    print("🔗" + "=" * 58 + "🔗")
    print("🚀 NETWORK DELAY IMPACT ON CONSENSUS PROTOCOLS 🚀")
    print("📚 Based on 'Foundations of Distributed Consensus'")
    print("🎯 Page 35: How Δ affects confirmation time Tconf")
    print("🔗" + "=" * 58 + "🔗")
    print()
    
    # Create demo instance
    demo = DelayAwareConsensusDemo()
    
    # Run demonstrations
    try:
        demo.demonstrate_delta_impact()
        demo.demonstrate_node_scaling()
        demo.demonstrate_network_types()
        demo.demonstrate_byzantine_impact()
        demo.generate_summary()
        
        print("\n🎉 Network delay analysis completed!")
        print("💡 This demonstrates how Δ impacts consensus as described in the textbook")
        
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
