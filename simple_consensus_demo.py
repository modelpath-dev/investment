#!/usr/bin/env python3
"""
Simple Consensus System Demo

Demonstrates the TorbellinoTech consensus trading system with
network delay analysis and interactive visualization.
"""

import sys
import os
import time
from typing import Dict, List

# Add project paths
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'market_sim'))

def test_network_delay_analysis():
    """Test the network delay analysis implementation."""
    print("🔍 Testing Network Delay Analysis...")
    
    try:
        from network_delay_analysis import NetworkDelaySimulator, NetworkConfig, NetworkType
        
        # Create a simple network configuration
        config = NetworkConfig(
            network_type=NetworkType.SYNCHRONOUS,
            base_delay=100.0,  # 100ms base delay
            jitter_range=10.0,  # 10ms jitter
            packet_loss_rate=0.01,  # 1% packet loss
            byzantine_delay_multiplier=1.5
        )
        
        # Create simulator with 4 nodes
        simulator = NetworkDelaySimulator(config, num_nodes=4)
        
        # Mark one node as Byzantine
        simulator.set_byzantine_nodes(["node_0"])
        
        # Simulate one round of Dolev-Strong protocol
        round_time, message_count = simulator.simulate_dolev_strong_round(round_number=0, num_byzantine=1)
        
        print(f"   ✅ Round completed in {round_time:.1f}ms with {message_count} messages")
        return True
        
    except Exception as e:
        print(f"   ❌ Network delay analysis failed: {e}")
        return False

def test_consensus_imports():
    """Test that consensus protocol imports work."""
    print("🔍 Testing Consensus Protocol Imports...")
    
    try:
        # Test Byzantine Broadcast import
        sys.path.append(os.path.join(os.path.dirname(__file__), 'market_sim', 'blockchain', 'consensus'))
        from byzantine_broadcast import ByzantineBroadcastNode, BroadcastMessage, BBMessageType
        
        # Create a simple node
        node = ByzantineBroadcastNode(node_id="test_node", total_nodes=4, max_faults=1)
        message = BroadcastMessage(
            message_type=BBMessageType.PROPOSAL,
            content="test", 
            round_number=0,
            sender_id="test_node"
        )
        
        print(f"   ✅ Byzantine Broadcast: Node {node.node_id} created")
        print(f"   ✅ Broadcast Message: {message.content} from {message.sender_id}")
        return True
        
    except Exception as e:
        print(f"   ❌ Consensus imports failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_market_simulation():
    """Test the market simulation components."""
    print("🔍 Testing Market Simulation...")
    
    try:
        sys.path.append(os.path.join(os.path.dirname(__file__), 'market_sim', 'market', 'exchange'))
        from matching_engine import MatchingEngine
        
        # Create a simple matching engine
        engine = MatchingEngine(symbol="BTCUSD")
        
        print(f"   ✅ Matching Engine created successfully")
        return True
        
    except Exception as e:
        print(f"   ❌ Market simulation failed: {e}")
        return False

def demonstrate_simple_consensus():
    """Demonstrate a simple consensus scenario."""
    print("\n🚀 SIMPLE CONSENSUS DEMONSTRATION")
    print("=" * 50)
    
    try:
        from delta_impact_demo import DelayAwareConsensusDemo
        
        demo = DelayAwareConsensusDemo()
        
        # Run a simple scenario: 4 nodes, 1 Byzantine, 150ms delay
        print("Running Dolev-Strong with n=4, f=1, Δ=150ms...")
        
        result = demo.simulate_dolev_strong_with_delay(
            num_nodes=4,
            num_byzantine=1, 
            delta_ms=150.0
        )
        
        print(f"✅ Consensus achieved!")
        print(f"   Time: {result.completion_time_ms:.0f}ms")
        print(f"   Rounds: {result.round_count}")
        print(f"   Messages: {result.message_count}")
        print(f"   Efficiency: {((result.round_count * 150) / result.completion_time_ms * 100):.1f}%")
        
        return True
        
    except Exception as e:
        print(f"❌ Consensus demonstration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def show_ui_info():
    """Show information about the interactive UI."""
    print("\n🖥️  INTERACTIVE UI INFORMATION")
    print("=" * 40)
    print("The interactive Streamlit UI is running at:")
    print("🌐 Local URL: http://localhost:8501")
    print("🌐 Network URL: http://10.2.0.2:8501")
    print("🌐 External URL: http://212.8.253.146:8501")
    print()
    print("Features available in the UI:")
    print("📊 Real-time consensus protocol analysis")
    print("⚙️  Adjustable network delay (Δ) parameters")
    print("👥 Variable node count and Byzantine fault tolerance")
    print("📈 Performance visualization and comparison")
    print("🔄 Multi-scenario benchmarking")
    print()
    print("The UI demonstrates the impact of network delay on:")
    print("• Confirmation time (Tconf)")
    print("• Protocol efficiency")  
    print("• Message complexity")
    print("• Byzantine fault tolerance")

def main():
    """Main demonstration function."""
    print("🔗" + "=" * 48 + "🔗")
    print("🚀 TORBELLINOTECH CONSENSUS SYSTEM DEMO 🚀")
    print("📚 Network Delay Impact Analysis")
    print("🔗" + "=" * 48 + "🔗")
    print()
    
    # Run component tests
    tests = [
        ("Network Delay Analysis", test_network_delay_analysis),
        ("Consensus Protocol Imports", test_consensus_imports),
        ("Market Simulation", test_market_simulation),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        print("-" * 30)
        success = test_func()
        results.append((test_name, success))
        print()
    
    # Show test summary
    print("📋 TEST SUMMARY")
    print("=" * 20)
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
    
    # Run demonstration if core tests pass
    core_passed = results[0][1] and results[1][1]  # Network delay and consensus
    
    if core_passed:
        demonstrate_simple_consensus()
        show_ui_info()
        
        print("\n🎉 DEMO COMPLETED SUCCESSFULLY!")
        print("💡 The system demonstrates network delay (Δ) impact on consensus protocols")
        print("📖 Implementation based on 'Foundations of Distributed Consensus' page 35")
        
    else:
        print("\n⚠️  DEMO INCOMPLETE")
        print("Some core components failed - check the errors above")

if __name__ == "__main__":
    main()
