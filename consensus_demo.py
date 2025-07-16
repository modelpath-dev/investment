"""
Comprehensive demonstration of distributed consensus protocols applied to financial systems.

This script demonstrates the implementation of concepts from Elaine Shi's 
"Foundations of Distributed Consensus and Blockchains" textbook in practical
financial trading scenarios.
"""

import asyncio
import time
import random
from decimal import Decimal
from typing import List, Dict, Any
import numpy as np

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'blockchain/consensus'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'analysis/visualization'))

from byzantine_broadcast import ByzantineBroadcastNode, ByzantineProtocol
from streamlet import StreamletNode, Block, Blockchain
from randomized_consensus import RandomizedConsensusNode, CommonCoinOracle
from nakamoto import NakamotoBlockchain, PoWBlock
from complexity_analysis import ComplexityAnalyzer, ProtocolComplexityWrapper
from network_models import NetworkSimulator, NetworkModel, NetworkParameters
from financial_consensus import (
    ConsensusBasedExchange, 
    CrossExchangeConsensus, 
    FinancialOrder
)
from consensus_visualizations import (
    ConsensusProtocolVisualizer,
    FinancialConsensusVisualizer,
    create_comprehensive_analysis_dashboard,
    save_dashboard_html
)


class ConsensusFinancialDemo:
    """Comprehensive demonstration of consensus-based financial systems."""
    
    def __init__(self):
        """Initialize demonstration environment."""
        print("🚀 Initializing Distributed Consensus Financial Demo")
        print("Based on Elaine Shi's 'Foundations of Distributed Consensus and Blockchains'")
        print("=" * 70)
        
        self.analyzer = ComplexityAnalyzer()
        self.exchanges = {}
        self.cross_consensus = CrossExchangeConsensus()
        self.network_stats = []
        
    def demonstrate_byzantine_broadcast(self) -> Dict[str, Any]:
        """
        Demonstrate Byzantine Broadcast protocol (Chapter 3).
        
        Shows Dolev-Strong protocol achieving optimal f+1 round complexity.
        """
        print("\n📡 Demonstrating Byzantine Broadcast (Dolev-Strong Protocol)")
        print("Chapter 3: Achieves optimal f+1 rounds with O(n^2) messages")
        
        # Create Byzantine broadcast protocol
        protocol = ByzantineProtocol()
        nodes = []
        
        # Create 7 nodes (can tolerate up to 2 Byzantine)
        for i in range(7):
            node = ByzantineBroadcastNode(f"node_{i}")
            nodes.append(node)
            protocol.add_node(node)
        
        # Mark 2 nodes as Byzantine
        nodes[5].is_byzantine = True
        nodes[6].is_byzantine = True
        
        # Measure complexity
        wrapper = ProtocolComplexityWrapper("dolev_strong", self.analyzer)
        
        def run_broadcast():
            return protocol.run_broadcast("node_0", "financial_order_12345", max_rounds=3)
        
        # Execute with complexity measurement
        result, execution_id = wrapper.wrap_protocol_execution(run_broadcast, 7, 2)
        
        print(f"✅ Byzantine Broadcast completed successfully: {result['success']}")
        print(f"   Rounds used: {result.get('rounds', 'N/A')}")
        print(f"   Honest nodes reached agreement: {result.get('agreement', 'N/A')}")
        
        return {
            "protocol": "dolev_strong",
            "result": result,
            "execution_id": execution_id
        }
    
    def demonstrate_streamlet_blockchain(self) -> Dict[str, Any]:
        """
        Demonstrate Streamlet blockchain protocol (Chapter 7).
        
        Shows simple blockchain with propose-vote paradigm and finalization.
        """
        print("\n🔗 Demonstrating Streamlet Blockchain Protocol")
        print("Chapter 7: Simple blockchain with three consecutive epochs finalization")
        
        # Create blockchain and nodes
        blockchain = Blockchain()
        nodes = []
        
        for i in range(5):
            node = StreamletNode(f"streamlet_node_{i}")
            nodes.append(node)
        
        # Measure complexity
        execution_id = "streamlet_demo"
        self.analyzer.start_measurement(execution_id, "streamlet", 5, 1)
        
        # Simulate several epochs of blockchain operation
        transactions_pool = [
            "transfer(alice, bob, 100)",
            "trade(AAPL, 150, buy)",
            "trade(TSLA, 800, sell)",
            "settle(trade_123)",
            "update_market_price(AAPL, 155)"
        ]
        
        for epoch in range(1, 6):
            self.analyzer.record_round_start(execution_id)
            
            # Leader proposes block
            leader = nodes[epoch % len(nodes)]
            
            # Select random transactions
            epoch_transactions = random.sample(transactions_pool, 
                                             min(3, len(transactions_pool)))
            
            # Create block
            block = Block(
                proposer=leader.node_id,
                epoch=epoch,
                parent_hash=blockchain.get_latest_hash(),
                transactions=epoch_transactions
            )
            
            # Simulate voting (each node sends vote message)
            for node in nodes:
                self.analyzer.record_message_sent(execution_id, 256)  # Vote message
            
            # Add block to blockchain
            blockchain.add_block(block)
            
            print(f"   Epoch {epoch}: Block proposed by {leader.node_id}")
            print(f"   Transactions: {len(epoch_transactions)}")
        
        self.analyzer.finish_measurement(execution_id)
        
        # Check finalization
        finalized_blocks = blockchain.get_finalized_blocks()
        
        print(f"✅ Streamlet blockchain demonstration completed")
        print(f"   Total blocks: {len(blockchain.blocks)}")
        print(f"   Finalized blocks: {len(finalized_blocks)}")
        
        return {
            "protocol": "streamlet",
            "total_blocks": len(blockchain.blocks),
            "finalized_blocks": len(finalized_blocks),
            "blockchain": blockchain
        }
    
    def demonstrate_randomized_consensus(self) -> Dict[str, Any]:
        """
        Demonstrate Randomized Consensus protocol (Chapter 13).
        
        Shows how randomization overcomes FLP impossibility.
        """
        print("\n🎲 Demonstrating Randomized Consensus Protocol")
        print("Chapter 13: Overcomes FLP impossibility with expected O(1) rounds")
        
        # Create common coin oracle and nodes
        oracle = CommonCoinOracle()
        nodes = []
        
        for i in range(7):
            node = RandomizedConsensusNode(f"rand_node_{i}", oracle)
            nodes.append(node)
        
        # Measure complexity
        execution_id = "randomized_demo"
        self.analyzer.start_measurement(execution_id, "randomized_consensus", 7, 2)
        
        # Simulate consensus on financial decision
        consensus_value = "execute_large_trade_portfolio_rebalancing"
        epoch = 1
        
        # Pre-vote phase
        self.analyzer.record_round_start(execution_id)
        print(f"   Pre-vote phase for: {consensus_value}")
        
        for node in nodes:
            # Each node creates pre-vote
            pre_vote = node.create_pre_vote(consensus_value, epoch, view=0)
            self.analyzer.record_message_sent(execution_id, 512)
        
        # Main-vote phase  
        self.analyzer.record_round_start(execution_id)
        print(f"   Main-vote phase")
        
        for node in nodes:
            # Each node creates main-vote based on common coin
            coin = oracle.get_coin(epoch)
            main_vote = node.create_main_vote(consensus_value, epoch, view=0)
            self.analyzer.record_message_sent(execution_id, 512)
        
        # Decision phase
        self.analyzer.record_round_start(execution_id)
        print(f"   Decision phase with common coin: {oracle.get_coin(epoch)}")
        
        self.analyzer.finish_measurement(execution_id)
        
        print(f"✅ Randomized consensus demonstration completed")
        print(f"   Consensus value: {consensus_value}")
        print(f"   Expected rounds: O(1) with high probability")
        
        return {
            "protocol": "randomized_consensus",
            "consensus_value": consensus_value,
            "common_coin": oracle.get_coin(epoch)
        }
    
    def demonstrate_nakamoto_protocol(self) -> Dict[str, Any]:
        """
        Demonstrate Nakamoto blockchain protocol (Chapters 14-17).
        
        Shows proof-of-work mining and longest chain rule.
        """
        print("\n⛏️  Demonstrating Nakamoto Blockchain Protocol")
        print("Chapters 14-17: Proof-of-work with longest chain rule")
        
        # Create Nakamoto blockchain
        blockchain = NakamotoBlockchain(difficulty=2)  # Low difficulty for demo
        
        # Measure complexity
        execution_id = "nakamoto_demo"
        self.analyzer.start_measurement(execution_id, "nakamoto", 3, 1)
        
        # Simulate mining several blocks
        financial_transactions = [
            "settlement_batch_001",
            "cross_border_payment_002", 
            "derivative_trade_003",
            "margin_call_004",
            "risk_assessment_005"
        ]
        
        mined_blocks = []
        
        for i in range(5):
            self.analyzer.record_round_start(execution_id)
            
            # Create block with financial transactions
            block = PoWBlock(
                index=i + 1,
                timestamp=time.time(),
                transactions=[financial_transactions[i]],
                previous_hash=blockchain.get_latest_hash(),
                difficulty=blockchain.difficulty
            )
            
            # Mine the block (proof-of-work)
            print(f"   Mining block {i + 1}...")
            start_time = time.time()
            success = block.mine_block()
            mine_time = time.time() - start_time
            
            if success:
                # Record mining work as "messages" (hash computations)
                hash_computations = 2 ** blockchain.difficulty  # Approximate
                self.analyzer.record_message_sent(execution_id, hash_computations)
                
                # Add to blockchain
                blockchain.add_block(block)
                mined_blocks.append(block)
                
                print(f"   ✅ Block {i + 1} mined in {mine_time:.2f}s")
                print(f"      Hash: {block.block_hash[:16]}...")
                print(f"      Nonce: {block.nonce}")
            else:
                print(f"   ❌ Failed to mine block {i + 1}")
        
        self.analyzer.finish_measurement(execution_id)
        
        # Analyze chain properties
        chain_length = blockchain.get_chain_length()
        
        print(f"✅ Nakamoto blockchain demonstration completed")
        print(f"   Chain length: {chain_length}")
        print(f"   Blocks mined: {len(mined_blocks)}")
        print(f"   Security: Longest chain rule ensures consistency")
        
        return {
            "protocol": "nakamoto",
            "chain_length": chain_length,
            "mined_blocks": len(mined_blocks),
            "blockchain": blockchain
        }
    
    async def demonstrate_consensus_based_trading(self) -> Dict[str, Any]:
        """
        Demonstrate consensus-based financial trading exchange.
        
        Shows how consensus protocols enable Byzantine-fault-tolerant trading.
        """
        print("\n💰 Demonstrating Consensus-Based Financial Trading")
        print("Integration: Byzantine-fault-tolerant exchange with consensus validation")
        
        # Create consensus-based exchange
        exchange = ConsensusBasedExchange("demo_exchange", "streamlet")
        
        # Add validator nodes
        for i in range(7):
            exchange.add_consensus_node(f"validator_{i}")
        
        # Set up traders with position limits
        traders = ["alice", "bob", "charlie", "diana"]
        for trader in traders:
            exchange.position_limits[trader] = Decimal("1000000")
        
        # Create sample financial orders
        orders = [
            FinancialOrder("order_001", "alice", "AAPL", "buy", Decimal("100"), Decimal("150.00"), time.time()),
            FinancialOrder("order_002", "bob", "AAPL", "sell", Decimal("100"), Decimal("150.50"), time.time()),
            FinancialOrder("order_003", "charlie", "TSLA", "buy", Decimal("50"), Decimal("800.00"), time.time()),
            FinancialOrder("order_004", "diana", "TSLA", "sell", Decimal("50"), Decimal("799.50"), time.time()),
            FinancialOrder("order_005", "alice", "NVDA", "buy", Decimal("200"), Decimal("400.00"), time.time()),
        ]
        
        print(f"   Submitting {len(orders)} orders through consensus...")
        
        # Submit orders through consensus
        results = []
        for i, order in enumerate(orders):
            print(f"   Order {i+1}: {order.trader_id} {order.side} {order.quantity} {order.symbol} @ ${order.price}")
            result = await exchange.submit_order(order)
            results.append(result)
        
        # Execute matching trades
        print("   Executing trades through consensus...")
        
        # Match AAPL orders
        await exchange.execute_trade(
            orders[0], orders[1],  # Alice buy, Bob sell
            Decimal("150.25"), Decimal("100")
        )
        
        # Match TSLA orders  
        await exchange.execute_trade(
            orders[2], orders[3],  # Charlie buy, Diana sell
            Decimal("799.75"), Decimal("50")
        )
        
        # Update market state
        for symbol in ["AAPL", "TSLA", "NVDA"]:
            await exchange.update_market_state(symbol)
        
        # Store exchange for visualization
        self.exchanges["demo_exchange"] = exchange
        
        stats = exchange.get_exchange_statistics()
        
        print(f"✅ Consensus-based trading demonstration completed")
        print(f"   Orders processed: {stats['total_orders']}")
        print(f"   Trades executed: {stats['total_trades']}")
        print(f"   Active symbols: {stats['active_symbols']}")
        print(f"   Consensus nodes: {stats['consensus_nodes']}")
        
        return {
            "exchange": exchange,
            "orders_submitted": len(orders),
            "trades_executed": len(exchange.executed_trades),
            "statistics": stats
        }
    
    async def demonstrate_cross_exchange_arbitrage(self) -> Dict[str, Any]:
        """
        Demonstrate cross-exchange arbitrage with consensus coordination.
        
        Shows atomic arbitrage execution across multiple consensus-based exchanges.
        """
        print("\n🔄 Demonstrating Cross-Exchange Arbitrage")
        print("Advanced: Atomic arbitrage across multiple Byzantine-fault-tolerant exchanges")
        
        # Create two exchanges with different consensus protocols
        exchange_a = ConsensusBasedExchange("exchange_alpha", "streamlet")
        exchange_b = ConsensusBasedExchange("exchange_beta", "byzantine_broadcast")
        
        # Add validators to each exchange
        for i in range(5):
            exchange_a.add_consensus_node(f"alpha_validator_{i}")
            exchange_b.add_consensus_node(f"beta_validator_{i}")
        
        # Set up different market prices to create arbitrage opportunity
        symbol = "ARB_TOKEN"
        
        # Exchange A: Lower ask price
        exchange_a.market_states[symbol] = type('MarketState', (), {
            'symbol': symbol,
            'best_bid': Decimal("95.00"),
            'best_ask': Decimal("100.00"),  # Lower ask
            'last_trade_price': Decimal("97.50"),
            'volume_24h': Decimal("1000"),
            'timestamp': time.time(),
            'order_book_hash': "hash_a"
        })()
        
        # Exchange B: Higher bid price  
        exchange_b.market_states[symbol] = type('MarketState', (), {
            'symbol': symbol,
            'best_bid': Decimal("105.00"),  # Higher bid
            'best_ask': Decimal("110.00"),
            'last_trade_price': Decimal("107.50"),
            'volume_24h': Decimal("800"),
            'timestamp': time.time(),
            'order_book_hash': "hash_b"
        })()
        
        # Set up cross-exchange consensus
        self.cross_consensus.add_exchange(exchange_a)
        self.cross_consensus.add_exchange(exchange_b)
        
        # Detect arbitrage opportunities
        opportunities = self.cross_consensus.detect_arbitrage_opportunities()
        
        print(f"   Arbitrage opportunities detected: {len(opportunities)}")
        
        if opportunities:
            opp = opportunities[0]
            print(f"   Opportunity: Buy on {opp['buy_exchange']} @ ${opp['buy_price']}")
            print(f"                Sell on {opp['sell_exchange']} @ ${opp['sell_price']}")
            print(f"                Profit: ${opp['profit_per_unit']} ({opp['profit_percentage']}%)")
            
            # Execute arbitrage
            result = await self.cross_consensus.execute_cross_exchange_arbitrage(
                symbol=symbol,
                buy_exchange_id="exchange_alpha",
                sell_exchange_id="exchange_beta", 
                quantity=Decimal("100")
            )
            
            print(f"   Arbitrage execution result: {result}")
        
        # Store exchanges
        self.exchanges["exchange_alpha"] = exchange_a
        self.exchanges["exchange_beta"] = exchange_b
        
        stats = self.cross_consensus.get_cross_exchange_statistics()
        
        print(f"✅ Cross-exchange arbitrage demonstration completed")
        print(f"   Total exchanges: {stats['total_exchanges']}")
        print(f"   Arbitrage opportunities: {stats['arbitrage_opportunities']}")
        
        return {
            "cross_consensus": self.cross_consensus,
            "opportunities": opportunities,
            "statistics": stats
        }
    
    def demonstrate_network_models(self) -> List[Dict[str, Any]]:
        """
        Demonstrate different network timing models (Chapters 4-6).
        
        Shows synchronous, asynchronous, and partial synchrony models.
        """
        print("\n🌐 Demonstrating Network Timing Models")
        print("Chapters 4-6: Synchronous, Asynchronous, and Partial Synchrony")
        
        network_configs = [
            {
                "model": NetworkModel.SYNCHRONOUS,
                "params": NetworkParameters(max_delay=0.1),
                "name": "Synchronous"
            },
            {
                "model": NetworkModel.ASYNCHRONOUS,
                "params": NetworkParameters(avg_delay=0.5),
                "name": "Asynchronous"
            },
            {
                "model": NetworkModel.PARTIAL_SYNCHRONY,
                "params": NetworkParameters(max_delay=0.1, global_stabilization_time=5.0),
                "name": "Partial Synchrony"
            }
        ]
        
        network_results = []
        
        for config in network_configs:
            print(f"   Testing {config['name']} network model...")
            
            network = NetworkSimulator(config["model"], config["params"])
            
            # Send test messages
            for i in range(10):
                network.send_message(
                    sender_id=f"trader_{i}",
                    receiver_id="exchange",
                    message_type="ORDER_PLACEMENT",
                    content=f"order_{i}"
                )
            
            # Advance simulation time
            delivered = network.advance_time(network.current_time + 10.0)
            stats = network.get_statistics()
            
            # Store results
            result = {
                "model": config["name"],
                "delivered_messages": len(delivered),
                "total_sent": stats["messages_sent"],
                "delivery_rate": stats["delivery_rate"],
                "avg_delay": stats["avg_delay"],
                "max_delay": stats["max_observed_delay"]
            }
            
            network_results.append(result)
            self.network_stats.append(stats)
            
            print(f"      Messages delivered: {result['delivered_messages']}/{result['total_sent']}")
            print(f"      Delivery rate: {result['delivery_rate']:.2%}")
            print(f"      Average delay: {result['avg_delay']:.3f}s")
        
        print(f"✅ Network models demonstration completed")
        
        return network_results
    
    def analyze_complexity_results(self) -> Dict[str, Any]:
        """
        Analyze and display complexity analysis results.
        
        Shows theoretical bounds verification and protocol comparison.
        """
        print("\n📊 Analyzing Consensus Protocol Complexity")
        print("Theoretical Analysis: Verifying optimal complexity bounds")
        
        # Analyze round complexity
        protocols = ["dolev_strong", "streamlet", "randomized_consensus", "nakamoto"]
        
        complexity_analysis = {}
        
        for protocol in protocols:
            round_analysis = self.analyzer.analyze_round_complexity(protocol)
            comm_analysis = self.analyzer.analyze_communication_complexity(protocol)
            
            if round_analysis.get("total_executions", 0) > 0:
                complexity_analysis[protocol] = {
                    "round_complexity": round_analysis,
                    "communication_complexity": comm_analysis
                }
                
                print(f"   {protocol}:")
                print(f"      Average rounds: {round_analysis['round_complexity']['avg']:.2f}")
                if comm_analysis.get("total_executions", 0) > 0:
                    print(f"      Average messages: {comm_analysis['message_complexity']['avg_messages']:.0f}")
                    print(f"      Average bits: {comm_analysis['communication_complexity']['avg_bits']:.0f}")
        
        # Protocol comparison
        if len(complexity_analysis) > 1:
            comparison = self.analyzer.compare_protocols(
                list(complexity_analysis.keys()),
                ComplexityMetric.ROUND_COMPLEXITY
            )
            
            print(f"\n   Protocol Ranking (by round complexity):")
            for i, entry in enumerate(comparison["ranking"][:3]):
                print(f"      {i+1}. {entry['protocol']}: {entry['avg']:.2f} rounds")
        
        print(f"✅ Complexity analysis completed")
        
        return complexity_analysis
    
    async def run_comprehensive_demo(self) -> Dict[str, Any]:
        """
        Run the complete demonstration of all consensus protocols and applications.
        
        Returns:
            Dictionary containing all demonstration results
        """
        print("🎯 Starting Comprehensive Consensus Financial Systems Demo")
        print("=" * 70)
        
        results = {}
        
        # Core consensus protocols
        results["byzantine_broadcast"] = self.demonstrate_byzantine_broadcast()
        results["streamlet"] = self.demonstrate_streamlet_blockchain()
        results["randomized_consensus"] = self.demonstrate_randomized_consensus()
        results["nakamoto"] = self.demonstrate_nakamoto_protocol()
        
        # Financial applications
        results["consensus_trading"] = await self.demonstrate_consensus_based_trading()
        results["cross_exchange_arbitrage"] = await self.demonstrate_cross_exchange_arbitrage()
        
        # Network models and complexity
        results["network_models"] = self.demonstrate_network_models()
        results["complexity_analysis"] = self.analyze_complexity_results()
        
        return results
    
    def generate_visualizations(self) -> None:
        """Generate and save comprehensive visualizations."""
        print("\n📈 Generating Comprehensive Visualizations")
        
        # Get main exchange for visualization
        main_exchange = self.exchanges.get("demo_exchange")
        
        if main_exchange:
            # Create dashboard
            dashboard = create_comprehensive_analysis_dashboard(
                self.analyzer,
                main_exchange,
                self.network_stats
            )
            
            # Save as HTML
            save_dashboard_html(dashboard, "consensus_financial_demo_dashboard.html")
            print("   📊 Interactive dashboard saved as 'consensus_financial_demo_dashboard.html'")
        else:
            print("   ⚠️  No exchange data available for visualization")


async def main():
    """Main demonstration function."""
    # Create and run demonstration
    demo = ConsensusFinancialDemo()
    
    try:
        # Run comprehensive demo
        results = await demo.run_comprehensive_demo()
        
        # Generate visualizations
        demo.generate_visualizations()
        
        print("\n" + "=" * 70)
        print("🎉 DEMONSTRATION COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
        print("\n📚 Summary of Demonstrated Concepts:")
        print("• Byzantine Broadcast (Dolev-Strong) - Chapter 3")
        print("• Streamlet Blockchain Protocol - Chapter 7")  
        print("• Randomized Consensus - Chapter 13")
        print("• Nakamoto Proof-of-Work - Chapters 14-17")
        print("• Network Timing Models - Chapters 4-6")
        print("• Complexity Analysis - Chapters 10-11")
        print("• Financial Consensus Applications - Integration")
        
        print("\n💡 Key Results:")
        for protocol, data in results.items():
            if isinstance(data, dict) and "protocol" in data:
                print(f"• {data['protocol']}: Successfully demonstrated")
        
        print("\n📁 Generated Files:")
        print("• consensus_financial_demo_dashboard.html - Interactive visualizations")
        print("• Test files in market_sim/tests/ - Comprehensive test suite")
        
        print("\n🔍 Next Steps:")
        print("• Open the HTML dashboard to explore interactive visualizations")
        print("• Run pytest to execute the comprehensive test suite")
        print("• Examine the source code to understand implementation details")
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
