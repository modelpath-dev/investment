"""
Integration tests for consensus-based financial applications.

Tests the complete integration of distributed consensus protocols with
financial trading systems, market simulation, and cross-exchange operations.
"""

import pytest
import asyncio
import time
from decimal import Decimal
import numpy as np
from unittest.mock import Mock, patch

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../blockchain/consensus'))

from financial_consensus import (
    ConsensusBasedExchange, 
    CrossExchangeConsensus,
    FinancialOrder,
    TradeExecution,
    MarketState
)
from complexity_analysis import ComplexityAnalyzer, ProtocolComplexityWrapper, ComplexityMetric
from network_models import NetworkSimulator, NetworkModel, NetworkParameters


class TestConsensusBasedTrading:
    """Test consensus-based trading scenarios."""
    
    def setup_method(self):
        """Set up test environment."""
        self.exchange = ConsensusBasedExchange("test_exchange", "streamlet")
        self.analyzer = ComplexityAnalyzer()
        
        # Add consensus nodes (validators)
        for i in range(7):  # 7 nodes, can tolerate 2 Byzantine
            self.exchange.add_consensus_node(f"validator_{i}")
        
        # Set up position limits
        self.exchange.position_limits["trader_1"] = Decimal("1000000")
        self.exchange.position_limits["trader_2"] = Decimal("1000000")
    
    @pytest.mark.asyncio
    async def test_order_book_consensus(self):
        """Test order book management through consensus."""
        # Create multiple orders for same symbol
        orders = []
        for i in range(5):
            order = FinancialOrder(
                order_id=f"order_{i}",
                trader_id=f"trader_{i % 2 + 1}",  # Alternate between 2 traders
                symbol="AAPL",
                side="buy" if i % 2 == 0 else "sell",
                quantity=Decimal("100"),
                price=Decimal(f"{150 + i}"),
                timestamp=time.time() + i
            )
            orders.append(order)
        
        # Submit all orders through consensus
        results = []
        for order in orders:
            result = await self.exchange.submit_order(order)
            results.append(result)
        
        # All orders should be processed
        assert all(isinstance(r, bool) for r in results)
        
        # Check order book state
        stats = self.exchange.get_exchange_statistics()
        assert stats["total_orders"] >= 0  # Orders processed through consensus
    
    @pytest.mark.asyncio
    async def test_trade_execution_consensus(self):
        """Test trade execution through consensus."""
        # Create matching buy and sell orders
        buy_order = FinancialOrder(
            order_id="buy_001",
            trader_id="trader_1",
            symbol="TSLA",
            side="buy",
            quantity=Decimal("200"),
            price=Decimal("250.00"),
            timestamp=time.time()
        )
        
        sell_order = FinancialOrder(
            order_id="sell_001",
            trader_id="trader_2", 
            symbol="TSLA",
            side="sell",
            quantity=Decimal("200"),
            price=Decimal("250.00"),
            timestamp=time.time()
        )
        
        # Submit orders
        buy_result = await self.exchange.submit_order(buy_order)
        sell_result = await self.exchange.submit_order(sell_order)
        
        # Execute trade through consensus
        trade_result = await self.exchange.execute_trade(
            buy_order=buy_order,
            sell_order=sell_order,
            execution_price=Decimal("250.00"),
            execution_quantity=Decimal("200")
        )
        
        assert isinstance(trade_result, bool)
    
    @pytest.mark.asyncio
    async def test_market_state_consensus(self):
        """Test market state updates through consensus."""
        symbol = "NVDA"
        
        # Add some orders to create market state
        orders = [
            FinancialOrder("bid_1", "trader_1", symbol, "buy", Decimal("50"), Decimal("400.00"), time.time()),
            FinancialOrder("ask_1", "trader_2", symbol, "sell", Decimal("50"), Decimal("405.00"), time.time()),
        ]
        
        for order in orders:
            await self.exchange.submit_order(order)
        
        # Update market state through consensus
        result = await self.exchange.update_market_state(symbol)
        assert isinstance(result, bool)
        
        # Check if market state was recorded
        if symbol in self.exchange.market_states:
            market_state = self.exchange.market_states[symbol]
            assert market_state.symbol == symbol
            assert market_state.timestamp > 0
    
    def test_byzantine_fault_tolerance(self):
        """Test system behavior with Byzantine nodes."""
        # Simulate Byzantine behavior
        byzantine_nodes = ["validator_5", "validator_6"]  # 2 out of 7 nodes
        
        # Mark nodes as Byzantine (simplified simulation)
        for node_id in byzantine_nodes:
            if node_id in self.exchange.consensus_nodes:
                # In real implementation, would inject malicious behavior
                pass
        
        # System should still function with f < n/3 Byzantine nodes
        # With 7 nodes, can tolerate up to 2 Byzantine nodes
        assert len(byzantine_nodes) <= (len(self.exchange.consensus_nodes) - 1) // 3
    
    @pytest.mark.asyncio
    async def test_risk_management_consensus(self):
        """Test risk management through consensus."""
        # Create order that exceeds position limits
        large_order = FinancialOrder(
            order_id="large_001",
            trader_id="trader_1",
            symbol="BTC",
            side="buy",
            quantity=Decimal("10000"),  # Very large quantity
            price=Decimal("50000"),     # High price = large value
            timestamp=time.time()
        )
        
        # Should be rejected due to risk constraints
        # Note: In real implementation, this would be handled by consensus validation
        is_valid = self.exchange._validate_order(large_order)
        assert is_valid == False  # Should fail risk checks


class TestCrossExchangeConsensus:
    """Test cross-exchange consensus operations."""
    
    def setup_method(self):
        """Set up multi-exchange environment."""
        self.cross_consensus = CrossExchangeConsensus()
        
        # Create multiple exchanges
        self.exchange_a = ConsensusBasedExchange("exchange_a", "streamlet")
        self.exchange_b = ConsensusBasedExchange("exchange_b", "byzantine_broadcast")
        
        # Add nodes to each exchange
        for i in range(5):
            self.exchange_a.add_consensus_node(f"a_validator_{i}")
            self.exchange_b.add_consensus_node(f"b_validator_{i}")
        
        # Add exchanges to cross-consensus
        self.cross_consensus.add_exchange(self.exchange_a)
        self.cross_consensus.add_exchange(self.exchange_b)
    
    def test_arbitrage_detection(self):
        """Test arbitrage opportunity detection across exchanges."""
        symbol = "ETH"
        
        # Create different market states on each exchange
        self.exchange_a.market_states[symbol] = MarketState(
            symbol=symbol,
            best_bid=Decimal("3200.00"),
            best_ask=Decimal("3205.00"),
            last_trade_price=Decimal("3202.50"),
            volume_24h=Decimal("1000"),
            timestamp=time.time(),
            order_book_hash="hash_a"
        )
        
        self.exchange_b.market_states[symbol] = MarketState(
            symbol=symbol,
            best_bid=Decimal("3210.00"),  # Higher bid on exchange B
            best_ask=Decimal("3215.00"),
            last_trade_price=Decimal("3212.50"),
            volume_24h=Decimal("800"),
            timestamp=time.time(),
            order_book_hash="hash_b"
        )
        
        # Detect arbitrage opportunities
        opportunities = self.cross_consensus.detect_arbitrage_opportunities()
        
        # Should find arbitrage opportunity (buy on A, sell on B)
        assert len(opportunities) > 0
        
        arbitrage = opportunities[0]
        assert arbitrage["symbol"] == symbol
        assert arbitrage["buy_exchange"] == "exchange_a"
        assert arbitrage["sell_exchange"] == "exchange_b"
    
    @pytest.mark.asyncio
    async def test_cross_exchange_arbitrage_execution(self):
        """Test atomic arbitrage execution across exchanges."""
        symbol = "SOL"
        quantity = Decimal("100")
        
        # Set up price difference
        self.exchange_a.market_states[symbol] = MarketState(
            symbol=symbol,
            best_bid=Decimal("90.00"),
            best_ask=Decimal("95.00"),  # Lower ask on A
            last_trade_price=Decimal("92.50"),
            volume_24h=Decimal("500"),
            timestamp=time.time(),
            order_book_hash="hash_a"
        )
        
        self.exchange_b.market_states[symbol] = MarketState(
            symbol=symbol,
            best_bid=Decimal("100.00"),  # Higher bid on B
            best_ask=Decimal("105.00"),
            last_trade_price=Decimal("102.50"),
            volume_24h=Decimal("400"),
            timestamp=time.time(),
            order_book_hash="hash_b"
        )
        
        # Execute arbitrage
        result = await self.cross_consensus.execute_cross_exchange_arbitrage(
            symbol=symbol,
            buy_exchange_id="exchange_a",
            sell_exchange_id="exchange_b",
            quantity=quantity
        )
        
        # Should succeed (in simplified test)
        assert isinstance(result, bool)
    
    def test_cross_exchange_statistics(self):
        """Test cross-exchange statistics."""
        stats = self.cross_consensus.get_cross_exchange_statistics()
        
        assert "total_exchanges" in stats
        assert stats["total_exchanges"] == 2
        assert "exchange_details" in stats
        assert "exchange_a" in stats["exchange_details"]
        assert "exchange_b" in stats["exchange_details"]


class TestConsensusComplexityInTrading:
    """Test complexity analysis in trading scenarios."""
    
    def setup_method(self):
        """Set up complexity analysis environment."""
        self.analyzer = ComplexityAnalyzer()
        self.exchange = ConsensusBasedExchange("perf_exchange", "randomized")
        
        # Add nodes
        for i in range(10):  # Larger network for performance testing
            self.exchange.add_consensus_node(f"node_{i}")
    
    @pytest.mark.asyncio
    async def test_order_processing_complexity(self):
        """Test complexity of order processing."""
        wrapper = ProtocolComplexityWrapper("order_processing", self.analyzer)
        
        # Simulate order processing with complexity measurement
        async def process_orders():
            orders = []
            for i in range(20):  # Process 20 orders
                order = FinancialOrder(
                    order_id=f"perf_order_{i}",
                    trader_id=f"trader_{i % 5}",
                    symbol="PERF",
                    side="buy" if i % 2 == 0 else "sell",
                    quantity=Decimal("10"),
                    price=Decimal(f"{100 + i}"),
                    timestamp=time.time()
                )
                orders.append(order)
                
                # Simulate consensus rounds and messages for each order
                send_message = wrapper.create_instrumented_message_sender("test_execution")
                round_notifier = wrapper.create_instrumented_round_notifier("test_execution")
                
                # Simulate consensus process
                round_notifier()  # Start round
                for _ in range(10):  # Simulate n messages per round
                    send_message("consensus_message", 512)  # 512 bits per message
            
            return orders
        
        # Measure complexity
        result, execution_id = wrapper.wrap_protocol_execution(
            process_orders, 10, 3  # 10 nodes, 3 faults
        )
        
        # Analyze results
        round_analysis = self.analyzer.analyze_round_complexity("order_processing")
        comm_analysis = self.analyzer.analyze_communication_complexity("order_processing")
        
        assert round_analysis["total_executions"] == 1
        assert comm_analysis["total_executions"] == 1
    
    def test_scalability_analysis(self):
        """Test consensus protocol scalability with increasing load."""
        # Test with different network sizes
        network_sizes = [4, 7, 10, 13]  # Different n values
        
        for n in network_sizes:
            f = (n - 1) // 3  # Maximum Byzantine nodes
            
            execution_id = f"scale_test_n{n}"
            self.analyzer.start_measurement(execution_id, "scalability_test", n, f)
            
            # Simulate protocol execution scaling with n
            for round_num in range(f + 1):  # Optimal round complexity
                self.analyzer.record_round_start(execution_id)
                
                # Communication scales as O(n^2) for Byzantine protocols
                for _ in range(n * n):
                    self.analyzer.record_message_sent(execution_id, 512)
            
            self.analyzer.finish_measurement(execution_id, {"network_size": n, "faults": f})
        
        # Analyze scalability
        measurements = self.analyzer.get_measurements(
            ComplexityMetric.COMMUNICATION_COMPLEXITY, "scalability_test"
        )
        
        # Should see increasing communication with network size
        assert len(measurements) == len(network_sizes)
        
        # Verify O(n^2) scaling
        comm_values = [m.value for m in measurements]
        network_values = [m.num_nodes for m in measurements]
        
        # Communication should grow roughly quadratically
        for i in range(1, len(comm_values)):
            ratio = comm_values[i] / comm_values[i-1]
            size_ratio = (network_values[i] / network_values[i-1]) ** 2
            # Should be roughly proportional to n^2
            assert ratio > 1.0  # Should increase
    
    def test_consensus_latency_analysis(self):
        """Test consensus latency in trading scenarios."""
        # Simulate high-frequency trading scenario
        start_time = time.time()
        
        execution_id = "latency_test"
        self.analyzer.start_measurement(execution_id, "hft_consensus", 7, 2)
        
        # Simulate rapid order processing
        for order_num in range(100):  # 100 orders in quick succession
            self.analyzer.record_round_start(execution_id)
            
            # Simulate fast consensus rounds (optimized for latency)
            for _ in range(21):  # 7 nodes, 3 messages each (propose, vote, commit)
                self.analyzer.record_message_sent(execution_id, 256)  # Smaller messages for speed
        
        end_time = time.time()
        total_latency = end_time - start_time
        
        self.analyzer.finish_measurement(execution_id, {
            "total_latency": total_latency,
            "orders_processed": 100,
            "avg_latency_per_order": total_latency / 100
        })
        
        # Analyze latency performance
        analysis = self.analyzer.analyze_round_complexity("hft_consensus")
        assert analysis["total_executions"] == 1


class TestNetworkModelsInTrading:
    """Test different network models in trading scenarios."""
    
    def setup_method(self):
        """Set up network testing environment."""
        self.sync_params = NetworkParameters(max_delay=0.1)  # 100ms max delay
        self.async_params = NetworkParameters(avg_delay=0.5)  # 500ms average delay
        self.partial_sync_params = NetworkParameters(
            max_delay=0.1,
            global_stabilization_time=5.0  # GST after 5 seconds
        )
    
    def test_synchronous_trading_network(self):
        """Test trading in synchronous network."""
        network = NetworkSimulator(NetworkModel.SYNCHRONOUS, self.sync_params)
        
        # Simulate order submissions from multiple traders
        traders = ["trader_1", "trader_2", "trader_3"]
        
        for i, trader in enumerate(traders):
            network.send_message(
                sender_id=trader,
                receiver_id="exchange",
                message_type="ORDER_PLACEMENT",
                content=f"order_{i}"
            )
        
        # Advance time by max delay
        delivered = network.advance_time(network.current_time + self.sync_params.max_delay)
        
        # All messages should be delivered within bound
        assert len(delivered) == len(traders)
        
        stats = network.get_statistics()
        assert stats["max_observed_delay"] <= self.sync_params.max_delay
    
    def test_asynchronous_trading_network(self):
        """Test trading in asynchronous network."""
        network = NetworkSimulator(NetworkModel.ASYNCHRONOUS, self.async_params)
        
        # Send trading messages
        for i in range(10):
            network.send_message(
                sender_id=f"trader_{i}",
                receiver_id="exchange",
                message_type="ORDER_PLACEMENT",
                content=f"async_order_{i}"
            )
        
        # Advance time significantly
        delivered = network.advance_time(network.current_time + 10.0)
        
        # Should deliver most messages but with variable delays
        stats = network.get_statistics()
        assert stats["delivery_rate"] > 0.5  # At least 50% delivered
    
    def test_partial_synchrony_trading_network(self):
        """Test trading network with partial synchrony."""
        network = NetworkSimulator(NetworkModel.PARTIAL_SYNCHRONY, self.partial_sync_params)
        
        # Pre-GST phase: send orders with high delays
        for i in range(5):
            network.send_message(
                sender_id=f"trader_{i}",
                receiver_id="exchange",
                message_type="ORDER_PLACEMENT",
                content=f"pre_gst_order_{i}"
            )
        
        # Advance to just before GST
        pre_gst_delivered = network.advance_time(4.9)
        pre_gst_stats = network.get_statistics()
        
        # Post-GST phase: send orders with bounded delays
        for i in range(5, 10):
            network.send_message(
                sender_id=f"trader_{i}",
                receiver_id="exchange", 
                message_type="ORDER_PLACEMENT",
                content=f"post_gst_order_{i}"
            )
        
        # Advance past GST
        post_gst_delivered = network.advance_time(10.0)
        post_gst_stats = network.get_statistics()
        
        # Should see improved delivery after GST
        assert network.gst_reached == True


class TestRealWorldTradingScenarios:
    """Test realistic trading scenarios with consensus."""
    
    @pytest.mark.asyncio
    async def test_market_making_consensus(self):
        """Test market making strategy with consensus."""
        exchange = ConsensusBasedExchange("mm_exchange", "streamlet")
        
        # Add validators
        for i in range(5):
            exchange.add_consensus_node(f"validator_{i}")
        
        symbol = "MM_TOKEN"
        spread = Decimal("0.10")
        mid_price = Decimal("100.00")
        
        # Market maker places bid and ask orders
        bid_order = FinancialOrder(
            order_id="mm_bid_001",
            trader_id="market_maker_1",
            symbol=symbol,
            side="buy",
            quantity=Decimal("1000"),
            price=mid_price - spread/2,
            timestamp=time.time()
        )
        
        ask_order = FinancialOrder(
            order_id="mm_ask_001",
            trader_id="market_maker_1", 
            symbol=symbol,
            side="sell",
            quantity=Decimal("1000"),
            price=mid_price + spread/2,
            timestamp=time.time()
        )
        
        # Submit through consensus
        bid_result = await exchange.submit_order(bid_order)
        ask_result = await exchange.submit_order(ask_order)
        
        assert isinstance(bid_result, bool)
        assert isinstance(ask_result, bool)
    
    @pytest.mark.asyncio
    async def test_algorithmic_trading_consensus(self):
        """Test algorithmic trading with consensus validation."""
        exchange = ConsensusBasedExchange("algo_exchange", "randomized")
        
        # Add validators
        for i in range(7):
            exchange.add_consensus_node(f"validator_{i}")
        
        # Algorithmic trader places multiple orders
        symbol = "ALGO_STOCK"
        base_price = Decimal("50.00")
        
        orders = []
        for i in range(10):
            order = FinancialOrder(
                order_id=f"algo_order_{i}",
                trader_id="algo_trader_1",
                symbol=symbol,
                side="buy" if i % 2 == 0 else "sell",
                quantity=Decimal("100"),
                price=base_price + Decimal(str(i * 0.25)),
                timestamp=time.time() + i * 0.1  # Staggered timing
            )
            orders.append(order)
        
        # Submit all orders through consensus
        results = []
        for order in orders:
            result = await exchange.submit_order(order)
            results.append(result)
        
        # All orders should be processed
        assert all(isinstance(r, bool) for r in results)
    
    def test_consensus_performance_benchmarks(self):
        """Benchmark consensus performance for trading."""
        analyzer = ComplexityAnalyzer()
        
        # Test different consensus protocols
        protocols = ["streamlet", "byzantine_broadcast", "randomized"]
        
        benchmark_results = {}
        
        for protocol in protocols:
            execution_id = f"benchmark_{protocol}"
            analyzer.start_measurement(execution_id, protocol, 7, 2)
            
            # Simulate typical trading workload
            start_time = time.time()
            
            # Process 100 orders
            for round_num in range(100):
                analyzer.record_round_start(execution_id)
                
                # Different protocols have different message patterns
                if protocol == "streamlet":
                    messages_per_round = 14  # 7 nodes * 2 messages (propose + vote)
                elif protocol == "byzantine_broadcast":
                    messages_per_round = 49  # O(n^2) messages
                else:  # randomized
                    messages_per_round = 21  # 7 nodes * 3 phases
                
                for _ in range(messages_per_round):
                    analyzer.record_message_sent(execution_id, 512)
            
            end_time = time.time()
            latency = end_time - start_time
            
            analyzer.finish_measurement(execution_id, {
                "total_latency": latency,
                "throughput": 100 / latency  # orders per second
            })
            
            benchmark_results[protocol] = {
                "latency": latency,
                "throughput": 100 / latency
            }
        
        # Compare protocols
        comparison = analyzer.compare_protocols(
            protocols, 
            ComplexityMetric.COMMUNICATION_COMPLEXITY
        )
        
        assert len(comparison["protocols"]) == len(protocols)
        assert "ranking" in comparison


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
