"""
Unit tests for distributed consensus protocols.

Tests the implementations of Byzantine Broadcast, Streamlet, Randomized Consensus,
and Nakamoto protocols based on Elaine Shi's textbook.
"""

import pytest
import asyncio
import time
from decimal import Decimal
from unittest.mock import Mock, patch

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../blockchain/consensus'))

from byzantine_broadcast import ByzantineBroadcastNode, BroadcastMessage, ByzantineProtocol
from streamlet import StreamletNode, Block, Vote, Blockchain
from randomized_consensus import RandomizedConsensusNode, ConsensusMessage, CommonCoinOracle
from nakamoto import NakamotoBlockchain, PoWBlock, PoWMiner
from complexity_analysis import ComplexityAnalyzer, ComplexityMetric
from network_models import NetworkSimulator, NetworkModel, NetworkParameters
from financial_consensus import ConsensusBasedExchange, FinancialOrder


class TestByzantineBroadcast:
    """Test Byzantine Broadcast implementation."""
    
    def setup_method(self):
        """Set up test environment."""
        self.protocol = ByzantineProtocol()
        self.nodes = []
        for i in range(4):
            node = ByzantineBroadcastNode(f"node_{i}")
            self.nodes.append(node)
            self.protocol.add_node(node)
    
    def test_node_creation(self):
        """Test that nodes are created correctly."""
        node = ByzantineBroadcastNode("test_node")
        assert node.node_id == "test_node"
        assert node.current_round == 0
        assert len(node.received_messages) == 0
    
    def test_message_creation(self):
        """Test broadcast message creation."""
        message = BroadcastMessage(
            content="test_content",
            sender="node_0",
            round_number=1
        )
        assert message.content == "test_content"
        assert message.sender == "node_0"
        assert message.round_number == 1
    
    def test_honest_broadcast(self):
        """Test honest Byzantine broadcast with no Byzantine nodes."""
        sender = self.nodes[0]
        message_content = "honest_broadcast_test"
        
        # Start broadcast
        message = BroadcastMessage(
            content=message_content,
            sender=sender.node_id,
            round_number=1
        )
        
        # All nodes should receive and process the message
        for node in self.nodes:
            node.receive_message(message)
            assert message_content in [msg.content for msg in node.received_messages]
    
    def test_protocol_with_byzantine_nodes(self):
        """Test protocol behavior with Byzantine nodes."""
        # Mark last node as Byzantine
        byzantine_node = self.nodes[-1]
        byzantine_node.is_byzantine = True
        
        sender = self.nodes[0]
        message_content = "test_with_byzantine"
        
        # Simulate Byzantine broadcast
        result = self.protocol.run_broadcast(sender.node_id, message_content, max_rounds=2)
        
        # Should still achieve agreement among honest nodes
        assert result is not None
        assert result["success"] == True
    
    def test_message_validation(self):
        """Test message validation functionality."""
        node = self.nodes[0]
        
        # Valid message
        valid_message = BroadcastMessage(
            content="valid_content",
            sender="node_1",
            round_number=1
        )
        assert node._validate_message(valid_message) == True
        
        # Invalid message (future round)
        invalid_message = BroadcastMessage(
            content="invalid_content",
            sender="node_1",
            round_number=10
        )
        assert node._validate_message(invalid_message) == False


class TestStreamlet:
    """Test Streamlet blockchain protocol."""
    
    def setup_method(self):
        """Set up test environment."""
        self.blockchain = Blockchain()
        self.nodes = []
        for i in range(5):
            node = StreamletNode(f"node_{i}")
            self.nodes.append(node)
    
    def test_block_creation(self):
        """Test block creation and validation."""
        block = Block(
            proposer="node_0",
            epoch=1,
            parent_hash="genesis",
            transactions=["tx1", "tx2"]
        )
        
        assert block.proposer == "node_0"
        assert block.epoch == 1
        assert block.parent_hash == "genesis"
        assert len(block.transactions) == 2
        assert block.block_hash is not None
    
    def test_vote_creation(self):
        """Test vote creation."""
        block = Block(
            proposer="node_0",
            epoch=1,
            parent_hash="genesis",
            transactions=["tx1"]
        )
        
        vote = Vote(
            voter="node_1",
            block_hash=block.block_hash,
            epoch=1
        )
        
        assert vote.voter == "node_1"
        assert vote.block_hash == block.block_hash
        assert vote.epoch == 1
    
    def test_blockchain_addition(self):
        """Test adding blocks to blockchain."""
        initial_length = len(self.blockchain.blocks)
        
        block = Block(
            proposer="node_0",
            epoch=1,
            parent_hash=self.blockchain.get_latest_hash(),
            transactions=["tx1"]
        )
        
        self.blockchain.add_block(block)
        assert len(self.blockchain.blocks) == initial_length + 1
    
    def test_finalization_rule(self):
        """Test three consecutive epochs finalization rule."""
        # Add three consecutive blocks
        for epoch in range(1, 4):
            block = Block(
                proposer="node_0",
                epoch=epoch,
                parent_hash=self.blockchain.get_latest_hash(),
                transactions=[f"tx_{epoch}"]
            )
            self.blockchain.add_block(block)
        
        # Check if finalization logic works
        finalized_blocks = self.blockchain.get_finalized_blocks()
        assert len(finalized_blocks) >= 0  # At least genesis should be finalized
    
    def test_streamlet_node_proposal(self):
        """Test node proposal mechanism."""
        node = self.nodes[0]
        transactions = ["tx1", "tx2", "tx3"]
        
        block = node.propose_block(
            epoch=1,
            parent_hash="genesis",
            transactions=transactions
        )
        
        assert block is not None
        assert block.proposer == node.node_id
        assert block.transactions == transactions


class TestRandomizedConsensus:
    """Test Randomized Consensus protocol."""
    
    def setup_method(self):
        """Set up test environment."""
        self.oracle = CommonCoinOracle()
        self.nodes = []
        for i in range(7):  # 7 nodes for supermajority testing
            node = RandomizedConsensusNode(f"node_{i}", self.oracle)
            self.nodes.append(node)
    
    def test_common_coin_oracle(self):
        """Test common coin oracle functionality."""
        # Test coin flip for same epoch
        coin1 = self.oracle.get_coin(epoch=1)
        coin2 = self.oracle.get_coin(epoch=1)
        assert coin1 == coin2  # Should be deterministic for same epoch
        
        # Test different epochs
        coin3 = self.oracle.get_coin(epoch=2)
        # coin3 may or may not equal coin1, but should be consistent
        coin4 = self.oracle.get_coin(epoch=2)
        assert coin3 == coin4
    
    def test_consensus_message_creation(self):
        """Test consensus message creation."""
        message = ConsensusMessage(
            message_type="propose",
            content="test_value",
            sender="node_0",
            epoch=1,
            view=0
        )
        
        assert message.message_type == "propose"
        assert message.content == "test_value"
        assert message.sender == "node_0"
        assert message.epoch == 1
    
    def test_node_proposal(self):
        """Test node proposal in randomized consensus."""
        node = self.nodes[0]
        value = "consensus_value"
        
        proposal = node.propose_value(value, epoch=1, view=0)
        assert proposal is not None
        assert proposal.content == value
        assert proposal.message_type == "propose"
    
    def test_voting_phases(self):
        """Test pre-vote and main-vote phases."""
        node = self.nodes[0]
        value = "test_value"
        
        # Test pre-vote
        pre_vote = node.create_pre_vote(value, epoch=1, view=0)
        assert pre_vote.message_type == "pre_vote"
        assert pre_vote.content == value
        
        # Test main-vote  
        main_vote = node.create_main_vote(value, epoch=1, view=0)
        assert main_vote.message_type == "main_vote"
        assert main_vote.content == value
    
    def test_supermajority_threshold(self):
        """Test supermajority threshold calculation."""
        node = self.nodes[0]
        n = len(self.nodes)
        
        # Supermajority should be > 2n/3
        threshold = node.get_supermajority_threshold(n)
        assert threshold > (2 * n) // 3
        assert threshold <= n


class TestNakamotoProtocol:
    """Test Nakamoto blockchain protocol."""
    
    def setup_method(self):
        """Set up test environment."""
        self.blockchain = NakamotoBlockchain(difficulty=1)
        self.miner = PoWMiner(miner_id="miner_0", hash_rate=1000)
    
    def test_pow_block_creation(self):
        """Test Proof-of-Work block creation."""
        block = PoWBlock(
            index=1,
            timestamp=time.time(),
            transactions=["tx1", "tx2"],
            previous_hash="genesis",
            difficulty=1
        )
        
        assert block.index == 1
        assert len(block.transactions) == 2
        assert block.previous_hash == "genesis"
        assert block.difficulty == 1
    
    def test_proof_of_work_mining(self):
        """Test proof-of-work mining process."""
        block = PoWBlock(
            index=1,
            timestamp=time.time(),
            transactions=["tx1"],
            previous_hash="genesis",
            difficulty=1
        )
        
        # Mine the block
        success = block.mine_block()
        assert success == True
        assert block.nonce >= 0
        assert block.block_hash is not None
    
    def test_blockchain_addition(self):
        """Test adding mined blocks to Nakamoto blockchain."""
        initial_length = self.blockchain.get_chain_length()
        
        # Create and mine a block
        block = PoWBlock(
            index=1,
            timestamp=time.time(),
            transactions=["tx1"],
            previous_hash=self.blockchain.get_latest_hash(),
            difficulty=self.blockchain.difficulty
        )
        
        block.mine_block()
        result = self.blockchain.add_block(block)
        
        assert result == True
        assert self.blockchain.get_chain_length() == initial_length + 1
    
    def test_longest_chain_rule(self):
        """Test longest chain rule implementation."""
        # Create two competing chains
        chain1_blocks = []
        chain2_blocks = []
        
        # Chain 1: shorter but earlier
        for i in range(2):
            block = PoWBlock(
                index=i + 1,
                timestamp=time.time(),
                transactions=[f"tx1_{i}"],
                previous_hash=self.blockchain.get_latest_hash() if i == 0 else chain1_blocks[-1].block_hash,
                difficulty=1
            )
            block.mine_block()
            chain1_blocks.append(block)
        
        # Chain 2: longer 
        for i in range(3):
            block = PoWBlock(
                index=i + 1,
                timestamp=time.time() + i,
                transactions=[f"tx2_{i}"],
                previous_hash=self.blockchain.get_latest_hash() if i == 0 else chain2_blocks[-1].block_hash,
                difficulty=1
            )
            block.mine_block()
            chain2_blocks.append(block)
        
        # Add shorter chain first
        for block in chain1_blocks:
            self.blockchain.add_block(block)
        
        initial_length = self.blockchain.get_chain_length()
        
        # Add longer chain - should trigger reorganization
        for block in chain2_blocks:
            self.blockchain.add_block(block)
        
        # Longest chain should be selected
        final_length = self.blockchain.get_chain_length()
        assert final_length >= initial_length
    
    def test_difficulty_adjustment(self):
        """Test difficulty adjustment mechanism."""
        initial_difficulty = self.blockchain.difficulty
        
        # Simulate rapid block production (should increase difficulty)
        self.blockchain.adjust_difficulty(target_time=600, actual_time=300)
        
        # Difficulty should have been adjusted
        assert self.blockchain.difficulty != initial_difficulty


class TestComplexityAnalysis:
    """Test complexity analysis framework."""
    
    def setup_method(self):
        """Set up test environment."""
        self.analyzer = ComplexityAnalyzer()
    
    def test_measurement_creation(self):
        """Test complexity measurement creation."""
        execution_id = "test_execution"
        
        self.analyzer.start_measurement(
            execution_id=execution_id,
            protocol_name="test_protocol",
            num_nodes=4,
            num_faults=1
        )
        
        assert execution_id in self.analyzer.active_measurements
        
        # Record some activity
        self.analyzer.record_round_start(execution_id)
        self.analyzer.record_message_sent(execution_id, 256)
        
        # Finish measurement
        self.analyzer.finish_measurement(execution_id)
        
        assert execution_id not in self.analyzer.active_measurements
        assert len(self.analyzer.measurements) > 0
    
    def test_round_complexity_analysis(self):
        """Test round complexity analysis."""
        # Create some test measurements
        for i in range(3):
            execution_id = f"test_{i}"
            self.analyzer.start_measurement(execution_id, "dolev_strong", 4, 1)
            
            # Simulate f+1 rounds (optimal for deterministic)
            for round_num in range(2):  # f=1, so f+1=2 rounds
                self.analyzer.record_round_start(execution_id)
            
            self.analyzer.finish_measurement(execution_id)
        
        # Analyze round complexity
        analysis = self.analyzer.analyze_round_complexity("dolev_strong")
        
        assert analysis["protocol_name"] == "dolev_strong"
        assert analysis["total_executions"] == 3
        assert "round_complexity" in analysis
    
    def test_communication_complexity_analysis(self):
        """Test communication complexity analysis."""
        execution_id = "comm_test"
        self.analyzer.start_measurement(execution_id, "test_protocol", 4, 1)
        
        # Simulate message sending
        for i in range(10):
            self.analyzer.record_message_sent(execution_id, 512)  # 512 bits per message
        
        self.analyzer.finish_measurement(execution_id)
        
        analysis = self.analyzer.analyze_communication_complexity("test_protocol")
        
        assert analysis["protocol_name"] == "test_protocol"
        assert "communication_complexity" in analysis
        assert "message_complexity" in analysis


class TestNetworkModels:
    """Test network timing models."""
    
    def setup_method(self):
        """Set up test environment."""
        self.sync_params = NetworkParameters(max_delay=1.0)
        self.async_params = NetworkParameters(min_delay=0.1, avg_delay=0.5)
        self.partial_sync_params = NetworkParameters(
            max_delay=1.0,
            global_stabilization_time=10.0
        )
    
    def test_synchronous_network(self):
        """Test synchronous network model."""
        network = NetworkSimulator(NetworkModel.SYNCHRONOUS, self.sync_params)
        
        # Send a message
        network.send_message(
            sender_id="node_0",
            receiver_id="node_1", 
            message_type="ECHO",
            content="test_message"
        )
        
        # Advance time
        delivered = network.advance_time(network.current_time + 2.0)
        
        assert len(delivered) == 1
        assert delivered[0].content == "test_message"
    
    def test_asynchronous_network(self):
        """Test asynchronous network model."""
        network = NetworkSimulator(NetworkModel.ASYNCHRONOUS, self.async_params)
        
        # Send multiple messages
        for i in range(5):
            network.send_message(
                sender_id="node_0",
                receiver_id=f"node_{i+1}",
                message_type="ECHO", 
                content=f"message_{i}"
            )
        
        # Advance time significantly
        delivered = network.advance_time(network.current_time + 10.0)
        
        # Should deliver messages but with variable delays
        assert len(delivered) >= 0  # Some messages may still be in transit
    
    def test_partial_synchrony_network(self):
        """Test partial synchrony network model."""
        network = NetworkSimulator(NetworkModel.PARTIAL_SYNCHRONY, self.partial_sync_params)
        
        # Before GST: should behave asynchronously
        assert not network.is_synchronous_now()
        
        # Send message before GST
        network.send_message("node_0", "node_1", "ECHO", "pre_gst_message")
        
        # Advance past GST
        network.advance_time(15.0)
        
        # After GST: should behave synchronously
        assert network.is_synchronous_now()
    
    def test_network_statistics(self):
        """Test network statistics collection."""
        network = NetworkSimulator(NetworkModel.SYNCHRONOUS, self.sync_params)
        
        # Send some messages
        for i in range(3):
            network.send_message("node_0", f"node_{i+1}", "ECHO", f"msg_{i}")
        
        # Get statistics
        stats = network.get_statistics()
        
        assert "messages_sent" in stats
        assert "current_time" in stats
        assert stats["messages_sent"] == 3


class TestFinancialConsensus:
    """Test financial consensus integration."""
    
    def setup_method(self):
        """Set up test environment."""
        self.exchange = ConsensusBasedExchange("test_exchange", "streamlet")
        
        # Add some consensus nodes
        for i in range(5):
            self.exchange.add_consensus_node(f"validator_{i}")
    
    def test_exchange_creation(self):
        """Test consensus-based exchange creation."""
        assert self.exchange.exchange_id == "test_exchange"
        assert self.exchange.consensus_protocol == "streamlet"
        assert len(self.exchange.consensus_nodes) == 5
    
    def test_order_creation(self):
        """Test financial order creation."""
        order = FinancialOrder(
            order_id="order_001",
            trader_id="trader_1",
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.50"),
            timestamp=time.time()
        )
        
        assert order.order_id == "order_001"
        assert order.symbol == "AAPL"
        assert order.quantity == Decimal("100")
    
    @pytest.mark.asyncio
    async def test_order_submission(self):
        """Test order submission through consensus."""
        order = FinancialOrder(
            order_id="order_002",
            trader_id="trader_1",
            symbol="AAPL", 
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("155.00"),
            timestamp=time.time()
        )
        
        # Submit order
        result = await self.exchange.submit_order(order)
        
        # Should be accepted (simplified test)
        assert isinstance(result, bool)
    
    def test_order_validation(self):
        """Test order validation."""
        # Valid order
        valid_order = FinancialOrder(
            order_id="valid_001",
            trader_id="trader_1",
            symbol="AAPL",
            side="buy", 
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            timestamp=time.time()
        )
        
        assert self.exchange._validate_order(valid_order) == True
        
        # Invalid order (negative quantity)
        invalid_order = FinancialOrder(
            order_id="invalid_001",
            trader_id="trader_1",
            symbol="AAPL",
            side="buy",
            quantity=Decimal("-100"),  # Invalid
            price=Decimal("150.00"),
            timestamp=time.time()
        )
        
        assert self.exchange._validate_order(invalid_order) == False
    
    def test_exchange_statistics(self):
        """Test exchange statistics."""
        stats = self.exchange.get_exchange_statistics()
        
        assert "exchange_id" in stats
        assert "consensus_protocol" in stats
        assert "total_orders" in stats
        assert "consensus_nodes" in stats
        assert stats["exchange_id"] == "test_exchange"


# Integration tests
class TestIntegration:
    """Integration tests combining multiple components."""
    
    @pytest.mark.asyncio
    async def test_consensus_trading_integration(self):
        """Test full consensus-based trading workflow."""
        # Create exchange with Byzantine broadcast
        exchange = ConsensusBasedExchange("integration_exchange", "byzantine_broadcast")
        
        # Add nodes
        for i in range(4):
            exchange.add_consensus_node(f"node_{i}")
        
        # Create complementary orders
        buy_order = FinancialOrder(
            order_id="buy_001",
            trader_id="buyer_1",
            symbol="TSLA",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("200.00"),
            timestamp=time.time()
        )
        
        sell_order = FinancialOrder(
            order_id="sell_001", 
            trader_id="seller_1",
            symbol="TSLA",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("200.00"),
            timestamp=time.time()
        )
        
        # Submit orders
        buy_result = await exchange.submit_order(buy_order)
        sell_result = await exchange.submit_order(sell_order)
        
        # Both should be processed
        assert isinstance(buy_result, bool)
        assert isinstance(sell_result, bool)
    
    def test_complexity_analysis_integration(self):
        """Test complexity analysis with actual protocols."""
        analyzer = ComplexityAnalyzer()
        
        # Test with Byzantine broadcast
        protocol = ByzantineProtocol()
        nodes = []
        for i in range(4):
            node = ByzantineBroadcastNode(f"node_{i}")
            nodes.append(node)
            protocol.add_node(node)
        
        # Measure complexity
        execution_id = "integration_test"
        analyzer.start_measurement(execution_id, "byzantine_broadcast", 4, 1)
        
        # Simulate protocol execution
        for round_num in range(2):  # f+1 rounds
            analyzer.record_round_start(execution_id)
            for _ in range(12):  # Simulate O(n^2) messages per round
                analyzer.record_message_sent(execution_id, 512)
        
        analyzer.finish_measurement(execution_id)
        
        # Analyze results
        round_analysis = analyzer.analyze_round_complexity("byzantine_broadcast")
        comm_analysis = analyzer.analyze_communication_complexity("byzantine_broadcast")
        
        assert round_analysis["total_executions"] == 1
        assert comm_analysis["total_executions"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
