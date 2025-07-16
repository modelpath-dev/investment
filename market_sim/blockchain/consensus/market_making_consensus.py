"""
Market-Making Consensus Protocol.

A novel consensus mechanism that combines distributed consensus with market-making
to reach agreement on fair market prices and order execution priorities.

This protocol demonstrates how Byzantine fault-tolerant consensus can be applied
to decentralized trading systems where market makers need to agree on:
1. Fair asset prices
2. Order execution priorities  
3. Cross-exchange arbitrage opportunities
4. Liquidity provision strategies

Inspired by concepts from "Foundations of Distributed Consensus and Blockchains"
adapted for financial market coordination.
"""

from typing import Dict, List, Set, Optional, Any, Tuple, NamedTuple
from dataclasses import dataclass, field
from enum import Enum
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
import json
import statistics
import random
from .crypto import SigningKey, DigitalSignature, CollisionResistantHash, elect_leader
from .byzantine_broadcast import ByzantineBroadcastProtocol


class MarketEventType(Enum):
    """Types of market events that require consensus."""
    PRICE_DISCOVERY = "price_discovery"
    ORDER_EXECUTION = "order_execution"
    LIQUIDITY_PROVISION = "liquidity_provision"
    ARBITRAGE_OPPORTUNITY = "arbitrage_opportunity"
    MARKET_HALT = "market_halt"


@dataclass
class MarketOrder:
    """Represents a market order in the consensus system."""
    order_id: str
    symbol: str
    side: str  # 'buy' or 'sell'
    quantity: Decimal
    price: Optional[Decimal]  # None for market orders
    timestamp: datetime
    trader_id: str
    priority: int = 0  # Higher priority = earlier execution
    
    def serialize(self) -> bytes:
        """Serialize order for hashing and consensus."""
        data = {
            'order_id': self.order_id,
            'symbol': self.symbol,
            'side': self.side,
            'quantity': str(self.quantity),
            'price': str(self.price) if self.price else None,
            'timestamp': self.timestamp.isoformat(),
            'trader_id': self.trader_id,
            'priority': self.priority
        }
        return json.dumps(data, sort_keys=True).encode('utf-8')


@dataclass
class PriceQuote:
    """A price quote from a market maker."""
    symbol: str
    bid_price: Decimal
    ask_price: Decimal
    bid_size: Decimal
    ask_size: Decimal
    maker_id: str
    timestamp: datetime
    confidence: float = 1.0  # Confidence in the quote (0-1)
    
    def get_mid_price(self) -> Decimal:
        """Get the mid-market price."""
        return (self.bid_price + self.ask_price) / 2
    
    def get_spread(self) -> Decimal:
        """Get the bid-ask spread."""
        return self.ask_price - self.bid_price
    
    def serialize(self) -> bytes:
        """Serialize quote for consensus."""
        data = {
            'symbol': self.symbol,
            'bid_price': str(self.bid_price),
            'ask_price': str(self.ask_price),
            'bid_size': str(self.bid_size),
            'ask_size': str(self.ask_size),
            'maker_id': self.maker_id,
            'timestamp': self.timestamp.isoformat(),
            'confidence': self.confidence
        }
        return json.dumps(data, sort_keys=True).encode('utf-8')


@dataclass
class MarketConsensusProposal:
    """A proposal for market consensus."""
    proposal_id: str
    event_type: MarketEventType
    symbol: str
    proposer_id: str
    timestamp: datetime
    
    # Event-specific data
    price_quotes: List[PriceQuote] = field(default_factory=list)
    orders: List[MarketOrder] = field(default_factory=list)
    proposed_fair_price: Optional[Decimal] = None
    execution_priorities: Dict[str, int] = field(default_factory=dict)
    
    def serialize(self) -> bytes:
        """Serialize proposal for consensus."""
        data = {
            'proposal_id': self.proposal_id,
            'event_type': self.event_type.value,
            'symbol': self.symbol,
            'proposer_id': self.proposer_id,
            'timestamp': self.timestamp.isoformat(),
            'proposed_fair_price': str(self.proposed_fair_price) if self.proposed_fair_price else None,
            'execution_priorities': self.execution_priorities,
            'price_quotes': [q.serialize().decode('utf-8') for q in self.price_quotes],
            'orders': [o.serialize().decode('utf-8') for o in self.orders]
        }
        return json.dumps(data, sort_keys=True).encode('utf-8')


class FairPriceCalculator:
    """Calculates fair prices from multiple market maker quotes."""
    
    @staticmethod
    def volume_weighted_average_price(quotes: List[PriceQuote]) -> Decimal:
        """Calculate VWAP from multiple quotes."""
        if not quotes:
            return Decimal('0')
        
        total_value = Decimal('0')
        total_volume = Decimal('0')
        
        for quote in quotes:
            mid_price = quote.get_mid_price()
            volume = quote.bid_size + quote.ask_size
            weight = Decimal(str(quote.confidence))
            
            weighted_volume = volume * weight
            total_value += mid_price * weighted_volume
            total_volume += weighted_volume
        
        if total_volume == 0:
            return Decimal('0')
        
        return total_value / total_volume
    
    @staticmethod
    def confidence_weighted_median(quotes: List[PriceQuote]) -> Decimal:
        """Calculate confidence-weighted median price."""
        if not quotes:
            return Decimal('0')
        
        # Create weighted list of prices
        weighted_prices = []
        for quote in quotes:
            mid_price = quote.get_mid_price()
            weight = int(quote.confidence * 100)  # Scale confidence to integer weights
            weighted_prices.extend([mid_price] * weight)
        
        if not weighted_prices:
            return Decimal('0')
        
        return Decimal(str(statistics.median(weighted_prices)))
    
    @staticmethod
    def byzantine_resistant_price(quotes: List[PriceQuote], fault_tolerance: float = 0.33) -> Decimal:
        """
        Calculate a Byzantine fault-tolerant fair price.
        
        Removes outliers (potentially Byzantine quotes) and computes consensus price.
        """
        if not quotes:
            return Decimal('0')
        
        prices = [q.get_mid_price() for q in quotes]
        n = len(prices)
        
        if n <= 3:
            return FairPriceCalculator.volume_weighted_average_price(quotes)
        
        # Remove up to f Byzantine quotes (outliers)
        max_faults = int(n * fault_tolerance)
        sorted_prices = sorted(prices)
        
        # Remove extreme outliers
        if max_faults > 0:
            start_idx = max_faults // 2
            end_idx = n - (max_faults - start_idx)
            filtered_prices = sorted_prices[start_idx:end_idx]
        else:
            filtered_prices = sorted_prices
        
        if not filtered_prices:
            return Decimal('0')
        
        return Decimal(str(statistics.mean(filtered_prices)))


class MarketMakingConsensusNode:
    """
    A market maker node participating in consensus on fair prices and execution.
    
    Combines Byzantine fault tolerance with market-making strategies.
    """
    
    def __init__(self, node_id: str, total_nodes: int, symbols: List[str]):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.symbols = symbols
        self.signing_key = SigningKey(node_id)
        
        # Market making state
        self.current_quotes: Dict[str, PriceQuote] = {}  # symbol -> quote
        self.order_book: Dict[str, List[MarketOrder]] = {symbol: [] for symbol in symbols}
        self.agreed_fair_prices: Dict[str, Decimal] = {}  # symbol -> price
        self.execution_queue: List[MarketOrder] = []
        
        # Consensus state  
        self.proposals_received: Dict[str, MarketConsensusProposal] = {}
        self.votes: Dict[str, Dict[str, bool]] = {}  # proposal_id -> {node_id -> vote}
        self.consensus_results: Dict[str, Any] = {}
        
        # Byzantine behavior detection
        self.node_reputation: Dict[str, float] = {nid: 1.0 for nid in range(total_nodes)}
        self.suspicious_behavior: Dict[str, int] = {}
        
        # Trading strategy parameters
        self.risk_tolerance = random.uniform(0.5, 1.5)
        self.spread_target = Decimal(str(random.uniform(0.001, 0.01)))  # 0.1% to 1%
        self.inventory_limits = {symbol: Decimal('1000') for symbol in symbols}
        self.current_inventory = {symbol: Decimal('0') for symbol in symbols}
    
    def update_market_quote(self, symbol: str, base_price: Decimal, market_volatility: float = 0.01) -> PriceQuote:
        """
        Update this node's market quote for a symbol.
        
        Incorporates market making strategy with risk management.
        """
        # Convert volatility to Decimal for proper arithmetic
        volatility_decimal = Decimal(str(market_volatility))
        
        # Adjust spread based on volatility and inventory
        inventory_factor = abs(self.current_inventory[symbol]) / self.inventory_limits[symbol]
        adjusted_spread = self.spread_target * (Decimal('1') + inventory_factor) * (Decimal('1') + volatility_decimal)
        
        # Calculate bid/ask around fair price
        half_spread = adjusted_spread / 2
        bid_price = base_price - half_spread
        ask_price = base_price + half_spread
        
        # Size quotes based on remaining inventory capacity
        inventory_ratio = self.current_inventory[symbol] / self.inventory_limits[symbol]
        base_size = Decimal('100')
        
        # Adjust sizes based on inventory (provide more liquidity on the side that reduces inventory)
        if inventory_ratio > 0:  # Long inventory, encourage selling
            bid_size = base_size * (Decimal('1') - abs(inventory_ratio))
            ask_size = base_size * (Decimal('1') + abs(inventory_ratio))
        else:  # Short inventory, encourage buying
            bid_size = base_size * (Decimal('1') + abs(inventory_ratio))
            ask_size = base_size * (Decimal('1') - abs(inventory_ratio))
        
        # Create confidence score based on market conditions
        confidence = max(0.1, 1.0 - market_volatility - float(inventory_factor) * 0.5)
        
        quote = PriceQuote(
            symbol=symbol,
            bid_price=bid_price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
            ask_price=ask_price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
            bid_size=bid_size.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP),
            ask_size=ask_size.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP),
            maker_id=self.node_id,
            timestamp=datetime.utcnow(),
            confidence=confidence
        )
        
        self.current_quotes[symbol] = quote
        return quote
    
    def create_price_discovery_proposal(self, symbol: str, quotes: List[PriceQuote]) -> MarketConsensusProposal:
        """Create a proposal for price discovery consensus."""
        # Calculate our proposed fair price
        fair_price = FairPriceCalculator.byzantine_resistant_price(quotes)
        
        proposal = MarketConsensusProposal(
            proposal_id=f"{self.node_id}_{symbol}_{int(datetime.utcnow().timestamp())}",
            event_type=MarketEventType.PRICE_DISCOVERY,
            symbol=symbol,
            proposer_id=self.node_id,
            timestamp=datetime.utcnow(),
            price_quotes=quotes,
            proposed_fair_price=fair_price
        )
        
        return proposal
    
    def create_order_execution_proposal(self, orders: List[MarketOrder]) -> MarketConsensusProposal:
        """Create a proposal for order execution consensus."""
        # Determine execution priorities based on time, price, and size
        priorities = {}
        sorted_orders = sorted(orders, key=lambda o: (
            o.timestamp,  # Time priority
            -(o.price or Decimal('0')) if o.side == 'buy' else (o.price or Decimal('inf')),  # Price priority
            -o.quantity  # Size priority
        ))
        
        for i, order in enumerate(sorted_orders):
            priorities[order.order_id] = i
        
        symbol = orders[0].symbol if orders else "UNKNOWN"
        proposal = MarketConsensusProposal(
            proposal_id=f"{self.node_id}_exec_{int(datetime.utcnow().timestamp())}",
            event_type=MarketEventType.ORDER_EXECUTION,
            symbol=symbol,
            proposer_id=self.node_id,
            timestamp=datetime.utcnow(),
            orders=orders,
            execution_priorities=priorities
        )
        
        return proposal
    
    def evaluate_proposal(self, proposal: MarketConsensusProposal) -> bool:
        """
        Evaluate a consensus proposal and decide whether to vote for it.
        
        Implements Byzantine fault detection and rational economic behavior.
        """
        # Basic validation
        if not self._validate_proposal_format(proposal):
            return False
        
        # Check reputation of proposer
        proposer_reputation = self.node_reputation.get(proposal.proposer_id, 0.5)
        if proposer_reputation < 0.3:  # Don't trust low-reputation nodes
            return False
        
        # Event-specific evaluation
        if proposal.event_type == MarketEventType.PRICE_DISCOVERY:
            return self._evaluate_price_discovery(proposal)
        elif proposal.event_type == MarketEventType.ORDER_EXECUTION:
            return self._evaluate_order_execution(proposal)
        
        return False
    
    def _validate_proposal_format(self, proposal: MarketConsensusProposal) -> bool:
        """Basic format validation for proposals."""
        if not proposal.proposal_id or not proposal.proposer_id:
            return False
        
        if proposal.event_type == MarketEventType.PRICE_DISCOVERY:
            return len(proposal.price_quotes) > 0 and proposal.proposed_fair_price is not None
        elif proposal.event_type == MarketEventType.ORDER_EXECUTION:
            return len(proposal.orders) > 0 and len(proposal.execution_priorities) > 0
        
        return True
    
    def _evaluate_price_discovery(self, proposal: MarketConsensusProposal) -> bool:
        """Evaluate a price discovery proposal."""
        quotes = proposal.price_quotes
        proposed_price = proposal.proposed_fair_price
        
        if not quotes or not proposed_price:
            return False
        
        # Calculate our own fair price estimate
        our_fair_price = FairPriceCalculator.byzantine_resistant_price(quotes)
        
        # Check if proposed price is reasonable (within tolerance)
        price_diff_pct = abs(proposed_price - our_fair_price) / our_fair_price
        tolerance = Decimal('0.05')  # 5% tolerance
        
        if price_diff_pct > tolerance:
            # Mark proposer as potentially suspicious
            self._mark_suspicious_behavior(proposal.proposer_id)
            return False
        
        # Check quote quality and consistency
        avg_spread = sum(q.get_spread() for q in quotes) / len(quotes)
        max_reasonable_spread = our_fair_price * Decimal('0.02')  # 2% max spread
        
        if avg_spread > max_reasonable_spread:
            return False
        
        return True
    
    def _evaluate_order_execution(self, proposal: MarketConsensusProposal) -> bool:
        """Evaluate an order execution proposal."""
        orders = proposal.orders
        priorities = proposal.execution_priorities
        
        if len(orders) != len(priorities):
            return False
        
        # Check if priorities follow reasonable rules (time, price, size)
        time_sorted = sorted(orders, key=lambda o: o.timestamp)
        
        # Verify that earlier orders generally have higher priority (lower numbers)
        for i in range(len(time_sorted) - 1):
            order1, order2 = time_sorted[i], time_sorted[i + 1]
            priority1 = priorities.get(order1.order_id, float('inf'))
            priority2 = priorities.get(order2.order_id, float('inf'))
            
            # Allow some flexibility for price-time priority
            if priority1 > priority2 + 2:  # Too much deviation from time priority
                self._mark_suspicious_behavior(proposal.proposer_id)
                return False
        
        return True
    
    def _mark_suspicious_behavior(self, node_id: str) -> None:
        """Mark a node as exhibiting suspicious behavior."""
        if node_id not in self.suspicious_behavior:
            self.suspicious_behavior[node_id] = 0
        
        self.suspicious_behavior[node_id] += 1
        
        # Update reputation
        if node_id in self.node_reputation:
            self.node_reputation[node_id] *= 0.9  # Decrease reputation
            self.node_reputation[node_id] = max(0.1, self.node_reputation[node_id])
    
    def process_consensus_result(self, proposal_id: str, result: Dict[str, Any]) -> None:
        """Process the result of a consensus round."""
        if proposal_id not in self.proposals_received:
            return
        
        proposal = self.proposals_received[proposal_id]
        
        if proposal.event_type == MarketEventType.PRICE_DISCOVERY:
            # Update agreed fair price
            if 'agreed_price' in result:
                self.agreed_fair_prices[proposal.symbol] = result['agreed_price']
                
                # Update our quotes based on new fair price
                self.update_market_quote(proposal.symbol, result['agreed_price'])
        
        elif proposal.event_type == MarketEventType.ORDER_EXECUTION:
            # Update execution queue
            if 'execution_order' in result:
                self.execution_queue = result['execution_order']
                
                # Execute orders that affect our inventory
                self._execute_orders(self.execution_queue)
        
        self.consensus_results[proposal_id] = result
    
    def _execute_orders(self, orders: List[MarketOrder]) -> None:
        """Execute orders and update inventory."""
        for order in orders:
            if order.symbol in self.current_inventory:
                if order.side == 'buy':
                    self.current_inventory[order.symbol] += order.quantity
                else:  # sell
                    self.current_inventory[order.symbol] -= order.quantity
    
    def get_trading_metrics(self) -> Dict[str, Any]:
        """Get trading and consensus metrics for this node."""
        total_suspicious = sum(self.suspicious_behavior.values())
        avg_reputation = sum(self.node_reputation.values()) / len(self.node_reputation)
        
        return {
            'node_id': self.node_id,
            'current_inventory': {k: str(v) for k, v in self.current_inventory.items()},
            'current_quotes': {k: {
                'bid': str(v.bid_price), 'ask': str(v.ask_price),
                'spread': str(v.get_spread()), 'confidence': v.confidence
            } for k, v in self.current_quotes.items()},
            'agreed_fair_prices': {k: str(v) for k, v in self.agreed_fair_prices.items()},
            'suspicious_behaviors_detected': total_suspicious,
            'average_node_reputation': avg_reputation,
            'consensus_participations': len(self.consensus_results)
        }


class MarketMakingConsensusProtocol:
    """
    Orchestrates market-making consensus among multiple nodes.
    
    Combines Byzantine Broadcast for proposal dissemination with specialized
    market evaluation logic for financial consensus.
    """
    
    def __init__(self, node_ids: List[str], symbols: List[str], max_faults: int = None):
        self.node_ids = node_ids
        self.symbols = symbols
        self.total_nodes = len(node_ids)
        self.max_faults = max_faults or (self.total_nodes - 1) // 3
        
        # Create market making nodes
        self.nodes: Dict[str, MarketMakingConsensusNode] = {}
        for node_id in node_ids:
            self.nodes[node_id] = MarketMakingConsensusNode(
                node_id, self.total_nodes, symbols
            )
        
        # Byzantine Broadcast for proposal dissemination
        self.bb_protocol = ByzantineBroadcastProtocol(node_ids, self.max_faults)
        
        # Market simulation state
        self.market_prices: Dict[str, Decimal] = {
            symbol: Decimal(str(100 + random.uniform(-10, 10))) for symbol in symbols
        }
        self.market_volatility: Dict[str, float] = {
            symbol: random.uniform(0.01, 0.05) for symbol in symbols
        }
        
        # Consensus tracking
        self.active_proposals: Dict[str, MarketConsensusProposal] = {}
        self.consensus_history: List[Dict[str, Any]] = []
    
    def simulate_market_round(self) -> Dict[str, Any]:
        """
        Simulate one round of market making with consensus.
        
        Returns statistics about the round.
        """
        round_stats = {
            'timestamp': datetime.utcnow().isoformat(),
            'price_discoveries': 0,
            'order_executions': 0,
            'consensus_achieved': {},
            'market_prices': {},
            'node_metrics': {}
        }
        
        # Step 1: Update market prices (simulate external market movement)
        for symbol in self.symbols:
            volatility = self.market_volatility[symbol]
            price_change = Decimal(str(random.gauss(0, volatility))) * self.market_prices[symbol]
            self.market_prices[symbol] += price_change
            self.market_prices[symbol] = max(Decimal('1'), self.market_prices[symbol])
            round_stats['market_prices'][symbol] = str(self.market_prices[symbol])
        
        # Step 2: Generate quotes from all market makers
        for symbol in self.symbols:
            quotes = []
            base_price = self.market_prices[symbol]
            volatility = self.market_volatility[symbol]
            
            for node in self.nodes.values():
                quote = node.update_market_quote(symbol, base_price, volatility)
                quotes.append(quote)
            
            # Step 3: Create price discovery proposal
            leader_id = self.node_ids[random.randint(0, self.total_nodes - 1)]
            leader = self.nodes[leader_id]
            proposal = leader.create_price_discovery_proposal(symbol, quotes)
            
            # Step 4: Run consensus on the proposal
            consensus_result = self._run_proposal_consensus(proposal)
            round_stats['consensus_achieved'][symbol] = consensus_result['success']
            
            if consensus_result['success']:
                round_stats['price_discoveries'] += 1
                
                # Process consensus result in all nodes
                for node in self.nodes.values():
                    node.process_consensus_result(proposal.proposal_id, consensus_result)
        
        # Step 5: Generate some random orders and run execution consensus
        if random.random() < 0.3:  # 30% chance of order execution round
            orders = self._generate_random_orders()
            if orders:
                leader_id = self.node_ids[random.randint(0, self.total_nodes - 1)]
                leader = self.nodes[leader_id]
                execution_proposal = leader.create_order_execution_proposal(orders)
                
                consensus_result = self._run_proposal_consensus(execution_proposal)
                if consensus_result['success']:
                    round_stats['order_executions'] += 1
                    
                    for node in self.nodes.values():
                        node.process_consensus_result(execution_proposal.proposal_id, consensus_result)
        
        # Step 6: Collect node metrics
        for node_id, node in self.nodes.items():
            round_stats['node_metrics'][node_id] = node.get_trading_metrics()
        
        self.consensus_history.append(round_stats)
        return round_stats
    
    def _run_proposal_consensus(self, proposal: MarketConsensusProposal) -> Dict[str, Any]:
        """
        Run consensus on a market proposal using Byzantine Broadcast.
        
        Returns consensus result with success flag and agreed values.
        """
        self.active_proposals[proposal.proposal_id] = proposal
        
        # Step 1: Broadcast proposal using Byzantine Broadcast
        try:
            bb_result = self.bb_protocol.broadcast(
                proposal.proposer_id, 
                proposal.serialize().decode('utf-8')
            )
        except Exception as e:
            return {'success': False, 'error': str(e)}
        
        # Step 2: Collect votes from honest nodes
        votes = {}
        for node_id, node in self.nodes.items():
            if node_id in bb_result:  # Node received the proposal
                vote = node.evaluate_proposal(proposal)
                votes[node_id] = vote
        
        # Step 3: Determine consensus result
        total_votes = len(votes)
        yes_votes = sum(votes.values())
        required_votes = (2 * total_votes + 2) // 3  # 2/3 majority
        
        consensus_achieved = yes_votes >= required_votes
        
        result = {
            'success': consensus_achieved,
            'proposal_id': proposal.proposal_id,
            'total_votes': total_votes,
            'yes_votes': yes_votes,
            'required_votes': required_votes,
            'votes': votes
        }
        
        if consensus_achieved:
            # Calculate agreed values based on proposal type
            if proposal.event_type == MarketEventType.PRICE_DISCOVERY:
                # Use Byzantine-resistant fair price calculation
                agreed_price = FairPriceCalculator.byzantine_resistant_price(proposal.price_quotes)
                result['agreed_price'] = agreed_price
                
            elif proposal.event_type == MarketEventType.ORDER_EXECUTION:
                # Sort orders by agreed priorities
                sorted_orders = sorted(proposal.orders, 
                                     key=lambda o: proposal.execution_priorities.get(o.order_id, 0))
                result['execution_order'] = sorted_orders
        
        return result
    
    def _generate_random_orders(self) -> List[MarketOrder]:
        """Generate random orders for simulation."""
        orders = []
        num_orders = random.randint(1, 5)
        
        for i in range(num_orders):
            symbol = random.choice(self.symbols)
            side = random.choice(['buy', 'sell'])
            quantity = Decimal(str(random.randint(10, 100)))
            
            # Price around current market price
            market_price = self.market_prices[symbol]
            price_offset = Decimal(str(random.uniform(-0.05, 0.05))) * market_price
            price = market_price + price_offset
            
            order = MarketOrder(
                order_id=f"order_{i}_{int(datetime.utcnow().timestamp())}",
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                timestamp=datetime.utcnow(),
                trader_id=f"trader_{random.randint(1, 10)}"
            )
            orders.append(order)
        
        return orders
    
    def run_simulation(self, num_rounds: int = 10) -> Dict[str, Any]:
        """
        Run a complete market making consensus simulation.
        
        Returns comprehensive simulation results.
        """
        print(f"Starting Market Making Consensus simulation with {self.total_nodes} nodes...")
        
        for round_num in range(num_rounds):
            print(f"Round {round_num + 1}/{num_rounds}")
            round_stats = self.simulate_market_round()
            
            # Print some progress info
            price_discoveries = round_stats['price_discoveries']
            order_executions = round_stats['order_executions']
            print(f"  Price discoveries: {price_discoveries}, Order executions: {order_executions}")
        
        # Calculate final statistics
        total_price_discoveries = sum(r['price_discoveries'] for r in self.consensus_history)
        total_order_executions = sum(r['order_executions'] for r in self.consensus_history)
        
        # Check consistency across nodes
        final_prices = {}
        consistency_check = True
        
        for symbol in self.symbols:
            prices = set()
            for node in self.nodes.values():
                if symbol in node.agreed_fair_prices:
                    prices.add(str(node.agreed_fair_prices[symbol]))
            
            if len(prices) > 1:
                consistency_check = False
            elif len(prices) == 1:
                final_prices[symbol] = next(iter(prices))
        
        return {
            'simulation_summary': {
                'total_rounds': num_rounds,
                'total_nodes': self.total_nodes,
                'symbols_traded': self.symbols,
                'max_byzantine_faults': self.max_faults,
                'total_price_discoveries': total_price_discoveries,
                'total_order_executions': total_order_executions,
                'price_consistency_achieved': consistency_check,
                'final_agreed_prices': final_prices
            },
            'round_history': self.consensus_history,
            'final_node_states': {
                node_id: node.get_trading_metrics() 
                for node_id, node in self.nodes.items()
            }
        }

# Alias for backward compatibility
MarketMakingConsensus = MarketMakingConsensusNode
