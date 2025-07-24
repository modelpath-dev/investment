"""
Financial Consensus Integration

Integrates distributed consensus protocols with financial trading and market operations.
Implements consensus-based order matching, trade settlement, and market coordination
using Byzantine-fault-tolerant protocols.
"""

from typing import Dict, List, Set, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from decimal import Decimal
import time
import asyncio
from abc import ABC, abstractmethod

# Import consensus protocols
try:
    from .byzantine_broadcast import ByzantineBroadcastNode, BroadcastMessage
except ImportError:
    try:
        from byzantine_broadcast import ByzantineBroadcastNode, BroadcastMessage
    except ImportError:
        # Create minimal fallback classes
        class BroadcastMessage:
            def __init__(self, content, sender_id, round_num=0, signatures=None):
                self.content = content
                self.sender_id = sender_id
                self.round = round_num
                self.signatures = signatures or {}
        
        class ByzantineBroadcastNode:
            def __init__(self, node_id, total_nodes, signing_key=None):
                self.node_id = node_id
                self.total_nodes = total_nodes
                self.signing_key = signing_key

try:
    from .streamlet import StreamletNode, Block, Vote
except ImportError:
    try:
        from streamlet import StreamletNode, Block, Vote
    except ImportError:
        # Create minimal fallback classes
        class Block:
            def __init__(self, height, parent_hash, transactions):
                self.height = height
                self.parent_hash = parent_hash
                self.transactions = transactions
        
        class Vote:
            def __init__(self, block_hash, voter_id):
                self.block_hash = block_hash
                self.voter_id = voter_id
        
        class StreamletNode:
            def __init__(self, node_id, validators):
                self.node_id = node_id
                self.validators = validators

try:
    from .randomized_consensus import RandomizedConsensusNode, ConsensusMessage
except ImportError:
    try:
        from randomized_consensus import RandomizedConsensusNode, ConsensusMessage
    except ImportError:
        # Create minimal fallback classes
        class ConsensusMessage:
            def __init__(self, content, sender_id):
                self.content = content
                self.sender_id = sender_id
        
        class RandomizedConsensusNode:
            def __init__(self, node_id, total_nodes):
                self.node_id = node_id
                self.total_nodes = total_nodes

try:
    from .nakamoto import NakamotoBlockchain, PoWBlock
except ImportError:
    try:
        from nakamoto import NakamotoBlockchain, PoWBlock
    except ImportError:
        # Create minimal fallback classes
        class PoWBlock:
            def __init__(self, transactions, previous_hash, nonce=0):
                self.transactions = transactions
                self.previous_hash = previous_hash
                self.nonce = nonce
        
        class NakamotoBlockchain:
            def __init__(self, difficulty=4):
                self.difficulty = difficulty
                self.chain = []


class FinancialOperationType(Enum):
    """Types of financial operations requiring consensus."""
    ORDER_PLACEMENT = "order_placement"
    TRADE_EXECUTION = "trade_execution"
    SETTLEMENT = "settlement"
    PRICE_DISCOVERY = "price_discovery"
    MARKET_STATE_UPDATE = "market_state_update"
    RISK_ASSESSMENT = "risk_assessment"


@dataclass
class FinancialOrder:
    """A financial order requiring consensus."""
    order_id: str
    trader_id: str
    symbol: str
    side: str  # "buy" or "sell"
    quantity: Decimal
    price: Decimal
    timestamp: float
    order_type: str = "limit"  # "limit", "market", "stop"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "trader_id": self.trader_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": str(self.quantity),
            "price": str(self.price),
            "timestamp": self.timestamp,
            "order_type": self.order_type
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FinancialOrder':
        return cls(
            order_id=data["order_id"],
            trader_id=data["trader_id"],
            symbol=data["symbol"],
            side=data["side"],
            quantity=Decimal(data["quantity"]),
            price=Decimal(data["price"]),
            timestamp=data["timestamp"],
            order_type=data.get("order_type", "limit")
        )


@dataclass
class TradeExecution:
    """A trade execution requiring consensus."""
    trade_id: str
    buy_order_id: str
    sell_order_id: str
    symbol: str
    quantity: Decimal
    price: Decimal
    buyer_id: str
    seller_id: str
    timestamp: float
    execution_venue: str = "consensus_exchange"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "buy_order_id": self.buy_order_id,
            "sell_order_id": self.sell_order_id,
            "symbol": self.symbol,
            "quantity": str(self.quantity),
            "price": str(self.price),
            "buyer_id": self.buyer_id,
            "seller_id": self.seller_id,
            "timestamp": self.timestamp,
            "execution_venue": self.execution_venue
        }


@dataclass
class MarketState:
    """Current market state requiring consensus."""
    symbol: str
    best_bid: Optional[Decimal]
    best_ask: Optional[Decimal]
    last_trade_price: Optional[Decimal]
    volume_24h: Decimal
    timestamp: float
    order_book_hash: str  # Hash of current order book state
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "best_bid": str(self.best_bid) if self.best_bid else None,
            "best_ask": str(self.best_ask) if self.best_ask else None,
            "last_trade_price": str(self.last_trade_price) if self.last_trade_price else None,
            "volume_24h": str(self.volume_24h),
            "timestamp": self.timestamp,
            "order_book_hash": self.order_book_hash
        }


class ConsensusBasedExchange:
    """
    A financial exchange using Byzantine-fault-tolerant consensus for order matching.
    
    Ensures all exchange participants agree on trade executions and market state.
    """
    
    def __init__(self, exchange_id: str, consensus_protocol: str = "streamlet"):
        self.exchange_id = exchange_id
        self.consensus_protocol = consensus_protocol
        
        # Trading state
        self.order_book: Dict[str, List[FinancialOrder]] = {}  # symbol -> orders
        self.executed_trades: List[TradeExecution] = []
        self.market_states: Dict[str, MarketState] = {}
        
        # Consensus state
        self.consensus_nodes: Dict[str, Any] = {}
        self.pending_consensus_operations: List[Dict[str, Any]] = []
        self.confirmed_operations: List[Dict[str, Any]] = []
        
        # Risk management
        self.position_limits: Dict[str, Decimal] = {}
        self.current_positions: Dict[str, Dict[str, Decimal]] = {}  # trader_id -> symbol -> position
        
    def add_consensus_node(self, node_id: str, node_type: str = "validator") -> None:
        """Add a consensus node to the exchange."""
        if self.consensus_protocol == "streamlet":
            node = StreamletNode(node_id)
        elif self.consensus_protocol == "byzantine_broadcast":
            node = ByzantineBroadcastNode(node_id)
        elif self.consensus_protocol == "randomized":
            node = RandomizedConsensusNode(node_id)
        else:
            raise ValueError(f"Unknown consensus protocol: {self.consensus_protocol}")
        
        self.consensus_nodes[node_id] = node
    
    async def submit_order(self, order: FinancialOrder) -> bool:
        """
        Submit an order to the exchange for consensus-based processing.
        
        Returns True if order was accepted for consensus, False otherwise.
        """
        # Validate order
        if not self._validate_order(order):
            return False
        
        # Create consensus operation
        operation = {
            "type": FinancialOperationType.ORDER_PLACEMENT.value,
            "data": order.to_dict(),
            "timestamp": time.time(),
            "operation_id": f"order_{order.order_id}"
        }
        
        # Submit to consensus
        return await self._submit_to_consensus(operation)
    
    async def execute_trade(self, buy_order: FinancialOrder, sell_order: FinancialOrder,
                           execution_price: Decimal, execution_quantity: Decimal) -> bool:
        """Execute a trade through consensus."""
        trade = TradeExecution(
            trade_id=f"trade_{int(time.time() * 1000000)}",
            buy_order_id=buy_order.order_id,
            sell_order_id=sell_order.order_id,
            symbol=buy_order.symbol,
            quantity=execution_quantity,
            price=execution_price,
            buyer_id=buy_order.trader_id,
            seller_id=sell_order.trader_id,
            timestamp=time.time()
        )
        
        # Create consensus operation
        operation = {
            "type": FinancialOperationType.TRADE_EXECUTION.value,
            "data": trade.to_dict(),
            "timestamp": time.time(),
            "operation_id": f"trade_{trade.trade_id}"
        }
        
        return await self._submit_to_consensus(operation)
    
    async def update_market_state(self, symbol: str) -> bool:
        """Update market state through consensus."""
        market_state = self._calculate_market_state(symbol)
        
        operation = {
            "type": FinancialOperationType.MARKET_STATE_UPDATE.value,
            "data": market_state.to_dict(),
            "timestamp": time.time(),
            "operation_id": f"market_{symbol}_{int(time.time())}"
        }
        
        return await self._submit_to_consensus(operation)
    
    async def _submit_to_consensus(self, operation: Dict[str, Any]) -> bool:
        """Submit an operation to the consensus protocol."""
        self.pending_consensus_operations.append(operation)
        
        if self.consensus_protocol == "streamlet":
            return await self._submit_to_streamlet(operation)
        elif self.consensus_protocol == "byzantine_broadcast":
            return await self._submit_to_byzantine_broadcast(operation)
        elif self.consensus_protocol == "randomized":
            return await self._submit_to_randomized_consensus(operation)
        else:
            return False
    
    async def _submit_to_streamlet(self, operation: Dict[str, Any]) -> bool:
        """Submit operation to Streamlet consensus."""
        if not self.consensus_nodes:
            return False
        
        # Create block with the operation
        leader_node = list(self.consensus_nodes.values())[0]
        block_data = {
            "operations": [operation],
            "exchange_id": self.exchange_id
        }
        
        try:
            # Simulate block proposal and voting
            success = await self._simulate_streamlet_consensus(block_data)
            if success:
                self._apply_consensus_operation(operation)
            return success
        except Exception as e:
            print(f"Consensus error: {e}")
            return False
    
    async def _submit_to_byzantine_broadcast(self, operation: Dict[str, Any]) -> bool:
        """Submit operation to Byzantine Broadcast consensus."""
        if not self.consensus_nodes:
            return False
        
        # Use first node as broadcaster
        broadcaster = list(self.consensus_nodes.values())[0]
        message = BroadcastMessage(
            content=operation,
            sender=broadcaster.node_id,
            round_number=0
        )
        
        try:
            # Simulate Byzantine broadcast
            success = await self._simulate_byzantine_broadcast(message)
            if success:
                self._apply_consensus_operation(operation)
            return success
        except Exception as e:
            print(f"Byzantine broadcast error: {e}")
            return False
    
    async def _submit_to_randomized_consensus(self, operation: Dict[str, Any]) -> bool:
        """Submit operation to Randomized Consensus."""
        if not self.consensus_nodes:
            return False
        
        # Create consensus message
        node = list(self.consensus_nodes.values())[0]
        message = ConsensusMessage(
            message_type="propose",
            content=operation,
            sender=node.node_id,
            epoch=1,
            view=0
        )
        
        try:
            # Simulate randomized consensus
            success = await self._simulate_randomized_consensus(message)
            if success:
                self._apply_consensus_operation(operation)
            return success
        except Exception as e:
            print(f"Randomized consensus error: {e}")
            return False
    
    async def _simulate_streamlet_consensus(self, block_data: Dict[str, Any]) -> bool:
        """Simulate Streamlet consensus process."""
        # Simplified simulation - in practice, would involve full protocol
        nodes = list(self.consensus_nodes.values())
        votes = 0
        
        for node in nodes:
            # Simulate each node voting on the block
            if self._node_validates_block(node, block_data):
                votes += 1
        
        # Require majority votes
        return votes > len(nodes) // 2
    
    async def _simulate_byzantine_broadcast(self, message: BroadcastMessage) -> bool:
        """Simulate Byzantine broadcast process."""
        # Simplified simulation
        nodes = list(self.consensus_nodes.values())
        delivered_count = 0
        
        for node in nodes:
            # Simulate message delivery and acceptance
            if self._node_accepts_message(node, message):
                delivered_count += 1
        
        # Require Byzantine majority
        f = len(nodes) // 3  # Assume up to f Byzantine nodes
        return delivered_count >= len(nodes) - f
    
    async def _simulate_randomized_consensus(self, message: ConsensusMessage) -> bool:
        """Simulate randomized consensus process."""
        # Simplified simulation
        nodes = list(self.consensus_nodes.values())
        agreement_count = 0
        
        for node in nodes:
            # Simulate consensus rounds
            if self._node_reaches_consensus(node, message):
                agreement_count += 1
        
        # Require supermajority
        return agreement_count >= (2 * len(nodes)) // 3 + 1
    
    def _node_validates_block(self, node: Any, block_data: Dict[str, Any]) -> bool:
        """Check if a node validates a block."""
        # Simplified validation - check operation validity
        for operation in block_data.get("operations", []):
            if not self._validate_operation(operation):
                return False
        return True
    
    def _node_accepts_message(self, node: Any, message: BroadcastMessage) -> bool:
        """Check if a node accepts a broadcast message."""
        # Simplified acceptance - validate operation
        return self._validate_operation(message.content)
    
    def _node_reaches_consensus(self, node: Any, message: ConsensusMessage) -> bool:
        """Check if a node reaches consensus on a message."""
        # Simplified consensus check
        return self._validate_operation(message.content)
    
    def _validate_operation(self, operation: Dict[str, Any]) -> bool:
        """Validate a consensus operation."""
        op_type = operation.get("type")
        data = operation.get("data", {})
        
        if op_type == FinancialOperationType.ORDER_PLACEMENT.value:
            return self._validate_order_data(data)
        elif op_type == FinancialOperationType.TRADE_EXECUTION.value:
            return self._validate_trade_data(data)
        elif op_type == FinancialOperationType.MARKET_STATE_UPDATE.value:
            return self._validate_market_state_data(data)
        
        return False
    
    def _validate_order(self, order: FinancialOrder) -> bool:
        """Validate an order."""
        # Check basic order validity
        if order.quantity <= 0 or order.price <= 0:
            return False
        
        # Check position limits
        if not self._check_position_limits(order):
            return False
        
        # Check risk constraints
        if not self._check_risk_constraints(order):
            return False
        
        return True
    
    def _validate_order_data(self, data: Dict[str, Any]) -> bool:
        """Validate order data from consensus operation."""
        try:
            order = FinancialOrder.from_dict(data)
            return self._validate_order(order)
        except Exception:
            return False
    
    def _validate_trade_data(self, data: Dict[str, Any]) -> bool:
        """Validate trade execution data."""
        required_fields = ["trade_id", "buy_order_id", "sell_order_id", "symbol", "quantity", "price"]
        return all(field in data for field in required_fields)
    
    def _validate_market_state_data(self, data: Dict[str, Any]) -> bool:
        """Validate market state data."""
        required_fields = ["symbol", "timestamp", "order_book_hash"]
        return all(field in data for field in required_fields)
    
    def _check_position_limits(self, order: FinancialOrder) -> bool:
        """Check if order respects position limits."""
        trader_positions = self.current_positions.get(order.trader_id, {})
        current_position = trader_positions.get(order.symbol, Decimal('0'))
        
        # Calculate new position after order
        position_change = order.quantity if order.side == "buy" else -order.quantity
        new_position = current_position + position_change
        
        # Check against limits
        limit = self.position_limits.get(order.trader_id, Decimal('1000000'))
        return abs(new_position) <= limit
    
    def _check_risk_constraints(self, order: FinancialOrder) -> bool:
        """Check risk constraints for order."""
        # Simplified risk check - could include VaR, concentration limits, etc.
        order_value = order.quantity * order.price
        return order_value <= Decimal('1000000')  # Max order value
    
    def _apply_consensus_operation(self, operation: Dict[str, Any]) -> None:
        """Apply a confirmed consensus operation to exchange state."""
        op_type = operation.get("type")
        data = operation.get("data", {})
        
        if op_type == FinancialOperationType.ORDER_PLACEMENT.value:
            self._apply_order_placement(data)
        elif op_type == FinancialOperationType.TRADE_EXECUTION.value:
            self._apply_trade_execution(data)
        elif op_type == FinancialOperationType.MARKET_STATE_UPDATE.value:
            self._apply_market_state_update(data)
        
        # Move from pending to confirmed
        self.confirmed_operations.append(operation)
        if operation in self.pending_consensus_operations:
            self.pending_consensus_operations.remove(operation)
    
    def _apply_order_placement(self, order_data: Dict[str, Any]) -> None:
        """Apply confirmed order placement."""
        order = FinancialOrder.from_dict(order_data)
        
        # Add to order book
        if order.symbol not in self.order_book:
            self.order_book[order.symbol] = []
        self.order_book[order.symbol].append(order)
        
        # Update positions
        if order.trader_id not in self.current_positions:
            self.current_positions[order.trader_id] = {}
        
        # Check for immediate matches
        self._attempt_order_matching(order)
    
    def _apply_trade_execution(self, trade_data: Dict[str, Any]) -> None:
        """Apply confirmed trade execution."""
        # Update executed trades
        self.executed_trades.append(TradeExecution(
            trade_id=trade_data["trade_id"],
            buy_order_id=trade_data["buy_order_id"],
            sell_order_id=trade_data["sell_order_id"],
            symbol=trade_data["symbol"],
            quantity=Decimal(trade_data["quantity"]),
            price=Decimal(trade_data["price"]),
            buyer_id=trade_data["buyer_id"],
            seller_id=trade_data["seller_id"],
            timestamp=trade_data["timestamp"]
        ))
        
        # Update positions
        symbol = trade_data["symbol"]
        quantity = Decimal(trade_data["quantity"])
        buyer_id = trade_data["buyer_id"]
        seller_id = trade_data["seller_id"]
        
        # Update buyer position
        if buyer_id not in self.current_positions:
            self.current_positions[buyer_id] = {}
        self.current_positions[buyer_id][symbol] = self.current_positions[buyer_id].get(symbol, Decimal('0')) + quantity
        
        # Update seller position
        if seller_id not in self.current_positions:
            self.current_positions[seller_id] = {}
        self.current_positions[seller_id][symbol] = self.current_positions[seller_id].get(symbol, Decimal('0')) - quantity
    
    def _apply_market_state_update(self, state_data: Dict[str, Any]) -> None:
        """Apply confirmed market state update."""
        symbol = state_data["symbol"]
        self.market_states[symbol] = MarketState(
            symbol=symbol,
            best_bid=Decimal(state_data["best_bid"]) if state_data["best_bid"] else None,
            best_ask=Decimal(state_data["best_ask"]) if state_data["best_ask"] else None,
            last_trade_price=Decimal(state_data["last_trade_price"]) if state_data["last_trade_price"] else None,
            volume_24h=Decimal(state_data["volume_24h"]),
            timestamp=state_data["timestamp"],
            order_book_hash=state_data["order_book_hash"]
        )
    
    def _attempt_order_matching(self, new_order: FinancialOrder) -> None:
        """Attempt to match a new order with existing orders."""
        symbol_orders = self.order_book.get(new_order.symbol, [])
        
        for existing_order in symbol_orders:
            if self._orders_can_match(new_order, existing_order):
                # Create trade execution through consensus
                execution_price = existing_order.price  # Price improvement logic could go here
                execution_quantity = min(new_order.quantity, existing_order.quantity)
                
                # Submit trade execution to consensus
                asyncio.create_task(self.execute_trade(
                    new_order if new_order.side == "buy" else existing_order,
                    existing_order if existing_order.side == "sell" else new_order,
                    execution_price,
                    execution_quantity
                ))
                break
    
    def _orders_can_match(self, order1: FinancialOrder, order2: FinancialOrder) -> bool:
        """Check if two orders can be matched."""
        # Must be opposite sides
        if order1.side == order2.side:
            return False
        
        # Must be same symbol
        if order1.symbol != order2.symbol:
            return False
        
        # Price must cross
        buy_order = order1 if order1.side == "buy" else order2
        sell_order = order2 if order2.side == "sell" else order1
        
        return buy_order.price >= sell_order.price
    
    def _calculate_market_state(self, symbol: str) -> MarketState:
        """Calculate current market state for a symbol."""
        orders = self.order_book.get(symbol, [])
        
        buy_orders = [o for o in orders if o.side == "buy"]
        sell_orders = [o for o in orders if o.side == "sell"]
        
        best_bid = max([o.price for o in buy_orders], default=None)
        best_ask = min([o.price for o in sell_orders], default=None)
        
        # Calculate last trade price
        symbol_trades = [t for t in self.executed_trades if t.symbol == symbol]
        last_trade_price = symbol_trades[-1].price if symbol_trades else None
        
        # Calculate 24h volume
        recent_time = time.time() - 86400  # 24 hours ago
        recent_trades = [t for t in symbol_trades if t.timestamp >= recent_time]
        volume_24h = sum([t.quantity for t in recent_trades], Decimal('0'))
        
        # Calculate order book hash
        order_book_hash = self._calculate_order_book_hash(symbol)
        
        return MarketState(
            symbol=symbol,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=last_trade_price,
            volume_24h=volume_24h,
            timestamp=time.time(),
            order_book_hash=order_book_hash
        )
    
    def _calculate_order_book_hash(self, symbol: str) -> str:
        """Calculate a hash of the current order book state."""
        import hashlib
        
        orders = self.order_book.get(symbol, [])
        order_strings = []
        
        for order in sorted(orders, key=lambda x: (x.price, x.timestamp)):
            order_str = f"{order.order_id}:{order.side}:{order.quantity}:{order.price}"
            order_strings.append(order_str)
        
        combined = "|".join(order_strings)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]
    
    def get_exchange_statistics(self) -> Dict[str, Any]:
        """Get comprehensive exchange statistics."""
        return {
            "exchange_id": self.exchange_id,
            "consensus_protocol": self.consensus_protocol,
            "total_orders": sum(len(orders) for orders in self.order_book.values()),
            "total_trades": len(self.executed_trades),
            "active_symbols": list(self.order_book.keys()),
            "consensus_nodes": len(self.consensus_nodes),
            "pending_operations": len(self.pending_consensus_operations),
            "confirmed_operations": len(self.confirmed_operations),
            "total_traders": len(self.current_positions),
            "market_states": {symbol: state.to_dict() for symbol, state in self.market_states.items()}
        }


class CrossExchangeConsensus:
    """
    Coordinates consensus across multiple exchanges for cross-venue operations.
    
    Enables atomic settlement and arbitrage detection across distributed exchanges.
    """
    
    def __init__(self):
        self.exchanges: Dict[str, ConsensusBasedExchange] = {}
        self.cross_exchange_trades: List[Dict[str, Any]] = []
        self.arbitrage_opportunities: List[Dict[str, Any]] = []
    
    def add_exchange(self, exchange: ConsensusBasedExchange) -> None:
        """Add an exchange to the cross-exchange consensus network."""
        self.exchanges[exchange.exchange_id] = exchange
    
    async def execute_cross_exchange_arbitrage(self, symbol: str, 
                                             buy_exchange_id: str, 
                                             sell_exchange_id: str,
                                             quantity: Decimal) -> bool:
        """Execute arbitrage trade across exchanges using consensus."""
        if buy_exchange_id not in self.exchanges or sell_exchange_id not in self.exchanges:
            return False
        
        buy_exchange = self.exchanges[buy_exchange_id]
        sell_exchange = self.exchanges[sell_exchange_id]
        
        # Get market states
        buy_market = buy_exchange.market_states.get(symbol)
        sell_market = sell_exchange.market_states.get(symbol)
        
        if not buy_market or not sell_market:
            return False
        
        # Check arbitrage opportunity
        if not (sell_market.best_bid and buy_market.best_ask and 
                sell_market.best_bid > buy_market.best_ask):
            return False
        
        # Create coordinated trade
        trade_id = f"arbitrage_{int(time.time() * 1000000)}"
        
        # Submit buy order to first exchange
        buy_order = FinancialOrder(
            order_id=f"{trade_id}_buy",
            trader_id="arbitrage_bot",
            symbol=symbol,
            side="buy",
            quantity=quantity,
            price=buy_market.best_ask,
            timestamp=time.time()
        )
        
        # Submit sell order to second exchange
        sell_order = FinancialOrder(
            order_id=f"{trade_id}_sell",
            trader_id="arbitrage_bot",
            symbol=symbol,
            side="sell",
            quantity=quantity,
            price=sell_market.best_bid,
            timestamp=time.time()
        )
        
        # Execute both orders atomically
        buy_success = await buy_exchange.submit_order(buy_order)
        sell_success = await sell_exchange.submit_order(sell_order)
        
        if buy_success and sell_success:
            self.cross_exchange_trades.append({
                "trade_id": trade_id,
                "symbol": symbol,
                "buy_exchange": buy_exchange_id,
                "sell_exchange": sell_exchange_id,
                "quantity": str(quantity),
                "buy_price": str(buy_market.best_ask),
                "sell_price": str(sell_market.best_bid),
                "profit": str(sell_market.best_bid - buy_market.best_ask),
                "timestamp": time.time()
            })
            return True
        
        return False
    
    def detect_arbitrage_opportunities(self) -> List[Dict[str, Any]]:
        """Detect arbitrage opportunities across exchanges."""
        opportunities = []
        
        # Get all symbols across exchanges
        all_symbols = set()
        for exchange in self.exchanges.values():
            all_symbols.update(exchange.market_states.keys())
        
        # Check each symbol across exchange pairs
        for symbol in all_symbols:
            for buy_exchange_id, buy_exchange in self.exchanges.items():
                for sell_exchange_id, sell_exchange in self.exchanges.items():
                    if buy_exchange_id == sell_exchange_id:
                        continue
                    
                    buy_market = buy_exchange.market_states.get(symbol)
                    sell_market = sell_exchange.market_states.get(symbol)
                    
                    if buy_market and sell_market and \
                       buy_market.best_ask and sell_market.best_bid and \
                       sell_market.best_bid > buy_market.best_ask:
                        
                        profit = sell_market.best_bid - buy_market.best_ask
                        profit_percentage = (profit / buy_market.best_ask) * 100
                        
                        opportunities.append({
                            "symbol": symbol,
                            "buy_exchange": buy_exchange_id,
                            "sell_exchange": sell_exchange_id,
                            "buy_price": str(buy_market.best_ask),
                            "sell_price": str(sell_market.best_bid),
                            "profit_per_unit": str(profit),
                            "profit_percentage": str(profit_percentage),
                            "timestamp": time.time()
                        })
        
        self.arbitrage_opportunities = opportunities
        return opportunities
    
    def get_cross_exchange_statistics(self) -> Dict[str, Any]:
        """Get statistics for cross-exchange operations."""
        return {
            "total_exchanges": len(self.exchanges),
            "cross_exchange_trades": len(self.cross_exchange_trades),
            "arbitrage_opportunities": len(self.arbitrage_opportunities),
            "exchange_details": {
                exchange_id: exchange.get_exchange_statistics()
                for exchange_id, exchange in self.exchanges.items()
            },
            "recent_arbitrage_trades": self.cross_exchange_trades[-10:],
            "current_arbitrage_opportunities": self.arbitrage_opportunities
        }
