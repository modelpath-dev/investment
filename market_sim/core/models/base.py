"""
Base models for market simulation entities.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Union
from uuid import UUID, uuid4
from market_sim.core.utils.time_utils import utc_now

@dataclass
class BaseEntity:
    """Base class for all market simulation entities"""
    id: UUID = None
    created_at: datetime = None
    
    def __post_init__(self):
        if self.id is None:
            self.id = uuid4()
        if self.created_at is None:
            self.created_at = utc_now()

class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    
class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"

class OrderStatus(Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

@dataclass
class Order:
    """Base class for market orders."""
    id: UUID = None
    created_at: datetime = None
    symbol: str = None
    side: OrderSide = None
    type: OrderType = None
    quantity: Decimal = None
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    status: OrderStatus = None
    filled_quantity: Decimal = None
    remaining_quantity: Decimal = None
    updated_at: datetime = None
    agent_id: str = None
    
    def __post_init__(self):
        if self.id is None:
            self.id = uuid4()
        if self.created_at is None:
            self.created_at = utc_now()
    
    @classmethod
    def create_market_order(cls, symbol: str, side: OrderSide, quantity: Decimal, agent_id: str) -> 'Order':
        now = utc_now()
        return cls(
            symbol=symbol,
            side=side,
            type=OrderType.MARKET,
            quantity=quantity,
            price=None,
            stop_price=None,
            status=OrderStatus.PENDING,
            filled_quantity=Decimal('0'),
            remaining_quantity=quantity,
            created_at=now,
            updated_at=now,
            agent_id=agent_id
        )
    
    @classmethod
    def create_limit_order(cls, symbol: str, side: OrderSide, quantity: Decimal, 
                          price: Decimal, agent_id: str) -> 'Order':
        now = utc_now()
        return cls(
            symbol=symbol,
            side=side,
            type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            stop_price=None,
            status=OrderStatus.PENDING,
            filled_quantity=Decimal('0'),
            remaining_quantity=quantity,
            created_at=now,
            updated_at=now,
            agent_id=agent_id
        )

@dataclass
class Trade:
    """Represents a completed trade between two orders."""
    id: UUID = None
    created_at: datetime = None
    symbol: str = None
    price: Decimal = None
    quantity: Decimal = None
    buyer_order_id: UUID = None
    seller_order_id: UUID = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.id is None:
            self.id = uuid4()
        if self.created_at is None:
            self.created_at = utc_now()
    
    @classmethod
    def create(cls, symbol: str, price: Decimal, quantity: Decimal, 
               buyer_order_id: UUID, seller_order_id: UUID) -> 'Trade':
        return cls(
            symbol=symbol,
            price=price,
            quantity=quantity,
            buyer_order_id=buyer_order_id,
            seller_order_id=seller_order_id,
            timestamp=datetime.utcnow()
        )

@dataclass
class OrderBook:
    """Represents the order book for a single symbol."""
    id: UUID = None
    created_at: datetime = None
    symbol: str = None
    bids: Dict[Decimal, List[Order]] = None  # Price -> Orders
    asks: Dict[Decimal, List[Order]] = None  # Price -> Orders
    last_updated: datetime = None
    
    def __post_init__(self):
        if self.id is None:
            self.id = uuid4()
        if self.created_at is None:
            self.created_at = utc_now()
        if self.bids is None:
            self.bids = {}
        if self.asks is None:
            self.asks = {}
    
    @classmethod
    def create(cls, symbol: str) -> 'OrderBook':
        return cls(
            symbol=symbol,
            bids={},
            asks={},
            last_updated=datetime.utcnow()
        )
    
    def add_order(self, order: Order) -> None:
        """Add an order to the book."""
        book = self.bids if order.side == OrderSide.BUY else self.asks
        if order.price not in book:
            book[order.price] = []
        book[order.price].append(order)
        self.last_updated = utc_now()
    
    def get_orders_at_price(self, side: OrderSide, price: Decimal) -> List[Order]:
        """Get all orders at a specific price level."""
        book = self.bids if side == OrderSide.BUY else self.asks
        return book.get(price, [])
    
    def remove_order(self, order: Order) -> None:
        """Remove an order from the book."""
        orders = self.bids if order.side == OrderSide.BUY else self.asks
        if order.price in orders:
            orders[order.price] = [o for o in orders[order.price] if o.id != order.id]
            if not orders[order.price]:
                del orders[order.price]
        self.last_updated = datetime.utcnow()

@dataclass
class Asset:
    """Base class for tradable assets."""
    id: UUID = None
    created_at: datetime = None
    symbol: str = None
    name: str = None
    asset_type: str = None
    decimals: int = None
    min_trade_size: Decimal = None
    max_trade_size: Optional[Decimal] = None
    tick_size: Decimal = None
    
    def __post_init__(self):
        if self.id is None:
            self.id = uuid4()
        if self.created_at is None:
            self.created_at = utc_now()

@dataclass
class Position:
    """Represents a trading position."""
    id: UUID = None
    created_at: datetime = None
    agent_id: str = None
    symbol: str = None
    quantity: Decimal = None
    average_entry_price: Decimal = None
    unrealized_pnl: Decimal = None
    realized_pnl: Decimal = None
    last_updated: datetime = None
    
    def __post_init__(self):
        if self.id is None:
            self.id = uuid4()
        if self.created_at is None:
            self.created_at = utc_now()
    
    @classmethod
    def create(cls, agent_id: str, symbol: str) -> 'Position':
        return cls(
            agent_id=agent_id,
            symbol=symbol,
            quantity=Decimal('0'),
            average_entry_price=Decimal('0'),
            unrealized_pnl=Decimal('0'),
            realized_pnl=Decimal('0'),
            last_updated=utc_now()
        )
    
    def update(self, trade_quantity: Decimal, trade_price: Decimal, side: OrderSide) -> None:
        """Update position after a trade."""
        if side == OrderSide.BUY:
            if self.quantity == 0:
                self.average_entry_price = trade_price
            else:
                total_cost = self.average_entry_price * self.quantity + trade_price * trade_quantity
                self.quantity += trade_quantity
                self.average_entry_price = total_cost / self.quantity
        else:  # SELL
            realized_pnl = (trade_price - self.average_entry_price) * trade_quantity
            self.realized_pnl += realized_pnl
            self.quantity -= trade_quantity
            
        self.last_updated = utc_now()