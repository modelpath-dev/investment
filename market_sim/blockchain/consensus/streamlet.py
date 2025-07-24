"""
Streamlet blockchain protocol implementation.

Implements the simple blockchain protocol from Chapter 7 of 
"Foundations of Distributed Consensus and Blockchains" adapted for financial markets.
"""

from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import time

try:
    from .crypto import SigningKey, DigitalSignature, CollisionResistantHash, elect_leader
except ImportError:
    try:
        from crypto import SigningKey, DigitalSignature, CollisionResistantHash, elect_leader
    except ImportError:
        # Fallback implementations for missing crypto module
        class SigningKey:
            def __init__(self):
                self.key_id = f"key_{hash(self) % 10000}"
            
            def sign(self, message):
                return DigitalSignature(f"sig_{hash(message) % 10000}")
        
        class DigitalSignature:
            def __init__(self, signature):
                self.signature = signature
            
            def verify(self, message, public_key):
                return True  # Simplified verification
        
        class CollisionResistantHash:
            @staticmethod
            def hash(data):
                return f"hash_{hash(str(data)) % 10000}"
        
        def elect_leader(epoch, validators):
            return validators[epoch % len(validators)] if validators else None


@dataclass
class Block:
    """
    A block in the Streamlet blockchain.
    
    Format: (parent_hash, epoch, transactions)
    """
    parent_hash: str
    epoch: int
    transactions: List[Any]
    proposer_id: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    nonce: int = 0
    
    def serialize(self) -> bytes:
        """Serialize block for hashing and signing."""
        data = {
            'parent_hash': self.parent_hash,
            'epoch': self.epoch,
            'transactions': self.transactions,
            'proposer': self.proposer_id,
            'timestamp': self.timestamp.isoformat(),
            'nonce': self.nonce
        }
        return json.dumps(data, sort_keys=True).encode('utf-8')
    
    def get_hash(self) -> str:
        """Get the hash of this block."""
        return CollisionResistantHash.hash_string(self.serialize().decode('utf-8'))
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, Block):
            return False
        return self.get_hash() == other.get_hash()
    
    def __hash__(self) -> int:
        return hash(self.get_hash())


@dataclass 
class Vote:
    """A vote on a block in Streamlet."""
    block_hash: str
    epoch: int
    voter_id: str
    signature: Optional[DigitalSignature] = None
    
    def serialize(self) -> bytes:
        """Serialize vote for signing."""
        data = {
            'block_hash': self.block_hash,
            'epoch': self.epoch,
            'voter': self.voter_id
        }
        return json.dumps(data, sort_keys=True).encode('utf-8')


@dataclass
class Notarization:
    """A notarization certificate for a block."""
    block: Block
    votes: List[Vote]
    
    def is_valid(self, total_nodes: int) -> bool:
        """Check if notarization has enough votes (≥ 2n/3)."""
        required_votes = (2 * total_nodes + 2) // 3  # Ceiling of 2n/3
        return len(self.votes) >= required_votes
    
    def verify_votes(self) -> bool:
        """Verify all votes in the notarization."""
        block_hash = self.block.get_hash()
        for vote in self.votes:
            if vote.block_hash != block_hash:
                return False
            if vote.signature and not vote.signature.verify():
                return False
        return True


class Blockchain:
    """A chain of blocks starting from genesis."""
    
    def __init__(self):
        # Genesis block
        self.genesis = Block(
            parent_hash="",
            epoch=0,
            transactions=[],
            proposer_id="genesis"
        )
        self.blocks: Dict[str, Block] = {self.genesis.get_hash(): self.genesis}
        self.chain: List[Block] = [self.genesis]
        
    def add_block(self, block: Block) -> bool:
        """Add a block to the blockchain if it extends the chain."""
        if block.parent_hash not in self.blocks:
            return False  # Parent not found
        
        # Check that epoch increases
        parent = self.blocks[block.parent_hash]
        if block.epoch <= parent.epoch:
            return False
        
        block_hash = block.get_hash()
        self.blocks[block_hash] = block
        
        # Rebuild chain from genesis
        self._rebuild_chain()
        return True
    
    def _rebuild_chain(self) -> None:
        """Rebuild the main chain from genesis."""
        self.chain = [self.genesis]
        current = self.genesis
        
        while True:
            # Find next block in chain
            next_block = None
            for block in self.blocks.values():
                if block.parent_hash == current.get_hash() and block != current:
                    if next_block is None or block.epoch < next_block.epoch:
                        next_block = block
            
            if next_block is None:
                break
                
            self.chain.append(next_block)
            current = next_block
    
    def get_tip(self) -> Block:
        """Get the tip (latest block) of the chain."""
        return self.chain[-1]
    
    def get_length(self) -> int:
        """Get the length of the chain."""
        return len(self.chain)
    
    def get_block_at_height(self, height: int) -> Optional[Block]:
        """Get block at specific height."""
        if 0 <= height < len(self.chain):
            return self.chain[height]
        return None
    
    def has_three_consecutive_epochs(self) -> Optional[int]:
        """
        Check for three adjacent blocks with consecutive epochs.
        Returns the epoch of the middle block if found, None otherwise.
        """
        if len(self.chain) < 4:  # Need at least genesis + 3 blocks
            return None
            
        for i in range(1, len(self.chain) - 2):  # Start from index 1 (skip genesis)
            b1, b2, b3 = self.chain[i], self.chain[i+1], self.chain[i+2]
            if b2.epoch == b1.epoch + 1 and b3.epoch == b2.epoch + 1:
                return b2.epoch
        
        return None


class StreamletNode:
    """
    A node participating in the Streamlet blockchain protocol.
    
    Implements the propose-vote paradigm from Chapter 7.
    """
    
    def __init__(self, node_id: str, total_nodes: int, epoch_duration: float = 1.0):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.epoch_duration = epoch_duration  # seconds
        self.signing_key = SigningKey(node_id)
        
        # Protocol state
        self.blockchain = Blockchain()
        self.notarized_chains: Dict[str, Blockchain] = {}  # hash -> blockchain
        self.votes_received: Dict[str, List[Vote]] = {}  # block_hash -> votes
        self.notarizations: Dict[str, Notarization] = {}  # block_hash -> notarization
        self.finalized_blocks: Set[str] = {self.blockchain.genesis.get_hash()}
        
        # Network and timing
        self.start_time = datetime.utcnow()
        self.current_epoch = 1
        self.pending_transactions: List[Any] = []
        
        # PKI
        self.public_keys: Dict[str, bytes] = {}
    
    def set_public_keys(self, public_keys: Dict[str, bytes]) -> None:
        """Set public keys for all nodes."""
        self.public_keys = public_keys
    
    def add_transaction(self, tx: Any) -> None:
        """Add a transaction to the pending pool."""
        self.pending_transactions.append(tx)
    
    def get_current_epoch(self) -> int:
        """Get the current epoch based on time."""
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        return int(elapsed // self.epoch_duration) + 1
    
    def is_leader(self, epoch: int) -> bool:
        """Check if this node is the leader for the given epoch."""
        leader_id = elect_leader(epoch, self.total_nodes, "streamlet")
        return leader_id == hash(self.node_id) % self.total_nodes
    
    def propose_block(self, epoch: int) -> Optional[Block]:
        """
        Propose a block for the current epoch (if leader).
        
        From Chapter 7: "The epoch's designated leader proposes a block 
        extending from the longest notarized chain it has seen."
        """
        if not self.is_leader(epoch):
            return None
        
        # Find longest notarized chain
        longest_chain = self._get_longest_notarized_chain()
        parent_hash = longest_chain.get_tip().get_hash()
        
        # Create new block
        block = Block(
            parent_hash=parent_hash,
            epoch=epoch,
            transactions=self.pending_transactions.copy(),
            proposer_id=self.node_id
        )
        
        # Clear pending transactions
        self.pending_transactions.clear()
        
        return block
    
    def process_block_proposal(self, block: Block) -> Optional[Vote]:
        """
        Process a block proposal and potentially vote for it.
        
        From Chapter 7: "Every node casts votes on the first proposal from 
        the epoch's leader, as long as the proposed block extends from 
        (one of) the longest notarized chain(s) it has seen."
        """
        # Verify proposer is the epoch leader
        if not self._verify_proposer(block):
            return None
        
        # Check if block extends from longest notarized chain
        longest_chain = self._get_longest_notarized_chain()
        if block.parent_hash != longest_chain.get_tip().get_hash():
            return None
        
        # Create and sign vote
        vote = Vote(
            block_hash=block.get_hash(),
            epoch=block.epoch,
            voter_id=self.node_id
        )
        vote.signature = self.signing_key.sign(vote.serialize())
        
        # Add to our own vote collection
        self._add_vote(vote)
        
        return vote
    
    def process_vote(self, vote: Vote) -> Optional[Notarization]:
        """
        Process a vote and check if block becomes notarized.
        
        From Chapter 7: "When a block gains votes from at least 2n/3 
        distinct nodes, it becomes notarized."
        """
        self._add_vote(vote)
        
        # Check if we have enough votes for notarization
        block_hash = vote.block_hash
        if block_hash in self.votes_received:
            votes = self.votes_received[block_hash]
            required_votes = (2 * self.total_nodes + 2) // 3
            
            if len(votes) >= required_votes and block_hash not in self.notarizations:
                # Find the block
                block = self._find_block(block_hash)
                if block:
                    notarization = Notarization(block=block, votes=votes.copy())
                    self.notarizations[block_hash] = notarization
                    self._update_notarized_chains(notarization)
                    return notarization
        
        return None
    
    def check_finalization(self) -> List[Block]:
        """
        Check for newly finalized blocks.
        
        From Chapter 7: "If in any notarized chain, there are three adjacent 
        blocks with consecutive epoch numbers, the prefix up to the second 
        of the triple is considered final."
        """
        newly_finalized = []
        
        for chain in self.notarized_chains.values():
            middle_epoch = chain.has_three_consecutive_epochs()
            if middle_epoch is not None:
                # Finalize up to the middle block
                for i, block in enumerate(chain.chain):
                    if block.epoch == middle_epoch:
                        # Finalize all blocks up to and including this one
                        for j in range(i + 1):
                            block_hash = chain.chain[j].get_hash()
                            if block_hash not in self.finalized_blocks:
                                self.finalized_blocks.add(block_hash)
                                newly_finalized.append(chain.chain[j])
                        break
        
        return newly_finalized
    
    def _verify_proposer(self, block: Block) -> bool:
        """Verify that the block proposer is the epoch leader."""
        return self.is_leader(block.epoch)
    
    def _get_longest_notarized_chain(self) -> Blockchain:
        """Get the longest notarized chain."""
        if not self.notarized_chains:
            return self.blockchain  # Return genesis chain
        
        longest = None
        max_length = 0
        
        for chain in self.notarized_chains.values():
            if chain.get_length() > max_length:
                max_length = chain.get_length()
                longest = chain
        
        return longest if longest else self.blockchain
    
    def _add_vote(self, vote: Vote) -> None:
        """Add a vote to the collection."""
        block_hash = vote.block_hash
        if block_hash not in self.votes_received:
            self.votes_received[block_hash] = []
        
        # Check if vote already exists from this voter
        for existing_vote in self.votes_received[block_hash]:
            if existing_vote.voter_id == vote.voter_id:
                return  # Duplicate vote
        
        self.votes_received[block_hash].append(vote)
    
    def _find_block(self, block_hash: str) -> Optional[Block]:
        """Find a block by its hash across all chains."""
        # Check main blockchain
        if block_hash in self.blockchain.blocks:
            return self.blockchain.blocks[block_hash]
        
        # Check notarized chains
        for chain in self.notarized_chains.values():
            if block_hash in chain.blocks:
                return chain.blocks[block_hash]
        
        return None
    
    def _update_notarized_chains(self, notarization: Notarization) -> None:
        """Update the collection of notarized chains."""
        block = notarization.block
        block_hash = block.get_hash()
        
        # Create new chain or extend existing one
        if block.parent_hash == self.blockchain.genesis.get_hash():
            # New chain from genesis
            new_chain = Blockchain()
            new_chain.add_block(block)
            self.notarized_chains[block_hash] = new_chain
        else:
            # Extend existing chain
            parent_chain = None
            for chain in self.notarized_chains.values():
                if block.parent_hash in chain.blocks:
                    parent_chain = chain
                    break
            
            if parent_chain:
                # Create new chain as extension
                new_chain = Blockchain()
                for b in parent_chain.chain:
                    new_chain.add_block(b)
                new_chain.add_block(block)
                self.notarized_chains[block_hash] = new_chain
    
    def get_finalized_chain(self) -> List[Block]:
        """Get the finalized portion of the blockchain."""
        finalized = []
        for chain in self.notarized_chains.values():
            for block in chain.chain:
                if block.get_hash() in self.finalized_blocks:
                    finalized.append(block)
        
        # Sort by epoch
        finalized.sort(key=lambda b: b.epoch)
        return finalized


class StreamletProtocol:
    """
    Orchestrates the Streamlet blockchain protocol among multiple nodes.
    
    Implements the network simulation for the Streamlet consensus algorithm.
    """
    
    def __init__(self, node_ids: List[str], epoch_duration: float = 1.0):
        self.node_ids = node_ids
        self.total_nodes = len(node_ids)
        self.epoch_duration = epoch_duration
        
        # Create nodes
        self.nodes: Dict[str, StreamletNode] = {}
        for node_id in node_ids:
            self.nodes[node_id] = StreamletNode(
                node_id, self.total_nodes, epoch_duration
            )
        
        # Set up PKI
        self._setup_pki()
        
        # Simulation state
        self.message_queue: List[Tuple[List[str], Any]] = []  # (recipients, message)
        self.current_epoch = 1
        self.finalized_transactions: List[Any] = []
    
    def _setup_pki(self) -> None:
        """Set up the Public Key Infrastructure."""
        public_keys = {}
        for node_id, node in self.nodes.items():
            public_keys[node_id] = node.signing_key.get_public_key_pem()
        
        for node in self.nodes.values():
            node.set_public_keys(public_keys)
    
    def add_transaction(self, tx: Any) -> None:
        """Add a transaction to all nodes' pending pools."""
        for node in self.nodes.values():
            node.add_transaction(tx)
    
    def run_epoch(self, epoch: int) -> Dict[str, Any]:
        """
        Run a single epoch of the Streamlet protocol.
        
        Returns statistics about the epoch.
        """
        stats = {
            'epoch': epoch,
            'proposals': 0,
            'votes': 0,
            'notarizations': 0,
            'finalizations': 0
        }
        
        # Phase 1: Leader proposes block
        leader_id = self.node_ids[elect_leader(epoch, self.total_nodes, "streamlet")]
        leader = self.nodes[leader_id]
        
        proposed_block = leader.propose_block(epoch)
        if proposed_block:
            stats['proposals'] = 1
            # Broadcast proposal to all nodes
            self.message_queue.append((self.node_ids, ('proposal', proposed_block)))
        
        # Phase 2: Process proposals and collect votes
        votes = []
        while self.message_queue:
            recipients, (msg_type, content) = self.message_queue.pop(0)
            
            if msg_type == 'proposal':
                block = content
                for node_id in recipients:
                    if node_id in self.nodes:
                        vote = self.nodes[node_id].process_block_proposal(block)
                        if vote:
                            votes.append((node_id, vote))
        
        stats['votes'] = len(votes)
        
        # Phase 3: Process votes and check for notarizations
        notarizations = []
        for node_id, vote in votes:
            # Broadcast vote to all nodes
            for recipient_id in self.node_ids:
                if recipient_id in self.nodes:
                    notarization = self.nodes[recipient_id].process_vote(vote)
                    if notarization and notarization not in notarizations:
                        notarizations.append(notarization)
        
        stats['notarizations'] = len(notarizations)
        
        # Phase 4: Check for finalizations
        all_finalizations = []
        for node in self.nodes.values():
            finalizations = node.check_finalization()
            all_finalizations.extend(finalizations)
        
        stats['finalizations'] = len(all_finalizations)
        
        # Update finalized transactions
        for block in all_finalizations:
            self.finalized_transactions.extend(block.transactions)
        
        return stats
    
    def run_protocol(self, num_epochs: int = 10) -> Dict[str, Any]:
        """
        Run the Streamlet protocol for multiple epochs.
        
        Returns comprehensive statistics about the protocol execution.
        """
        epoch_stats = []
        
        for epoch in range(1, num_epochs + 1):
            stats = self.run_epoch(epoch)
            epoch_stats.append(stats)
            time.sleep(0.1)  # Small delay for simulation
        
        # Collect final statistics
        finalized_chains = {}
        for node_id, node in self.nodes.items():
            finalized_chains[node_id] = [
                {'epoch': b.epoch, 'hash': b.get_hash()[:8], 'txs': len(b.transactions)}
                for b in node.get_finalized_chain()
            ]
        
        # Check consistency
        chain_hashes = set()
        for chain in finalized_chains.values():
            chain_hash = tuple(block['hash'] for block in chain)
            chain_hashes.add(chain_hash)
        
        return {
            'total_epochs': num_epochs,
            'epoch_statistics': epoch_stats,
            'finalized_chains': finalized_chains,
            'consistency_achieved': len(chain_hashes) <= 1,
            'total_finalized_transactions': len(self.finalized_transactions),
            'finalized_transactions': self.finalized_transactions
        }
