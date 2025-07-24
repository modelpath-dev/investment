"""
Byzantine Broadcast implementation using the Dolev-Strong protocol.

Implements the algorithm from Chapter 3 of "Foundations of Distributed Consensus 
and Blockchains" adapted for financial market coordination.
"""

from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import time
try:
    from .crypto import SigningKey, DigitalSignature, CollisionResistantHash
except ImportError:
    # Fallback for when module is imported directly
    from crypto import SigningKey, DigitalSignature, CollisionResistantHash


class BBMessageType(Enum):
    """Types of Byzantine Broadcast messages."""
    PROPOSAL = "proposal"
    VOTE = "vote"
    ECHO = "echo"


@dataclass
class BBMessage:
    """A message in the Byzantine Broadcast protocol."""
    message_type: BBMessageType
    content: Any
    round_number: int
    sender_id: str
    signatures: List[DigitalSignature] = field(default_factory=list)
    
    def serialize(self) -> bytes:
        """Serialize message for signing."""
        data = {
            'type': self.message_type.value,
            'content': self.content,
            'round': self.round_number,
            'sender': self.sender_id
        }
        return json.dumps(data, sort_keys=True).encode('utf-8')
    
    def add_signature(self, signing_key: SigningKey) -> None:
        """Add a signature to this message."""
        signature = signing_key.sign(self.serialize())
        self.signatures.append(signature)
    
    def verify_signatures(self) -> bool:
        """Verify all signatures on this message."""
        message_bytes = self.serialize()
        for sig in self.signatures:
            if sig.message != message_bytes:
                return False
            if not sig.verify():
                return False
        return True
    
    def get_signing_nodes(self) -> Set[str]:
        """Get the set of nodes that signed this message."""
        # Extract node IDs from the public keys in signatures
        # For simplified implementation, we use the public_key field
        return {sig.public_key for sig in self.signatures}


# Alias for backward compatibility
BroadcastMessage = BBMessage


class ByzantineBroadcastNode:
    """
    A node participating in Byzantine Broadcast using Dolev-Strong protocol.
    
    Implements the f+1 round protocol from Chapter 3.
    """
    
    def __init__(self, node_id: str, total_nodes: int, max_faults: int):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.max_faults = max_faults
        self.signing_key = SigningKey(node_id)
        
        # Protocol state
        self.extracted_set: Set[Any] = set()
        self.current_round = 0
        self.messages_received: Dict[int, List[BBMessage]] = {}
        self.output_value: Optional[Any] = None
        self.protocol_complete = False
        
        # Public keys of all nodes (simulated PKI)
        self.public_keys: Dict[str, bytes] = {}
    
    def set_public_keys(self, public_keys: Dict[str, bytes]) -> None:
        """Set the public keys of all nodes (PKI setup)."""
        self.public_keys = public_keys
    
    def start_broadcast(self, value: Any) -> List[BBMessage]:
        """
        Start Byzantine Broadcast as the designated sender.
        
        Implements Round 0 of the Dolev-Strong protocol.
        """
        if self.current_round != 0:
            raise ValueError("Broadcast already started")
        
        # Add value to extracted set immediately
        self.extracted_set.add(value)
        
        # Create initial proposal
        message = BBMessage(
            message_type=BBMessageType.PROPOSAL,
            content=value,
            round_number=0,
            sender_id=self.node_id
        )
        message.add_signature(self.signing_key)
        
        self.current_round = 1
        return [message]
    
    def process_message(self, message: BBMessage) -> List[BBMessage]:
        """
        Process an incoming message and return messages to send.
        
        Implements the core Dolev-Strong algorithm from Chapter 3.
        """
        if not message.verify_signatures():
            return []  # Invalid signatures, ignore
        
        round_num = message.round_number
        if round_num not in self.messages_received:
            self.messages_received[round_num] = []
        
        self.messages_received[round_num].append(message)
        
        # Process based on message type and round
        if message.message_type == BBMessageType.PROPOSAL and round_num == 0:
            return self._process_initial_proposal(message)
        elif round_num < self.max_faults + 1:
            return self._process_dolev_strong_round(message)
        
        return []
    
    def _process_initial_proposal(self, message: BBMessage) -> List[BBMessage]:
        """Process the initial proposal from the designated sender."""
        if len(message.signatures) != 1:
            return []  # Should have exactly one signature from sender
        
        value = message.content
        if value not in self.extracted_set:
            self.extracted_set.add(value)
            
            # Forward with our signature
            new_message = BBMessage(
                message_type=BBMessageType.ECHO,
                content=value,
                round_number=1,
                sender_id=self.node_id,
                signatures=message.signatures.copy()
            )
            new_message.add_signature(self.signing_key)
            
            return [new_message]
        
        return []
    
    def _process_dolev_strong_round(self, message: BBMessage) -> List[BBMessage]:
        """
        Process messages in rounds 1 to f+1 of Dolev-Strong protocol.
        
        From Chapter 3: "For each round r = 1 to f + 1: 
        For every message ⟨b̃⟩₁,j₁,j₂,...,jᵣ₋₁ node i receives with r signatures 
        from distinct nodes including the sender:
        - If b̃ ∉ extrᵢ: add b̃ to extrᵢ and send ⟨b̃⟩₁,j₁,...,jᵣ₋₁,i to everyone"
        """
        value = message.content
        required_signatures = message.round_number
        
        # Check if message has exactly r signatures from distinct nodes
        if len(message.signatures) != required_signatures:
            return []
        
        signing_nodes = message.get_signing_nodes()
        if len(signing_nodes) != required_signatures:
            return []  # Not distinct signers
        
        # Add to extracted set if not present
        if value not in self.extracted_set:
            self.extracted_set.add(value)
            
            # Forward with our signature if not in final round
            if message.round_number < self.max_faults + 1:
                new_message = BBMessage(
                    message_type=BBMessageType.ECHO,
                    content=value,
                    round_number=message.round_number + 1,
                    sender_id=self.node_id,
                    signatures=message.signatures.copy()
                )
                new_message.add_signature(self.signing_key)
                
                return [new_message]
        
        return []
    
    def finalize_output(self) -> Any:
        """
        Finalize the output after f+1 rounds.
        
        From Chapter 3: "At the end of round f + 1: If |extrᵢ| = 1: 
        node i outputs the bit in extrᵢ; else node i outputs 0."
        """
        if self.protocol_complete:
            return self.output_value
        
        if len(self.extracted_set) == 1:
            self.output_value = next(iter(self.extracted_set))
        else:
            self.output_value = 0  # Default value
        
        self.protocol_complete = True
        return self.output_value
    
    def advance_round(self) -> None:
        """Advance to the next round."""
        self.current_round += 1
        
        # Check if protocol should terminate
        if self.current_round > self.max_faults + 1:
            self.finalize_output()


class ByzantineBroadcastProtocol:
    """
    Orchestrates Byzantine Broadcast among multiple nodes.
    
    Simulates the network and message delivery for the Dolev-Strong protocol.
    """
    
    def __init__(self, node_ids: List[str], max_faults: int):
        self.node_ids = node_ids
        self.total_nodes = len(node_ids)
        self.max_faults = max_faults
        
        if max_faults >= self.total_nodes:
            raise ValueError("Cannot tolerate f >= n faults")
        
        # Create nodes
        self.nodes: Dict[str, ByzantineBroadcastNode] = {}
        for node_id in node_ids:
            self.nodes[node_id] = ByzantineBroadcastNode(
                node_id, self.total_nodes, max_faults
            )
        
        # Set up PKI
        self._setup_pki()
        
        # Network state
        self.message_queue: List[Tuple[str, BBMessage]] = []
        self.current_round = 0
        self.protocol_complete = False
        
    def _setup_pki(self) -> None:
        """Set up the Public Key Infrastructure."""
        public_keys = {}
        for node_id, node in self.nodes.items():
            public_keys[node_id] = node.signing_key.get_public_key_pem()
        
        for node in self.nodes.values():
            node.set_public_keys(public_keys)
    
    def broadcast(self, sender_id: str, value: Any) -> Dict[str, Any]:
        """
        Execute Byzantine Broadcast with the given sender and value.
        
        Returns the outputs of all honest nodes.
        """
        if sender_id not in self.nodes:
            raise ValueError(f"Unknown sender: {sender_id}")
        
        print(f"DEBUG: Starting broadcast from {sender_id} with value: {value}")
        
        # Start the broadcast
        initial_messages = self.nodes[sender_id].start_broadcast(value)
        print(f"DEBUG: Got {len(initial_messages)} initial messages")
        
        # Add initial messages to queue (broadcast to all)
        for message in initial_messages:
            for node_id in self.node_ids:
                if node_id != sender_id:  # Don't send to self
                    self.message_queue.append((node_id, message))
        
        print(f"DEBUG: Message queue size: {len(self.message_queue)}")
        
        # Run protocol for f+1 rounds (but at least 1 round)
        max_rounds = max(1, self.max_faults + 1)
        print(f"DEBUG: Will run {max_rounds} rounds")
        
        for round_num in range(1, max_rounds + 1):
            print(f"DEBUG: Executing round {round_num}")
            self._execute_round()
            print(f"DEBUG: Round {round_num} complete, queue size: {len(self.message_queue)}")
            
            # Advance all nodes to next round
            for node in self.nodes.values():
                node.advance_round()
            
            # Early termination if no messages in queue
            if not self.message_queue:
                print(f"DEBUG: No more messages, terminating early")
                break
        
        print(f"DEBUG: All rounds complete, collecting outputs")
        
        # Collect outputs
        outputs = {}
        for node_id, node in self.nodes.items():
            outputs[node_id] = node.finalize_output()
        
        self.protocol_complete = True
        print(f"DEBUG: Broadcast complete")
        return outputs
    
    def _execute_round(self) -> None:
        """Execute one round of message processing."""
        new_messages = []
        
        # Process all messages in current queue for this round only
        current_queue = self.message_queue.copy()
        self.message_queue.clear()
        
        for recipient_id, message in current_queue:
            if recipient_id in self.nodes:
                node = self.nodes[recipient_id]
                responses = node.process_message(message)
                
                # Broadcast responses to all other nodes
                for response in responses:
                    for node_id in self.node_ids:
                        if node_id != recipient_id:
                            new_messages.append((node_id, response))
        
        # Add new messages to queue for next round
        self.message_queue.extend(new_messages)
    
    def get_protocol_stats(self) -> Dict[str, Any]:
        """Get statistics about the protocol execution."""
        if not self.protocol_complete:
            return {"status": "in_progress"}
        
        outputs = [node.output_value for node in self.nodes.values()]
        extracted_sets = [len(node.extracted_set) for node in self.nodes.values()]
        
        return {
            "status": "complete",
            "consistency_achieved": len(set(outputs)) == 1,
            "output_value": outputs[0] if len(set(outputs)) == 1 else None,
            "extracted_set_sizes": extracted_sets,
            "total_rounds": self.max_faults + 1
        }

# Alias for backward compatibility with test suite
DolevStrongBroadcast = ByzantineBroadcastNode
