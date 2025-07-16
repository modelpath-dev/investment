"""
Visualization tools for distributed consensus protocols and financial applications.

Creates interactive visualizations and charts to demonstrate consensus protocol
behavior, complexity analysis, and financial trading scenarios.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import networkx as nx
from typing import Dict, List, Any, Optional, Tuple
import time
from decimal import Decimal

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../blockchain/consensus'))

from complexity_analysis import ComplexityAnalyzer, ComplexityMetric
from network_models import NetworkSimulator, NetworkModel
from financial_consensus import ConsensusBasedExchange


class ConsensusProtocolVisualizer:
    """Visualizes consensus protocol behavior and performance."""
    
    def __init__(self):
        """Initialize visualizer."""
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
    
    def visualize_round_complexity(self, analyzer: ComplexityAnalyzer, 
                                 protocol_names: List[str]) -> go.Figure:
        """
        Visualize round complexity comparison across protocols.
        
        Args:
            analyzer: ComplexityAnalyzer with measurement data
            protocol_names: List of protocol names to compare
            
        Returns:
            Plotly figure showing round complexity comparison
        """
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Round Complexity by Protocol", "Round Complexity Distribution",
                          "Round Complexity vs Network Size", "Round Complexity vs Faults"),
            specs=[[{"secondary_y": True}, {"type": "histogram"}],
                   [{"type": "scatter"}, {"type": "scatter"}]]
        )
        
        colors = px.colors.qualitative.Set1
        
        for i, protocol in enumerate(protocol_names):
            measurements = analyzer.get_measurements(ComplexityMetric.ROUND_COMPLEXITY, protocol)
            
            if not measurements:
                continue
            
            rounds = [m.value for m in measurements]
            nodes = [m.num_nodes for m in measurements]
            faults = [m.num_faults for m in measurements]
            
            # Bar chart of average round complexity
            avg_rounds = np.mean(rounds)
            fig.add_trace(
                go.Bar(name=f"{protocol}", x=[protocol], y=[avg_rounds],
                      marker_color=colors[i % len(colors)]),
                row=1, col=1
            )
            
            # Histogram of round complexity distribution
            fig.add_trace(
                go.Histogram(x=rounds, name=f"{protocol} dist", 
                           marker_color=colors[i % len(colors)], opacity=0.7),
                row=1, col=2
            )
            
            # Scatter plot: rounds vs network size
            fig.add_trace(
                go.Scatter(x=nodes, y=rounds, mode='markers', 
                          name=f"{protocol} vs nodes",
                          marker=dict(color=colors[i % len(colors)], size=8)),
                row=2, col=1
            )
            
            # Scatter plot: rounds vs faults
            fig.add_trace(
                go.Scatter(x=faults, y=rounds, mode='markers',
                          name=f"{protocol} vs faults", 
                          marker=dict(color=colors[i % len(colors)], size=8)),
                row=2, col=2
            )
        
        # Update layout
        fig.update_layout(
            title="Consensus Protocol Round Complexity Analysis",
            height=800,
            showlegend=True
        )
        
        fig.update_xaxes(title_text="Protocol", row=1, col=1)
        fig.update_yaxes(title_text="Average Rounds", row=1, col=1)
        
        fig.update_xaxes(title_text="Rounds", row=1, col=2)
        fig.update_yaxes(title_text="Frequency", row=1, col=2)
        
        fig.update_xaxes(title_text="Network Size (n)", row=2, col=1)
        fig.update_yaxes(title_text="Rounds", row=2, col=1)
        
        fig.update_xaxes(title_text="Faults (f)", row=2, col=2)
        fig.update_yaxes(title_text="Rounds", row=2, col=2)
        
        return fig
    
    def visualize_communication_complexity(self, analyzer: ComplexityAnalyzer,
                                         protocol_names: List[str]) -> go.Figure:
        """
        Visualize communication complexity across protocols.
        
        Args:
            analyzer: ComplexityAnalyzer with measurement data
            protocol_names: List of protocol names to compare
            
        Returns:
            Plotly figure showing communication complexity analysis
        """
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Message Complexity", "Bit Complexity",
                          "Scalability Analysis", "Efficiency Metrics"),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "scatter"}, {"type": "bar"}]]
        )
        
        colors = px.colors.qualitative.Set2
        
        protocol_data = {}
        
        for i, protocol in enumerate(protocol_names):
            msg_measurements = analyzer.get_measurements(ComplexityMetric.MESSAGE_COMPLEXITY, protocol)
            bit_measurements = analyzer.get_measurements(ComplexityMetric.COMMUNICATION_COMPLEXITY, protocol)
            
            if not msg_measurements or not bit_measurements:
                continue
            
            messages = [m.value for m in msg_measurements]
            bits = [m.value for m in bit_measurements]
            nodes = [m.num_nodes for m in msg_measurements]
            
            protocol_data[protocol] = {
                'avg_messages': np.mean(messages),
                'avg_bits': np.mean(bits),
                'messages': messages,
                'bits': bits,
                'nodes': nodes,
                'bits_per_message': np.mean(bits) / np.mean(messages) if messages else 0
            }
            
            # Message complexity bar chart
            fig.add_trace(
                go.Bar(name=f"{protocol}", x=[protocol], y=[np.mean(messages)],
                      marker_color=colors[i % len(colors)]),
                row=1, col=1
            )
            
            # Bit complexity bar chart  
            fig.add_trace(
                go.Bar(name=f"{protocol}", x=[protocol], y=[np.mean(bits)],
                      marker_color=colors[i % len(colors)], showlegend=False),
                row=1, col=2
            )
            
            # Scalability: messages vs network size
            if len(set(nodes)) > 1:  # Only if we have varying network sizes
                fig.add_trace(
                    go.Scatter(x=nodes, y=messages, mode='markers+lines',
                              name=f"{protocol} scaling",
                              marker=dict(color=colors[i % len(colors)], size=8)),
                    row=2, col=1
                )
        
        # Efficiency metrics (bits per message)
        if protocol_data:
            protocols = list(protocol_data.keys())
            efficiency = [protocol_data[p]['bits_per_message'] for p in protocols]
            
            fig.add_trace(
                go.Bar(x=protocols, y=efficiency, name="Bits per Message",
                      marker_color=colors[:len(protocols)], showlegend=False),
                row=2, col=2
            )
        
        # Update layout
        fig.update_layout(
            title="Communication Complexity Analysis",
            height=800
        )
        
        fig.update_xaxes(title_text="Protocol", row=1, col=1)
        fig.update_yaxes(title_text="Average Messages", row=1, col=1)
        
        fig.update_xaxes(title_text="Protocol", row=1, col=2)
        fig.update_yaxes(title_text="Average Bits", row=1, col=2)
        
        fig.update_xaxes(title_text="Network Size", row=2, col=1)
        fig.update_yaxes(title_text="Messages", row=2, col=1)
        
        fig.update_xaxes(title_text="Protocol", row=2, col=2)
        fig.update_yaxes(title_text="Bits per Message", row=2, col=2)
        
        return fig
    
    def visualize_network_behavior(self, network_stats: List[Dict[str, Any]]) -> go.Figure:
        """
        Visualize network timing behavior across different models.
        
        Args:
            network_stats: List of network statistics from different models
            
        Returns:
            Plotly figure showing network behavior analysis
        """
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Message Delivery Delays", "Delivery Rate by Model",
                          "Network Timeline", "GST Impact (Partial Synchrony)"),
        )
        
        colors = ['blue', 'red', 'green', 'orange']
        
        for i, stats in enumerate(network_stats):
            model_name = stats.get('model', f'Model {i}')
            
            # Message delay distribution (if available)
            if 'delays' in stats:
                delays = stats['delays']
                fig.add_trace(
                    go.Histogram(x=delays, name=f"{model_name} delays",
                               marker_color=colors[i % len(colors)], opacity=0.7),
                    row=1, col=1
                )
            
            # Delivery rate
            delivery_rate = stats.get('delivery_rate', 0)
            fig.add_trace(
                go.Bar(x=[model_name], y=[delivery_rate], name=f"{model_name}",
                      marker_color=colors[i % len(colors)]),
                row=1, col=2
            )
            
            # Network timeline (if timestamps available)
            if 'timestamps' in stats and 'events' in stats:
                timestamps = stats['timestamps']
                events = stats['events']
                fig.add_trace(
                    go.Scatter(x=timestamps, y=events, mode='markers+lines',
                              name=f"{model_name} timeline",
                              marker=dict(color=colors[i % len(colors)])),
                    row=2, col=1
                )
            
            # GST impact for partial synchrony
            if model_name.lower() == 'partial_synchrony' and 'gst_time' in stats:
                gst_time = stats['gst_time']
                pre_gst_rate = stats.get('pre_gst_delivery_rate', 0)
                post_gst_rate = stats.get('post_gst_delivery_rate', 0)
                
                fig.add_trace(
                    go.Bar(x=['Pre-GST', 'Post-GST'], y=[pre_gst_rate, post_gst_rate],
                          name="GST Impact", marker_color=['red', 'green']),
                    row=2, col=2
                )
        
        fig.update_layout(
            title="Network Model Behavior Analysis",
            height=800
        )
        
        return fig
    
    def create_consensus_network_graph(self, nodes: List[str], 
                                     byzantine_nodes: List[str] = None) -> go.Figure:
        """
        Create a network graph visualization of consensus nodes.
        
        Args:
            nodes: List of node IDs
            byzantine_nodes: List of Byzantine node IDs
            
        Returns:
            Plotly figure showing network topology
        """
        # Create network graph
        G = nx.complete_graph(len(nodes))  # Fully connected network
        pos = nx.spring_layout(G)
        
        # Prepare node traces
        node_x = []
        node_y = []
        node_colors = []
        node_text = []
        
        byzantine_nodes = byzantine_nodes or []
        
        for i, node in enumerate(nodes):
            x, y = pos[i]
            node_x.append(x)
            node_y.append(y)
            
            if node in byzantine_nodes:
                node_colors.append('red')
                node_text.append(f"{node}<br>(Byzantine)")
            else:
                node_colors.append('lightblue')
                node_text.append(f"{node}<br>(Honest)")
        
        # Prepare edge traces
        edge_x = []
        edge_y = []
        
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        # Create figure
        fig = go.Figure()
        
        # Add edges
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines',
            name='Network Links'
        ))
        
        # Add nodes
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            hoverinfo='text',
            text=[node.split('_')[-1] for node in nodes],  # Show just node number
            hovertext=node_text,
            textposition="middle center",
            marker=dict(
                size=30,
                color=node_colors,
                line=dict(width=2, color='black')
            ),
            name='Consensus Nodes'
        ))
        
        fig.update_layout(
            title="Consensus Network Topology",
            titlefont_size=16,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            annotations=[ dict(
                text="Red nodes are Byzantine, Blue nodes are Honest",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002,
                xanchor='left', yanchor='bottom',
                font=dict(color='black', size=12)
            )],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        
        return fig


class FinancialConsensusVisualizer:
    """Visualizes financial consensus applications."""
    
    def __init__(self):
        """Initialize financial visualizer."""
        plt.style.use('seaborn-v0_8')
    
    def visualize_order_book_consensus(self, exchange: ConsensusBasedExchange,
                                     symbol: str) -> go.Figure:
        """
        Visualize order book state achieved through consensus.
        
        Args:
            exchange: ConsensusBasedExchange instance
            symbol: Trading symbol to visualize
            
        Returns:
            Plotly figure showing order book
        """
        orders = exchange.order_book.get(symbol, [])
        
        if not orders:
            # Create empty figure
            fig = go.Figure()
            fig.add_annotation(
                text=f"No orders found for {symbol}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=16)
            )
            return fig
        
        # Separate buy and sell orders
        buy_orders = [o for o in orders if o.side == "buy"]
        sell_orders = [o for o in orders if o.side == "sell"]
        
        # Sort orders
        buy_orders.sort(key=lambda x: x.price, reverse=True)  # Highest price first
        sell_orders.sort(key=lambda x: x.price)  # Lowest price first
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=(f"Buy Orders ({symbol})", f"Sell Orders ({symbol})"),
            shared_yaxes=True
        )
        
        # Buy orders (bids)
        if buy_orders:
            buy_prices = [float(o.price) for o in buy_orders]
            buy_quantities = [float(o.quantity) for o in buy_orders]
            
            fig.add_trace(
                go.Bar(
                    x=[-q for q in buy_quantities],  # Negative for left side
                    y=buy_prices,
                    orientation='h',
                    name='Bids',
                    marker_color='green',
                    hovertemplate='Price: $%{y}<br>Quantity: %{x}<extra></extra>'
                ),
                row=1, col=1
            )
        
        # Sell orders (asks)
        if sell_orders:
            sell_prices = [float(o.price) for o in sell_orders]
            sell_quantities = [float(o.quantity) for o in sell_orders]
            
            fig.add_trace(
                go.Bar(
                    x=sell_quantities,  # Positive for right side
                    y=sell_prices,
                    orientation='h',
                    name='Asks',
                    marker_color='red',
                    hovertemplate='Price: $%{y}<br>Quantity: %{x}<extra></extra>'
                ),
                row=1, col=2
            )
        
        # Update layout
        fig.update_layout(
            title=f"Consensus-Based Order Book: {symbol}",
            height=600,
            showlegend=True
        )
        
        fig.update_xaxes(title_text="Quantity", row=1, col=1)
        fig.update_xaxes(title_text="Quantity", row=1, col=2)
        fig.update_yaxes(title_text="Price ($)", row=1, col=1)
        
        return fig
    
    def visualize_trade_execution_timeline(self, exchange: ConsensusBasedExchange) -> go.Figure:
        """
        Visualize trade execution timeline through consensus.
        
        Args:
            exchange: ConsensusBasedExchange instance
            
        Returns:
            Plotly figure showing trade timeline
        """
        trades = exchange.executed_trades
        
        if not trades:
            fig = go.Figure()
            fig.add_annotation(
                text="No trades executed yet",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=16)
            )
            return fig
        
        # Prepare data
        timestamps = [pd.to_datetime(t.timestamp, unit='s') for t in trades]
        prices = [float(t.price) for t in trades]
        quantities = [float(t.quantity) for t in trades]
        symbols = [t.symbol for t in trades]
        
        # Create timeline plot
        fig = go.Figure()
        
        # Group by symbol
        unique_symbols = list(set(symbols))
        colors = px.colors.qualitative.Set1
        
        for i, symbol in enumerate(unique_symbols):
            symbol_trades = [t for t in trades if t.symbol == symbol]
            symbol_times = [pd.to_datetime(t.timestamp, unit='s') for t in symbol_trades]
            symbol_prices = [float(t.price) for t in symbol_trades]
            symbol_quantities = [float(t.quantity) for t in symbol_trades]
            
            fig.add_trace(go.Scatter(
                x=symbol_times,
                y=symbol_prices,
                mode='markers+lines',
                name=symbol,
                marker=dict(
                    size=[q/10 for q in symbol_quantities],  # Size proportional to quantity
                    color=colors[i % len(colors)],
                    line=dict(width=1, color='black')
                ),
                hovertemplate=f'{symbol}<br>Price: $%{{y}}<br>Quantity: %{{text}}<br>Time: %{{x}}<extra></extra>',
                text=symbol_quantities
            ))
        
        fig.update_layout(
            title="Trade Execution Timeline (Consensus-Validated)",
            xaxis_title="Time",
            yaxis_title="Price ($)",
            height=500,
            hovermode='closest'
        )
        
        return fig
    
    def visualize_consensus_performance_metrics(self, exchange: ConsensusBasedExchange) -> go.Figure:
        """
        Visualize consensus performance metrics for trading.
        
        Args:
            exchange: ConsensusBasedExchange instance
            
        Returns:
            Plotly figure showing performance metrics
        """
        stats = exchange.get_exchange_statistics()
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Order Processing", "Consensus Operations", 
                          "Market Activity", "System Health"),
            specs=[[{"type": "indicator"}, {"type": "pie"}],
                   [{"type": "bar"}, {"type": "gauge"}]]
        )
        
        # Order processing indicator
        total_orders = stats.get('total_orders', 0)
        fig.add_trace(
            go.Indicator(
                mode="number+delta",
                value=total_orders,
                title={"text": "Total Orders Processed"},
                delta={"reference": 0, "valueformat": ".0f"}
            ),
            row=1, col=1
        )
        
        # Consensus operations pie chart
        pending_ops = stats.get('pending_operations', 0)
        confirmed_ops = stats.get('confirmed_operations', 0)
        
        if pending_ops + confirmed_ops > 0:
            fig.add_trace(
                go.Pie(
                    labels=['Confirmed', 'Pending'],
                    values=[confirmed_ops, pending_ops],
                    name="Consensus Ops"
                ),
                row=1, col=2
            )
        
        # Market activity bar chart
        symbols = list(stats.get('market_states', {}).keys())
        if symbols:
            volumes = []
            for symbol in symbols:
                market_state = stats['market_states'][symbol]
                volume = float(market_state.get('volume_24h', 0))
                volumes.append(volume)
            
            fig.add_trace(
                go.Bar(x=symbols, y=volumes, name="24h Volume"),
                row=2, col=1
            )
        
        # System health gauge
        consensus_nodes = stats.get('consensus_nodes', 0)
        max_expected_nodes = 10  # Assume max 10 nodes for visualization
        health_percentage = min(100, (consensus_nodes / max_expected_nodes) * 100)
        
        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=health_percentage,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "System Health %"},
                gauge={
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "lightgray"},
                        {'range': [50, 80], 'color': "yellow"},
                        {'range': [80, 100], 'color': "green"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ),
            row=2, col=2
        )
        
        fig.update_layout(
            title="Consensus-Based Exchange Performance Metrics",
            height=700
        )
        
        return fig
    
    def visualize_cross_exchange_arbitrage(self, cross_consensus) -> go.Figure:
        """
        Visualize cross-exchange arbitrage opportunities.
        
        Args:
            cross_consensus: CrossExchangeConsensus instance
            
        Returns:
            Plotly figure showing arbitrage analysis
        """
        opportunities = cross_consensus.arbitrage_opportunities
        
        if not opportunities:
            fig = go.Figure()
            fig.add_annotation(
                text="No arbitrage opportunities detected",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=16)
            )
            return fig
        
        # Prepare data
        symbols = [opp['symbol'] for opp in opportunities]
        profit_percentages = [float(opp['profit_percentage']) for opp in opportunities]
        buy_exchanges = [opp['buy_exchange'] for opp in opportunities]
        sell_exchanges = [opp['sell_exchange'] for opp in opportunities]
        
        # Create scatter plot
        fig = go.Figure()
        
        # Group by symbol
        unique_symbols = list(set(symbols))
        colors = px.colors.qualitative.Set2
        
        for i, symbol in enumerate(unique_symbols):
            symbol_data = [opp for opp in opportunities if opp['symbol'] == symbol]
            symbol_profits = [float(opp['profit_percentage']) for opp in symbol_data]
            symbol_labels = [f"{opp['buy_exchange']} → {opp['sell_exchange']}" for opp in symbol_data]
            
            fig.add_trace(go.Scatter(
                x=list(range(len(symbol_data))),
                y=symbol_profits,
                mode='markers+text',
                name=symbol,
                text=symbol_labels,
                textposition="top center",
                marker=dict(
                    size=15,
                    color=colors[i % len(colors)],
                    line=dict(width=2, color='black')
                ),
                hovertemplate=f'{symbol}<br>Profit: %{{y:.2f}}%<br>Route: %{{text}}<extra></extra>'
            ))
        
        fig.update_layout(
            title="Cross-Exchange Arbitrage Opportunities",
            xaxis_title="Opportunity Index",
            yaxis_title="Profit Percentage (%)",
            height=500,
            hovermode='closest'
        )
        
        return fig


def create_comprehensive_analysis_dashboard(analyzer: ComplexityAnalyzer,
                                          exchange: ConsensusBasedExchange,
                                          network_stats: List[Dict]) -> Dict[str, go.Figure]:
    """
    Create a comprehensive dashboard with all visualization components.
    
    Args:
        analyzer: ComplexityAnalyzer with measurement data
        exchange: ConsensusBasedExchange instance
        network_stats: Network statistics from different models
        
    Returns:
        Dictionary of Plotly figures for dashboard
    """
    protocol_viz = ConsensusProtocolVisualizer()
    financial_viz = FinancialConsensusVisualizer()
    
    dashboard = {}
    
    # Protocol analysis visualizations
    protocols = ["streamlet", "byzantine_broadcast", "randomized", "nakamoto"]
    dashboard['round_complexity'] = protocol_viz.visualize_round_complexity(analyzer, protocols)
    dashboard['communication_complexity'] = protocol_viz.visualize_communication_complexity(analyzer, protocols)
    dashboard['network_behavior'] = protocol_viz.visualize_network_behavior(network_stats)
    
    # Consensus network topology
    nodes = [f"node_{i}" for i in range(7)]
    byzantine_nodes = ["node_5", "node_6"]  # Last 2 nodes are Byzantine
    dashboard['network_topology'] = protocol_viz.create_consensus_network_graph(nodes, byzantine_nodes)
    
    # Financial consensus visualizations
    dashboard['exchange_performance'] = financial_viz.visualize_consensus_performance_metrics(exchange)
    
    # Add order book visualization if orders exist
    symbols = list(exchange.order_book.keys())
    if symbols:
        dashboard['order_book'] = financial_viz.visualize_order_book_consensus(exchange, symbols[0])
    
    # Add trade timeline if trades exist
    if exchange.executed_trades:
        dashboard['trade_timeline'] = financial_viz.visualize_trade_execution_timeline(exchange)
    
    return dashboard


def save_dashboard_html(dashboard: Dict[str, go.Figure], filename: str) -> None:
    """
    Save dashboard as HTML file.
    
    Args:
        dashboard: Dictionary of Plotly figures
        filename: Output HTML filename
    """
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Distributed Consensus & Financial Applications Dashboard</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .chart-container {{ margin: 20px 0; border: 1px solid #ddd; padding: 10px; }}
            h1, h2 {{ color: #333; }}
        </style>
    </head>
    <body>
        <h1>Distributed Consensus & Financial Applications Analysis Dashboard</h1>
        <p>Interactive visualizations of consensus protocols and financial trading systems based on Elaine Shi's textbook.</p>
    """
    
    for title, fig in dashboard.items():
        html_content += f"""
        <div class="chart-container">
            <h2>{title.replace('_', ' ').title()}</h2>
            <div id="{title}"></div>
            <script>
                Plotly.newPlot('{title}', {fig.to_json()});
            </script>
        </div>
        """
    
    html_content += """
    </body>
    </html>
    """
    
    with open(filename, 'w') as f:
        f.write(html_content)
    
    print(f"Dashboard saved as {filename}")


if __name__ == "__main__":
    # Example usage
    print("Consensus Protocol Visualization Tools")
    print("Use these classes to create interactive visualizations of your consensus protocols and financial applications.")
