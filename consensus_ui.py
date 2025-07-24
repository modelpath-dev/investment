#!/usr/bin/env python3
"""
Interactive UI for Consensus Protocol Analysis with Network Delay

This creates a simple interactive interface to explore how network delay
affects consensus protocol performance, based on the TorbellinoTech test.
"""

import sys
import os
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import List, Dict

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from network_delay_analysis import NetworkDelaySimulator, NetworkConfig, NetworkType
    from delta_impact_demo import DelayAwareConsensusDemo, ConsensusResult
except ImportError as e:
    st.error(f"Import error: {e}")
    st.error("Please ensure all required modules are installed and available")
    st.stop()


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Consensus Protocol Network Delay Analysis",
        page_icon="🔗",
        layout="wide"
    )
    
    st.title("🔗 Consensus Protocol Network Delay Analysis")
    st.markdown("**Interactive exploration of how network delay (Δ) affects consensus performance**")
    st.markdown("*Based on TorbellinoTech programming test and distributed consensus theory*")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Protocol Configuration")
    
    # Protocol selection
    protocol = st.sidebar.selectbox(
        "Select Protocol",
        ["Dolev-Strong Byzantine Broadcast", "Streamlet", "PBFT", "HotStuff"],
        index=0
    )
    
    # Network parameters
    st.sidebar.subheader("Network Parameters")
    
    num_nodes = st.sidebar.slider(
        "Number of Nodes (n)",
        min_value=4,
        max_value=20,
        value=7,
        step=1,
        help="Total number of nodes in the network"
    )
    
    max_byzantine = (num_nodes - 1) // 3
    num_byzantine = st.sidebar.slider(
        "Byzantine Nodes (f)",
        min_value=0,
        max_value=max_byzantine,
        value=min(2, max_byzantine),
        step=1,
        help=f"Number of Byzantine faulty nodes (max {max_byzantine} for n={num_nodes})"
    )
    
    network_delay = st.sidebar.slider(
        "Network Delay Δ (ms)",
        min_value=10,
        max_value=2000,
        value=200,
        step=50,
        help="Maximum network delay between nodes"
    )
    
    network_type = st.sidebar.selectbox(
        "Network Timing Model",
        [NetworkType.SYNCHRONOUS, NetworkType.ASYNCHRONOUS, NetworkType.PARTIAL_SYNCHRONY],
        format_func=lambda x: x.value
    )
    
    # Analysis options
    st.sidebar.subheader("Analysis Options")
    
    show_theoretical = st.sidebar.checkbox("Show Theoretical Bounds", value=True)
    show_comparison = st.sidebar.checkbox("Compare Multiple Scenarios", value=False)
    auto_refresh = st.sidebar.checkbox("Auto-refresh Analysis", value=False)
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📊 Protocol Performance Analysis")
        
        # Run analysis button
        if st.button("🔄 Run Analysis", type="primary") or auto_refresh:
            with st.spinner("Running consensus protocol simulation..."):
                # Create demo instance
                demo = DelayAwareConsensusDemo()
                
                # Run single scenario
                result = demo.simulate_dolev_strong_with_delay(
                    num_nodes=num_nodes,
                    num_byzantine=num_byzantine,
                    delta_ms=network_delay,
                    network_type=network_type
                )
                
                # Display results
                st.success("✅ Analysis completed!")
                
                # Key metrics
                col_a, col_b, col_c, col_d = st.columns(4)
                
                with col_a:
                    st.metric(
                        "Completion Time",
                        f"{result.completion_time_ms:.0f} ms",
                        delta=f"vs theoretical: {result.completion_time_ms - (result.round_count * network_delay):.0f} ms"
                    )
                
                with col_b:
                    st.metric(
                        "Round Count",
                        result.round_count,
                        help="Number of communication rounds required"
                    )
                
                with col_c:
                    st.metric(
                        "Message Count",
                        result.message_count,
                        help="Total messages exchanged"
                    )
                
                with col_d:
                    efficiency = (result.round_count * network_delay) / result.completion_time_ms
                    st.metric(
                        "Efficiency",
                        f"{efficiency:.1%}",
                        help="Actual vs theoretical performance"
                    )
                
                # Detailed breakdown
                st.subheader("📈 Performance Breakdown")
                
                # Theoretical vs actual comparison
                theoretical_time = result.round_count * network_delay
                overhead = result.completion_time_ms - theoretical_time
                
                breakdown_data = {
                    "Component": ["Theoretical Minimum", "Network Overhead", "Protocol Overhead"],
                    "Time (ms)": [theoretical_time, overhead * 0.7, overhead * 0.3],
                    "Percentage": [
                        theoretical_time / result.completion_time_ms * 100,
                        (overhead * 0.7) / result.completion_time_ms * 100,
                        (overhead * 0.3) / result.completion_time_ms * 100
                    ]
                }
                
                breakdown_df = pd.DataFrame(breakdown_data)
                
                fig_breakdown = px.bar(
                    breakdown_df,
                    x="Component",
                    y="Time (ms)",
                    color="Component",
                    title="Time Breakdown Analysis"
                )
                
                st.plotly_chart(fig_breakdown, use_container_width=True)
        
        # Comparison analysis
        if show_comparison:
            st.header("🔄 Scenario Comparison")
            
            if st.button("🧪 Run Multi-Scenario Analysis"):
                with st.spinner("Running multiple scenarios..."):
                    demo = DelayAwareConsensusDemo()
                    
                    # Test different delay values
                    delay_values = [50, 100, 200, 500, 1000]
                    comparison_results = []
                    
                    for delay in delay_values:
                        result = demo.simulate_dolev_strong_with_delay(num_nodes, num_byzantine, delay)
                        comparison_results.append({
                            "Network Delay (ms)": delay,
                            "Completion Time (ms)": result.completion_time_ms,
                            "Efficiency (%)": (result.round_count * delay) / result.completion_time_ms * 100,
                            "Messages": result.message_count
                        })
                    
                    comparison_df = pd.DataFrame(comparison_results)
                    
                    # Plot comparison
                    fig_comparison = go.Figure()
                    
                    fig_comparison.add_trace(go.Scatter(
                        x=comparison_df["Network Delay (ms)"],
                        y=comparison_df["Completion Time (ms)"],
                        mode='lines+markers',
                        name='Actual Completion Time',
                        line=dict(color='red', width=3)
                    ))
                    
                    # Add theoretical line
                    theoretical_times = [(num_byzantine + 1) * delay for delay in delay_values]
                    fig_comparison.add_trace(go.Scatter(
                        x=delay_values,
                        y=theoretical_times,
                        mode='lines',
                        name='Theoretical Minimum',
                        line=dict(color='blue', dash='dash', width=2)
                    ))
                    
                    fig_comparison.update_layout(
                        title="Completion Time vs Network Delay",
                        xaxis_title="Network Delay Δ (ms)",
                        yaxis_title="Completion Time (ms)",
                        hovermode='x unified'
                    )
                    
                    st.plotly_chart(fig_comparison, use_container_width=True)
                    
                    # Show data table
                    st.subheader("📋 Detailed Results")
                    st.dataframe(comparison_df, use_container_width=True)
    
    with col2:
        st.header("📚 Protocol Information")
        
        # Protocol description
        if protocol == "Dolev-Strong Byzantine Broadcast":
            st.markdown("""
            **Dolev-Strong Protocol**
            
            - **Rounds**: f+1 (optimal)
            - **Message Complexity**: O(n^(f+1))
            - **Fault Tolerance**: f < n
            - **Network**: Synchronous
            
            This protocol achieves Byzantine agreement 
            in exactly f+1 rounds, which is optimal.
            """)
        
        # Network delay impact
        st.subheader("🌐 Network Delay Impact")
        st.markdown(f"""
        **Current Configuration:**
        - Network Delay (Δ): {network_delay} ms
        - Expected Rounds: {num_byzantine + 1}
        - Theoretical Time: {(num_byzantine + 1) * network_delay} ms
        
        **Key Insights:**
        - Higher Δ increases confirmation time
        - More Byzantine nodes require more rounds
        - Network timing affects predictability
        """)
        
        # Theoretical bounds
        if show_theoretical:
            st.subheader("📐 Theoretical Analysis")
            st.markdown(f"""
            **Lower Bounds:**
            - Minimum rounds: {num_byzantine + 1}
            - Minimum time: {(num_byzantine + 1) * network_delay} ms
            
            **Upper Bounds:**
            - Timeout per round: {network_delay * 2} ms
            - Maximum time: {(num_byzantine + 1) * network_delay * 2} ms
            
            **Fault Tolerance:**
            - Max Byzantine: {(num_nodes - 1) // 3}
            - Safety margin: {num_nodes - 3 * num_byzantine - 1}
            """)
        
        # Help section
        st.subheader("❓ Help")
        with st.expander("Understanding the Analysis"):
            st.markdown("""
            **Network Delay (Δ):**
            Maximum time for a message to travel between any two nodes.
            
            **Completion Time:**
            Total time from protocol start to consensus achievement.
            
            **Efficiency:**
            How close actual performance is to theoretical minimum.
            
            **Round Count:**
            Number of communication phases required for consensus.
            """)


if __name__ == "__main__":
    main()
