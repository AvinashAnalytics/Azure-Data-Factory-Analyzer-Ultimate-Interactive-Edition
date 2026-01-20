"""
Azure Data Factory Analyzer v9.0 - Ultimate Interactive Edition
Features:
- 3D Network Graphs
- Comprehensive Dependency Tracking
- Pipeline Impact Analysis
- Orphaned Resource Detection
- Advanced Filtering
- Interactive Dashboards
"""

import streamlit as st
import pandas as pd
import json
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import networkx as nx
from pyvis.network import Network
import tempfile
from pathlib import Path
import base64
from datetime import datetime
import re
from collections import defaultdict, Counter
from typing import Dict, List, Any, Tuple
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Page Configuration
st.set_page_config(
    page_title="ADF Analyzer v9.0",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced Custom CSS
st.markdown("""
<style>
    .main {
        padding: 0rem 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        background-color: #f0f2f6;
        border-radius: 5px;
        font-weight: 600;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        transition: transform 0.2s;
    }
    .metric-card:hover {
        transform: translateY(-5px);
    }
    .metric-value {
        font-size: 2.5em;
        font-weight: bold;
        margin: 10px 0;
    }
    .metric-label {
        font-size: 1.1em;
        opacity: 0.9;
    }
    .dependency-badge {
        display: inline-block;
        padding: 5px 10px;
        margin: 3px;
        border-radius: 15px;
        font-size: 0.85em;
        font-weight: 600;
    }
    .badge-trigger {
        background: #FFD700;
        color: #000;
    }
    .badge-dataflow {
        background: #87CEEB;
        color: #000;
    }
    .badge-pipeline {
        background: #90EE90;
        color: #000;
    }
    .badge-standalone {
        background: #FFB6C1;
        color: #000;
    }
    .badge-orphaned {
        background: #FFA07A;
        color: #000;
    }
</style>
""", unsafe_allow_html=True)


class EnhancedADFAnalyzer:
    """Enhanced ADF Analyzer with full dependency tracking"""
    
    def __init__(self):
        self.initialize_session_state()
        
    def initialize_session_state(self):
        """Initialize session state"""
        if 'data_loaded' not in st.session_state:
            st.session_state.data_loaded = False
        if 'excel_data' not in st.session_state:
            st.session_state.excel_data = {}
        if 'dependency_graph' not in st.session_state:
            st.session_state.dependency_graph = None
    
    def run(self):
        """Main application entry"""
        
        # Header
        st.markdown("""
        <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #0066cc 0%, #004499 100%); color: white; border-radius: 10px; margin-bottom: 20px;">
            <h1 style="margin: 0;">🏭 Azure Data Factory Analyzer v9.0</h1>
            <p style="margin: 10px 0 0 0; opacity: 0.9;">Ultimate Interactive Analysis Dashboard with 3D Visualizations</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Sidebar
        with st.sidebar:
            self.render_sidebar()
        
        # Main content
        if st.session_state.data_loaded:
            self.render_main_dashboard()
        else:
            self.render_welcome_screen()
    
    def render_sidebar(self):
        """Render sidebar controls"""
        st.markdown("## 📁 Data Input")
        
        # File upload
        uploaded_file = st.file_uploader(
            "Upload Excel Analysis File",
            type=['xlsx'],
            help="Upload the Excel file from the Enterprise Parser"
        )
        
        if uploaded_file:
            if st.button("🔍 Load Analysis", type="primary", use_container_width=True):
                self.load_excel_analysis(uploaded_file)
        
        # Sample data
        if st.button("📊 Load Sample Data", use_container_width=True):
            self.load_sample_data()
        
        if st.session_state.data_loaded:
            st.success("✅ Data loaded successfully!")
            
            # Quick stats
            st.markdown("---")
            st.markdown("## 📊 Quick Stats")
            
            summary = st.session_state.excel_data.get('Summary', pd.DataFrame())
            if not summary.empty:
                metrics = summary.set_index('Metric')['Value'].to_dict()
                
                st.metric("Pipelines", metrics.get('Pipelines', 0))
                st.metric("DataFlows", metrics.get('DataFlows', 0))
                st.metric("Dependencies", metrics.get('Total Dependencies', 0))
            
            # Filters
            st.markdown("---")
            st.markdown("## 🎯 Filters")
            self.render_filters()
    
    def render_filters(self):
        """Render filter controls"""
        pipeline_analysis = st.session_state.excel_data.get('Pipeline_Analysis', pd.DataFrame())
        
        if not pipeline_analysis.empty:
            # Filter options
            filter_options = st.multiselect(
                "Show Pipelines",
                ["All", "With Triggers", "With DataFlows", "Calling Other Pipelines", "Standalone", "Orphaned"],
                default=["All"]
            )
            
            st.session_state.filter_options = filter_options
    
    def load_excel_analysis(self, uploaded_file):
        """Load Excel analysis file"""
        try:
            with st.spinner("🔄 Loading analysis..."):
                # Read all sheets
                excel_file = pd.ExcelFile(uploaded_file)
                
                data = {}
                for sheet_name in excel_file.sheet_names:
                    data[sheet_name] = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                st.session_state.excel_data = data
                st.session_state.data_loaded = True
                
                # Build dependency graph
                self.build_dependency_graph()
                
                st.success("✅ Analysis loaded successfully!")
                st.balloons()
                
        except Exception as e:
            st.error(f"❌ Error loading file: {str(e)}")
    
    def build_dependency_graph(self):
        """Build NetworkX dependency graph"""
        try:
            G = nx.DiGraph()
            
            # Add pipeline nodes
            pipeline_analysis = st.session_state.excel_data.get('Pipeline_Analysis', pd.DataFrame())
            for _, row in pipeline_analysis.iterrows():
                G.add_node(
                    row['Pipeline'],
                    type='pipeline',
                    has_trigger=row.get('Has_Trigger', 'No') == 'Yes',
                    has_dataflow=row.get('Has_DataFlow', 'No') == 'Yes',
                    is_standalone=row.get('Is_Standalone', 'No') == 'Yes'
                )
            
            # Add trigger edges
            trigger_pipeline = st.session_state.excel_data.get('Trigger_Pipeline', pd.DataFrame())
            if not trigger_pipeline.empty:
                for _, row in trigger_pipeline.iterrows():
                    G.add_node(row['trigger'], type='trigger')
                    G.add_edge(row['trigger'], row['pipeline'], relation='triggers')
            
            # Add pipeline-to-pipeline edges
            pipeline_pipeline = st.session_state.excel_data.get('Pipeline_Pipeline', pd.DataFrame())
            if not pipeline_pipeline.empty:
                for _, row in pipeline_pipeline.iterrows():
                    G.add_edge(row['from_pipeline'], row['to_pipeline'], relation='executes')
            
            # Add dataflow edges
            pipeline_dataflow = st.session_state.excel_data.get('Pipeline_DataFlow', pd.DataFrame())
            if not pipeline_dataflow.empty:
                for _, row in pipeline_dataflow.iterrows():
                    G.add_node(row['dataflow'], type='dataflow')
                    G.add_edge(row['pipeline'], row['dataflow'], relation='uses_dataflow')
            
            st.session_state.dependency_graph = G
            
        except Exception as e:
            st.error(f"Error building graph: {str(e)}")
    
    def render_welcome_screen(self):
        """Welcome screen"""
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("""
            <div style="text-align: center; padding: 50px;">
                <h2>Welcome to ADF Analyzer v9.0! 🚀</h2>
                <p style="font-size: 1.2em; margin: 20px 0;">
                    Upload your Enterprise Parser Excel output to begin
                </p>
                <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                    <h3>✨ New Features v9.0</h3>
                    <ul style="text-align: left; display: inline-block;">
                        <li>🌐 3D Network Graphs</li>
                        <li>🔗 Complete Dependency Tracking</li>
                        <li>📊 Pipeline Impact Analysis</li>
                        <li>🎯 Orphaned Resource Detection</li>
                        <li>📈 Advanced Statistics</li>
                        <li>🔍 Interactive Exploration</li>
                        <li>💡 Smart Recommendations</li>
                    </ul>
                </div>
                <p>👈 Use the sidebar to upload your analysis file</p>
            </div>
            """, unsafe_allow_html=True)
    
    def render_main_dashboard(self):
        """Render main dashboard"""
        
        # Enhanced metrics
        self.render_enhanced_metrics()
        
        # Main tabs
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs([
            "🏠 Overview",
            "🌐 3D Network",
            "🔗 Dependencies",
            "📊 Pipeline Analysis",
            "🎯 Impact Analysis",
            "⚠️ Orphaned Resources",
            "📈 Statistics",
            "🌊 DataFlows",
            "⏰ Triggers",
            "🔍 Explorer"
        ])
        
        with tab1:
            self.render_overview()
        
        with tab2:
            self.render_3d_network()
        
        with tab3:
            self.render_dependencies()
        
        with tab4:
            self.render_pipeline_analysis()
        
        with tab5:
            self.render_impact_analysis()
        
        with tab6:
            self.render_orphaned_resources()
        
        with tab7:
            self.render_statistics()
        
        with tab8:
            self.render_dataflows()
        
        with tab9:
            self.render_triggers()
        
        with tab10:
            self.render_explorer()
    
    def render_enhanced_metrics(self):
        """Render enhanced metrics row"""
        summary = st.session_state.excel_data.get('Summary', pd.DataFrame())
        
        if summary.empty:
            return
        
        metrics = summary.set_index('Metric')['Value'].to_dict()
        
        col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Pipelines</div>
                <div class="metric-value">{metrics.get('Pipelines', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
                <div class="metric-label">DataFlows</div>
                <div class="metric-value">{metrics.get('DataFlows', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
                <div class="metric-label">Datasets</div>
                <div class="metric-value">{metrics.get('Datasets', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card" style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);">
                <div class="metric-label">Triggers</div>
                <div class="metric-value">{metrics.get('Triggers', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            st.markdown(f"""
            <div class="metric-card" style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);">
                <div class="metric-label">Dependencies</div>
                <div class="metric-value">{metrics.get('Total Dependencies', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col6:
            st.markdown(f"""
            <div class="metric-card" style="background: linear-gradient(135deg, #30cfd0 0%, #330867 100%);">
                <div class="metric-label">With Triggers</div>
                <div class="metric-value">{metrics.get('Pipelines with Triggers', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col7:
            st.markdown(f"""
            <div class="metric-card" style="background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%); color: #333;">
                <div class="metric-label">Standalone</div>
                <div class="metric-value">{metrics.get('Standalone Pipelines', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
    
    def render_overview(self):
        """Render overview dashboard"""
        st.markdown("### 🏠 Factory Overview")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Pipeline distribution chart
            pipeline_analysis = st.session_state.excel_data.get('Pipeline_Analysis', pd.DataFrame())
            
            if not pipeline_analysis.empty:
                # Create summary
                summary_data = {
                    'Category': [
                        'With Triggers',
                        'With DataFlows',
                        'Calling Pipelines',
                        'Standalone',
                        'Orphaned'
                    ],
                    'Count': [
                        (pipeline_analysis['Has_Trigger'] == 'Yes').sum(),
                        (pipeline_analysis['Has_DataFlow'] == 'Yes').sum(),
                        (pipeline_analysis['Calls_Pipeline'] == 'Yes').sum(),
                        (pipeline_analysis['Is_Standalone'] == 'Yes').sum(),
                        (pipeline_analysis.get('Is_Orphaned', 'No') == 'Yes').sum()
                    ]
                }
                
                fig = px.bar(
                    summary_data,
                    x='Count',
                    y='Category',
                    orientation='h',
                    title="Pipeline Categories",
                    color='Count',
                    color_continuous_scale='viridis'
                )
                
                fig.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Dependency type breakdown
            dep_types = []
            dep_counts = []
            
            for dep_type in ['ARM_DependsOn', 'Trigger_Pipeline', 'Pipeline_DataFlow', 'Pipeline_Pipeline']:
                df = st.session_state.excel_data.get(dep_type, pd.DataFrame())
                if not df.empty:
                    dep_types.append(dep_type.replace('_', ' '))
                    dep_counts.append(len(df))
            
            if dep_types:
                fig = go.Figure(data=[go.Pie(
                    labels=dep_types,
                    values=dep_counts,
                    hole=0.4
                )])
                
                fig.update_layout(
                    title="Dependency Types",
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        # Recent analysis info
        st.markdown("---")
        st.markdown("### 📅 Analysis Information")
        
        summary = st.session_state.excel_data.get('Summary', pd.DataFrame())
        if not summary.empty:
            metrics = summary.set_index('Metric')['Value'].to_dict()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.info(f"**Analysis Date:** {metrics.get('Analysis Date', 'N/A')}")
            
            with col2:
                st.info(f"**Source:** {Path(str(metrics.get('Source File', 'N/A'))).name}")
            
            with col3:
                errors = metrics.get('Parse Errors', 0)
                if errors > 0:
                    st.warning(f"**Parse Errors:** {errors}")
                else:
                    st.success("**Parse Errors:** 0")
    
    def render_3d_network(self):
        """Render 3D network visualization"""
        st.markdown("### 🌐 3D Dependency Network")
        st.markdown("Interactive 3D visualization of your data factory dependencies")
        
        if st.session_state.dependency_graph is None:
            st.warning("No dependency graph available")
            return
        
        col1, col2 = st.columns([3, 1])
        
        with col2:
            # Controls
            st.markdown("#### 🎛️ Controls")
            
            show_node_types = st.multiselect(
                "Show Node Types",
                ["Triggers", "Pipelines", "DataFlows"],
                default=["Triggers", "Pipelines", "DataFlows"]
            )
            
            layout_3d = st.selectbox(
                "3D Layout",
                ["Spring", "Shell", "Spectral", "Kamada-Kawai"],
                index=0
            )
            
            node_size_3d = st.slider("Node Size", 5, 20, 10)
            
            show_labels = st.checkbox("Show Labels", value=True)
        
        with col1:
            # Create 3D network
            G = st.session_state.dependency_graph
            
            # Filter nodes by type
            nodes_to_show = []
            for node, data in G.nodes(data=True):
                node_type = data.get('type', 'unknown')
                if (node_type == 'trigger' and "Triggers" in show_node_types) or \
                   (node_type == 'pipeline' and "Pipelines" in show_node_types) or \
                   (node_type == 'dataflow' and "DataFlows" in show_node_types):
                    nodes_to_show.append(node)
            
            # Create subgraph
            H = G.subgraph(nodes_to_show)
            
            if len(H.nodes()) == 0:
                st.warning("No nodes to display with current filters")
                return
            
            # Calculate 3D layout
            if layout_3d == "Spring":
                pos = nx.spring_layout(H, dim=3, seed=42)
            elif layout_3d == "Shell":
                pos = nx.shell_layout(H, dim=3)
            elif layout_3d == "Spectral":
                pos = nx.spectral_layout(H, dim=3)
            else:  # Kamada-Kawai
                pos = nx.kamada_kawai_layout(H, dim=3)
            
            # Extract coordinates
            x_nodes = [pos[node][0] for node in H.nodes()]
            y_nodes = [pos[node][1] for node in H.nodes()]
            z_nodes = [pos[node][2] for node in H.nodes()]
            
            # Node colors based on type
            node_colors = []
            node_text = []
            
            for node in H.nodes():
                node_type = H.nodes[node].get('type', 'unknown')
                
                if node_type == 'trigger':
                    node_colors.append('gold')
                    node_text.append(f"🔔 {node}")
                elif node_type == 'pipeline':
                    if H.nodes[node].get('has_trigger'):
                        node_colors.append('lightgreen')
                    elif H.nodes[node].get('is_standalone'):
                        node_colors.append('lightcoral')
                    else:
                        node_colors.append('skyblue')
                    node_text.append(f"📦 {node}")
                elif node_type == 'dataflow':
                    node_colors.append('plum')
                    node_text.append(f"🌊 {node}")
                else:
                    node_colors.append('gray')
                    node_text.append(node)
            
            # Create edges
            x_edges = []
            y_edges = []
            z_edges = []
            
            for edge in H.edges():
                x_edges.extend([pos[edge[0]][0], pos[edge[1]][0], None])
                y_edges.extend([pos[edge[0]][1], pos[edge[1]][1], None])
                z_edges.extend([pos[edge[0]][2], pos[edge[1]][2], None])
            
            # Create 3D plot
            fig = go.Figure()
            
            # Add edges
            fig.add_trace(go.Scatter3d(
                x=x_edges,
                y=y_edges,
                z=z_edges,
                mode='lines',
                line=dict(color='rgba(125, 125, 125, 0.3)', width=2),
                hoverinfo='none',
                showlegend=False
            ))
            
            # Add nodes
            fig.add_trace(go.Scatter3d(
                x=x_nodes,
                y=y_nodes,
                z=z_nodes,
                mode='markers+text' if show_labels else 'markers',
                marker=dict(
                    size=node_size_3d,
                    color=node_colors,
                    line=dict(color='white', width=2)
                ),
                text=node_text if show_labels else None,
                textposition="top center",
                hovertext=node_text,
                hoverinfo='text',
                showlegend=False
            ))
            
            # Update layout
            fig.update_layout(
                title="3D Dependency Network",
                height=700,
                scene=dict(
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    zaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    bgcolor='rgba(240, 240, 240, 0.9)'
                ),
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Legend
            st.markdown("""
            **Legend:**
            - 🟡 Gold = Triggers
            - 🟢 Light Green = Pipelines with Triggers
            - 🔵 Sky Blue = Regular Pipelines
            - 🔴 Light Coral = Standalone Pipelines
            - 🟣 Plum = DataFlows
            """)
    
    def render_dependencies(self):
        """Render dependency visualization"""
        st.markdown("### 🔗 Dependency Relationships")
        
        # Dependency type selector
        dep_type = st.selectbox(
            "Select Dependency Type",
            ["Trigger → Pipeline", "Pipeline → DataFlow", "Pipeline → Pipeline", "ARM Dependencies"]
        )
        
        # Load appropriate data
        if dep_type == "Trigger → Pipeline":
            df = st.session_state.excel_data.get('Trigger_Pipeline', pd.DataFrame())
            source_col = 'trigger'
            target_col = 'pipeline'
        elif dep_type == "Pipeline → DataFlow":
            df = st.session_state.excel_data.get('Pipeline_DataFlow', pd.DataFrame())
            source_col = 'pipeline'
            target_col = 'dataflow'
        elif dep_type == "Pipeline → Pipeline":
            df = st.session_state.excel_data.get('Pipeline_Pipeline', pd.DataFrame())
            source_col = 'from_pipeline'
            target_col = 'to_pipeline'
        else:  # ARM Dependencies
            df = st.session_state.excel_data.get('ARM_DependsOn', pd.DataFrame())
            source_col = 'from'
            target_col = 'to'
        
        if df.empty:
            st.info(f"No {dep_type} dependencies found")
            return
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Create Sankey diagram
            sources = []
            targets = []
            values = []
            
            # Build node list
            nodes = list(set(df[source_col].tolist() + df[target_col].tolist()))
            node_dict = {node: idx for idx, node in enumerate(nodes)}
            
            # Build links
            for _, row in df.iterrows():
                sources.append(node_dict[row[source_col]])
                targets.append(node_dict[row[target_col]])
                values.append(1)
            
            # Create Sankey
            fig = go.Figure(data=[go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=nodes,
                    color=[
                        '#FFD700' if 'Trigger' in n or 'TR_' in n else
                        '#87CEEB' if 'DF_' in n or 'df_' in n else
                        '#90EE90'
                        for n in nodes
                    ]
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color='rgba(0, 0, 255, 0.2)'
                )
            )])
            
            fig.update_layout(
                title=f"{dep_type} Flow",
                height=600,
                font=dict(size=10)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Dependency statistics
            st.markdown("#### Statistics")
            
            st.metric("Total Dependencies", len(df))
            st.metric("Unique Sources", df[source_col].nunique())
            st.metric("Unique Targets", df[target_col].nunique())
            
            # Top sources
            st.markdown("**Top Sources:**")
            top_sources = df[source_col].value_counts().head(5)
            for source, count in top_sources.items():
                st.write(f"• {source}: {count}")
        
        # Detailed table
        st.markdown("---")
        st.markdown("#### Dependency Details")
        st.dataframe(df, use_container_width=True, height=300)
    
    def render_pipeline_analysis(self):
        """Render comprehensive pipeline analysis"""
        st.markdown("### 📊 Pipeline Analysis Dashboard")
        
        pipeline_analysis = st.session_state.excel_data.get('Pipeline_Analysis', pd.DataFrame())
        
        if pipeline_analysis.empty:
            st.warning("No pipeline analysis data available")
            return
        
        # Apply filters
        filter_options = st.session_state.get('filter_options', ["All"])
        
        filtered_df = pipeline_analysis.copy()
        
        if "All" not in filter_options:
            mask = pd.Series([False] * len(filtered_df))
            
            if "With Triggers" in filter_options:
                mask |= (filtered_df['Has_Trigger'] == 'Yes')
            if "With DataFlows" in filter_options:
                mask |= (filtered_df['Has_DataFlow'] == 'Yes')
            if "Calling Other Pipelines" in filter_options:
                mask |= (filtered_df['Calls_Pipeline'] == 'Yes')
            if "Standalone" in filter_options:
                mask |= (filtered_df['Is_Standalone'] == 'Yes')
            if "Orphaned" in filter_options:
                mask |= (filtered_df.get('Is_Orphaned', 'No') == 'Yes')
            
            filtered_df = filtered_df[mask]
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Pipelines", len(filtered_df))
        with col2:
            st.metric("With Triggers", (filtered_df['Has_Trigger'] == 'Yes').sum())
        with col3:
            st.metric("With DataFlows", (filtered_df['Has_DataFlow'] == 'Yes').sum())
        with col4:
            st.metric("Standalone", (filtered_df['Is_Standalone'] == 'Yes').sum())
        
        # Pipeline matrix
        st.markdown("---")
        st.markdown("#### Pipeline Dependency Matrix")
        
        # Create matrix data
        matrix_data = []
        for _, row in filtered_df.head(50).iterrows():
            matrix_data.append({
                'Pipeline': row['Pipeline'][:30],
                'Has Trigger': '✅' if row['Has_Trigger'] == 'Yes' else '❌',
                'Trigger Count': row.get('Trigger_Count', 0),
                'Has DataFlow': '✅' if row['Has_DataFlow'] == 'Yes' else '❌',
                'DataFlow Count': row.get('DataFlow_Count', 0),
                'Calls Pipeline': '✅' if row['Calls_Pipeline'] == 'Yes' else '❌',
                'Is Standalone': '✅' if row['Is_Standalone'] == 'Yes' else '❌'
            })
        
        if matrix_data:
            st.dataframe(
                pd.DataFrame(matrix_data),
                use_container_width=True,
                height=400
            )
        
        # Detailed view
        st.markdown("---")
        st.markdown("#### Detailed Pipeline View")
        
        # Pipeline selector
        selected_pipeline = st.selectbox(
            "Select a pipeline for details",
            filtered_df['Pipeline'].tolist()
        )
        
        if selected_pipeline:
            pipeline_row = filtered_df[filtered_df['Pipeline'] == selected_pipeline].iloc[0]
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                st.markdown("**Dependencies:**")
                
                badges = []
                if pipeline_row['Has_Trigger'] == 'Yes':
                    triggers = pipeline_row.get('Triggers', '').split(', ')
                    for trigger in triggers:
                        if trigger:
                            badges.append(f'<span class="dependency-badge badge-trigger">🔔 {trigger}</span>')
                
                if pipeline_row['Has_DataFlow'] == 'Yes':
                    dataflows = pipeline_row.get('DataFlows', '').split(', ')
                    for dataflow in dataflows:
                        if dataflow:
                            badges.append(f'<span class="dependency-badge badge-dataflow">🌊 {dataflow}</span>')
                
                if pipeline_row['Calls_Pipeline'] == 'Yes':
                    called = pipeline_row.get('Called_Pipelines', '').split(', ')
                    for pipe in called:
                        if pipe:
                            badges.append(f'<span class="dependency-badge badge-pipeline">📦 {pipe}</span>')
                
                if pipeline_row['Is_Standalone'] == 'Yes':
                    badges.append('<span class="dependency-badge badge-standalone">⚠️ Standalone</span>')
                
                if badges:
                    st.markdown(' '.join(badges), unsafe_allow_html=True)
                else:
                    st.info("No dependencies")
            
            with col2:
                st.markdown("**Metrics:**")
                st.write(f"• Trigger Count: {pipeline_row.get('Trigger_Count', 0)}")
                st.write(f"• DataFlow Count: {pipeline_row.get('DataFlow_Count', 0)}")
                st.write(f"• Called Pipeline Count: {pipeline_row.get('Called_Pipeline_Count', 0)}")
    
    def render_impact_analysis(self):
        """Render impact analysis"""
        st.markdown("### 🎯 Impact Analysis")
        st.markdown("Analyze the impact of changes to resources")
        
        if st.session_state.dependency_graph is None:
            st.warning("No dependency graph available")
            return
        
        G = st.session_state.dependency_graph
        
        # Resource selector
        all_nodes = list(G.nodes())
        selected_resource = st.selectbox(
            "Select a resource to analyze impact",
            all_nodes
        )
        
        if selected_resource:
            col1, col2 = st.columns([1, 1])
            
            with col1:
                st.markdown("#### 📥 Upstream Dependencies")
                st.markdown("*What this resource depends on*")
                
                # Find predecessors (upstream)
                upstream = list(G.predecessors(selected_resource))
                
                if upstream:
                    st.metric("Upstream Count", len(upstream))
                    
                    for node in upstream[:10]:
                        node_type = G.nodes[node].get('type', 'unknown')
                        icon = '🔔' if node_type == 'trigger' else '📦' if node_type == 'pipeline' else '🌊'
                        st.write(f"{icon} {node}")
                    
                    if len(upstream) > 10:
                        st.write(f"... and {len(upstream) - 10} more")
                else:
                    st.info("No upstream dependencies (this is a root resource)")
            
            with col2:
                st.markdown("#### 📤 Downstream Impact")
                st.markdown("*What depends on this resource*")
                
                # Find successors (downstream)
                downstream = list(G.successors(selected_resource))
                
                if downstream:
                    st.metric("Downstream Count", len(downstream))
                    
                    for node in downstream[:10]:
                        node_type = G.nodes[node].get('type', 'unknown')
                        icon = '🔔' if node_type == 'trigger' else '📦' if node_type == 'pipeline' else '🌊'
                        st.write(f"{icon} {node}")
                    
                    if len(downstream) > 10:
                        st.write(f"... and {len(downstream) - 10} more")
                    
                    # Impact warning
                    if len(downstream) > 5:
                        st.warning(f"⚠️ High Impact: {len(downstream)} resources depend on this!")
                else:
                    st.info("No downstream impact (this is a leaf resource)")
            
            # Impact visualization
            st.markdown("---")
            st.markdown("#### Impact Graph")
            
            # Create subgraph showing impact
            impact_nodes = [selected_resource] + upstream + downstream
            H = G.subgraph(impact_nodes)
            
            # Create network visualization
            pos = nx.spring_layout(H, k=0.5, iterations=50)
            
            # Create plotly figure
            edge_trace = []
            for edge in H.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_trace.append(
                    go.Scatter(
                        x=[x0, x1, None],
                        y=[y0, y1, None],
                        mode='lines',
                        line=dict(width=2, color='#888'),
                        hoverinfo='none',
                        showlegend=False
                    )
                )
            
            node_trace = go.Scatter(
                x=[pos[node][0] for node in H.nodes()],
                y=[pos[node][1] for node in H.nodes()],
                mode='markers+text',
                text=[node for node in H.nodes()],
                textposition="top center",
                marker=dict(
                    size=[30 if node == selected_resource else 20 for node in H.nodes()],
                    color=[
                        'red' if node == selected_resource else
                        'lightgreen' if node in upstream else
                        'lightblue'
                        for node in H.nodes()
                    ],
                    line=dict(width=2, color='white')
                ),
                hovertext=[f"{node} ({H.nodes[node].get('type', 'unknown')})" for node in H.nodes()],
                hoverinfo='text'
            )
            
            fig = go.Figure(data=edge_trace + [node_trace])
            fig.update_layout(
                title=f"Impact Graph for: {selected_resource}",
                showlegend=False,
                hovermode='closest',
                height=500,
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    def render_orphaned_resources(self):
        """Render orphaned resources"""
        st.markdown("### ⚠️ Orphaned Resources")
        st.markdown("Resources that are not used by anything")
        
        pipeline_analysis = st.session_state.excel_data.get('Pipeline_Analysis', pd.DataFrame())
        
        if pipeline_analysis.empty:
            st.warning("No pipeline analysis data available")
            return
        
        # Filter orphaned resources
        orphaned = pipeline_analysis[pipeline_analysis.get('Is_Orphaned', 'No') == 'Yes']
        
        if orphaned.empty:
            st.success("✅ No orphaned resources found! All resources are being used.")
            return
        
        st.warning(f"Found {len(orphaned)} orphaned resources")
        
        # Display orphaned resources
        orphaned_data = []
        for _, row in orphaned.iterrows():
            orphaned_data.append({
                'Resource': row['Pipeline'],
                'Type': 'Pipeline',
                'Has Trigger': row.get('Has_Trigger', 'No'),
                'Is Standalone': row.get('Is_Standalone', 'No'),
                'Recommendation': 'Review for deletion or add trigger'
            })
        
        if orphaned_data:
            st.dataframe(
                pd.DataFrame(orphaned_data),
                use_container_width=True,
                height=400
            )
            
            # Recommendations
            st.markdown("---")
            st.markdown("#### 💡 Recommendations")
            
            st.info("""
            **What are orphaned resources?**
            - Resources that exist but are not referenced or used by anything
            - They don't have triggers and aren't called by other pipelines
            - They may be:
              * Legacy resources that can be deleted
              * Work-in-progress resources
              * Resources intended for manual execution only
            
            **Actions to take:**
            1. Review each orphaned resource
            2. Add a trigger if it should run automatically
            3. Delete if it's no longer needed
            4. Document if it's for manual execution
            """)
    
    def render_statistics(self):
        """Render statistical analysis"""
        st.markdown("### 📈 Statistical Analysis")
        
        statistics = st.session_state.excel_data.get('Statistics', pd.DataFrame())
        
        if statistics.empty:
            st.warning("No statistics data available")
            return
        
        # Category selector
        categories = statistics['Category'].unique()
        
        tab1, tab2, tab3 = st.tabs(["📊 Distribution", "📈 Trends", "🎯 Insights"])
        
        with tab1:
            selected_category = st.selectbox("Select Category", categories)
            
            category_data = statistics[statistics['Category'] == selected_category]
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Bar chart
                fig = px.bar(
                    category_data.sort_values('Count', ascending=False).head(15),
                    x='Count',
                    y='Type',
                    orientation='h',
                    title=f"{selected_category} Distribution",
                    color='Count',
                    color_continuous_scale='viridis'
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Pie chart
                fig = go.Figure(data=[go.Pie(
                    labels=category_data['Type'].head(10),
                    values=category_data['Count'].head(10),
                    hole=0.3
                )])
                fig.update_layout(
                    title=f"Top 10 {selected_category}",
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            # Cumulative distribution
            st.markdown("#### Cumulative Distribution")
            
            category_data_sorted = category_data.sort_values('Count', ascending=False)
            category_data_sorted['Cumulative'] = category_data_sorted['Count'].cumsum()
            category_data_sorted['Percentage'] = (category_data_sorted['Cumulative'] / category_data_sorted['Count'].sum() * 100)
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=category_data_sorted['Type'],
                y=category_data_sorted['Count'],
                name='Count',
                yaxis='y'
            ))
            
            fig.add_trace(go.Scatter(
                x=category_data_sorted['Type'],
                y=category_data_sorted['Percentage'],
                name='Cumulative %',
                yaxis='y2',
                line=dict(color='red', width=3)
            ))
            
            fig.update_layout(
                title="Count and Cumulative Percentage",
                yaxis=dict(title='Count'),
                yaxis2=dict(title='Cumulative %', overlaying='y', side='right'),
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            # Insights
            st.markdown("#### 🎯 Key Insights")
            
            for category in categories:
                category_data = statistics[statistics['Category'] == category]
                
                if not category_data.empty:
                    total = category_data['Count'].sum()
                    top_item = category_data.nlargest(1, 'Count').iloc[0]
                    
                    st.markdown(f"""
                    **{category}:**
                    - Total: {total}
                    - Unique Types: {len(category_data)}
                    - Most Common: {top_item['Type']} ({top_item['Count']} instances, {top_item['Count']/total*100:.1f}%)
                    """)
    
    def render_dataflows(self):
        """Render dataflow analysis"""
        st.markdown("### 🌊 DataFlow Analysis")
        
        dataflows = st.session_state.excel_data.get('DataFlows', pd.DataFrame())
        
        if dataflows.empty:
            st.info("No DataFlows found in the analysis")
            return
        
        st.metric("Total DataFlows", len(dataflows))
        
        # DataFlow list
        st.dataframe(dataflows, use_container_width=True, height=400)
    
    def render_triggers(self):
        """Render trigger analysis"""
        st.markdown("### ⏰ Trigger Analysis")
        
        triggers = st.session_state.excel_data.get('Triggers', pd.DataFrame())
        
        if triggers.empty:
            st.info("No Triggers found in the analysis")
            return
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.dataframe(triggers, use_container_width=True, height=500)
        
        with col2:
            # Trigger state distribution
            if 'State' in triggers.columns:
                state_counts = triggers['State'].value_counts()
                
                fig = go.Figure(data=[go.Pie(
                    labels=state_counts.index,
                    values=state_counts.values,
                    hole=0.4
                )])
                
                fig.update_layout(title="Trigger States", height=300)
                st.plotly_chart(fig, use_container_width=True)
    
    def render_explorer(self):
        """Render data explorer"""
        st.markdown("### 🔍 Data Explorer")
        st.markdown("Explore all sheets from the analysis")
        
        # Sheet selector
        available_sheets = list(st.session_state.excel_data.keys())
        selected_sheet = st.selectbox("Select Sheet", available_sheets)
        
        if selected_sheet:
            df = st.session_state.excel_data[selected_sheet]
            
            if not df.empty:
                # Search
                search_term = st.text_input("🔍 Search in sheet", "")
                
                if search_term:
                    # Search in all columns
                    mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                    filtered_df = df[mask]
                    st.write(f"Found {len(filtered_df)} matching rows")
                else:
                    filtered_df = df
                
                # Display
                st.dataframe(filtered_df, use_container_width=True, height=600)
                
                # Download button
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"{selected_sheet}.csv",
                    mime="text/csv"
                )
            else:
                st.info(f"Sheet '{selected_sheet}' is empty")
    
    def load_sample_data(self):
        """Load sample data for demonstration"""
        # Create sample data matching the enterprise parser output
        sample_data = {
            'Summary': pd.DataFrame([
                {'Metric': 'Analysis Date', 'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                {'Metric': 'Pipelines', 'Value': 10},
                {'Metric': 'DataFlows', 'Value': 5},
                {'Metric': 'Datasets', 'Value': 15},
                {'Metric': 'Triggers', 'Value': 8},
                {'Metric': 'Total Dependencies', 'Value': 45},
                {'Metric': 'Pipelines with Triggers', 'Value': 6},
                {'Metric': 'Standalone Pipelines', 'Value': 2}
            ]),
            'Pipeline_Analysis': pd.DataFrame([
                {'Pipeline': 'DataIngestion', 'Has_Trigger': 'Yes', 'Trigger_Count': 2, 'Triggers': 'HourlyTrigger, DailyTrigger',
                 'Has_DataFlow': 'Yes', 'DataFlow_Count': 1, 'DataFlows': 'DF_Transform',
                 'Calls_Pipeline': 'No', 'Is_Standalone': 'No', 'Is_Orphaned': 'No'},
                {'Pipeline': 'DataTransform', 'Has_Trigger': 'Yes', 'Trigger_Count': 1, 'Triggers': 'HourlyTrigger',
                 'Has_DataFlow': 'Yes', 'DataFlow_Count': 2, 'DataFlows': 'DF_Clean, DF_Aggregate',
                 'Calls_Pipeline': 'Yes', 'Is_Standalone': 'No', 'Is_Orphaned': 'No'},
                {'Pipeline': 'OrphanedPipeline', 'Has_Trigger': 'No', 'Trigger_Count': 0, 'Triggers': '',
                 'Has_DataFlow': 'No', 'DataFlow_Count': 0, 'DataFlows': '',
                 'Calls_Pipeline': 'No', 'Is_Standalone': 'Yes', 'Is_Orphaned': 'Yes'}
            ]),
            'Trigger_Pipeline': pd.DataFrame([
                {'trigger': 'HourlyTrigger', 'pipeline': 'DataIngestion'},
                {'trigger': 'HourlyTrigger', 'pipeline': 'DataTransform'},
                {'trigger': 'DailyTrigger', 'pipeline': 'DataIngestion'}
            ]),
            'Pipeline_DataFlow': pd.DataFrame([
                {'pipeline': 'DataIngestion', 'dataflow': 'DF_Transform', 'activity': 'ExecuteDF1'},
                {'pipeline': 'DataTransform', 'dataflow': 'DF_Clean', 'activity': 'ExecuteDF2'},
                {'pipeline': 'DataTransform', 'dataflow': 'DF_Aggregate', 'activity': 'ExecuteDF3'}
            ]),
            'Statistics': pd.DataFrame([
                {'Category': 'Activity', 'Type': 'Copy', 'Count': 15},
                {'Category': 'Activity', 'Type': 'ExecuteDataFlow', 'Count': 8},
                {'Category': 'Activity', 'Type': 'Lookup', 'Count': 5},
                {'Category': 'DataFlow', 'Type': 'MappingDataFlow', 'Count': 5}
            ])
        }
        
        st.session_state.excel_data = sample_data
        st.session_state.data_loaded = True
        
        # Build dependency graph
        self.build_dependency_graph()
        
        st.success("✅ Sample data loaded!")


# Run the application
if __name__ == "__main__":
    app = EnhancedADFAnalyzer()
    app.run()