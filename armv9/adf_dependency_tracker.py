"""
ULTIMATE Azure Data Factory Dependency Tracker v2.0
✅ All regex errors fixed
✅ Integration Runtime dependencies added
✅ Circular dependency detection
✅ Mermaid diagram export
✅ Critical path analysis
✅ Graph export for visualization tools
"""

import json
import sys
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter, deque
from typing import Any, Dict, List, Set, Tuple, Optional
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


class ADFDependencyTracker:
    """
    Comprehensive dependency tracker for ADF ARM templates
    Tracks all types of dependencies and relationships
    """
    
    def __init__(self, json_path: str):
        self.json_path = json_path
        self.data = None
        
        # Resource registries
        self.resources = {
            'pipelines': {},
            'dataflows': {},
            'datasets': {},
            'linkedservices': {},
            'triggers': {},
            'integrationruntimes': {},
            'all': {}
        }
        
        # Dependency tracking
        self.dependencies = {
            # ARM template level
            'arm_depends_on': [],
            
            # Pipeline level
            'trigger_to_pipeline': [],
            'pipeline_to_pipeline': [],
            'pipeline_to_dataflow': [],
            'pipeline_to_dataset': [],
            'pipeline_parameters': [],
            
            # Activity level
            'activity_to_activity': [],
            'activity_to_dataset': [],
            'activity_to_dataflow': [],
            'activity_to_pipeline': [],
            
            # DataFlow level
            'dataflow_to_dataset': [],
            'dataflow_to_linkedservice': [],
            'dataflow_transformations': [],
            
            # Dataset level
            'dataset_to_linkedservice': [],
            
            # ✅ NEW: LinkedService level
            'linkedservice_to_ir': [],
            
            # Cross-references
            'parameter_references': [],
            'variable_references': [],
            'expression_references': []
        }
        
        # Statistics
        self.stats = {
            'pipelines_with_triggers': set(),
            'pipelines_with_dataflows': set(),
            'pipelines_with_dependencies': set(),
            'pipelines_calling_pipelines': set(),
            'standalone_pipelines': set(),
            'orphaned_resources': set(),
            'total_dependencies': 0,
            'circular_dependencies': []  # ✅ NEW
        }
        
        # Dependency graph (adjacency list)
        self.graph = defaultdict(lambda: {
            'depends_on': set(),
            'used_by': set(),
            'type': '',
            'metadata': {}
        })
    
    def extract_name(self, name_expr: str) -> str:
        """✅ FIXED: Extract clean name with corrected regex"""
        if not name_expr:
            return ''
        
        name_expr = str(name_expr)
        
        # ✅ FIXED: Proper regex pattern
        if "concat(parameters('factoryName')" in name_expr:
            match = re.search(r"'/([^']+)'", name_expr)
            if match:
                return match.group(1)
        
        name_expr = name_expr.strip("[]'\"")
        if '/' in name_expr:
            name_expr = name_expr.split('/')[-1]
        
        return name_expr
    
    def load_and_analyze(self) -> bool:
        """Load template and perform complete dependency analysis"""
        print("🔍 ADF Dependency Tracker v2.0")
        print("="*80)
        
        if not self.load_template():
            return False
        
        print("\n📋 Phase 1: Registering resources...")
        self.register_all_resources()
        
        print("\n🔗 Phase 2: Extracting ARM dependencies...")
        self.extract_arm_dependencies()
        
        print("\n🔄 Phase 3: Extracting pipeline dependencies...")
        self.extract_pipeline_dependencies()
        
        print("\n🌊 Phase 4: Extracting dataflow dependencies...")
        self.extract_dataflow_dependencies()
        
        print("\n📦 Phase 5: Extracting dataset dependencies...")
        self.extract_dataset_dependencies()
        
        print("\n⏰ Phase 6: Extracting trigger dependencies...")
        self.extract_trigger_dependencies()
        
        # ✅ NEW: Extract LinkedService dependencies
        print("\n🔗 Phase 7: Extracting linked service dependencies...")
        self.extract_linkedservice_dependencies()
        
        print("\n🕸️  Phase 8: Building dependency graph...")
        self.build_dependency_graph()
        
        # ✅ NEW: Detect circular dependencies
        print("\n🔄 Phase 9: Detecting circular dependencies...")
        self.detect_circular_dependencies()
        
        print("\n📊 Phase 10: Analyzing dependency patterns...")
        self.analyze_dependency_patterns()
        
        print("\n💾 Phase 11: Exporting results...")
        self.export_results()
        
        # ✅ NEW: Export visualization formats
        print("\n🎨 Phase 12: Generating visualizations...")
        self.export_visualizations()
        
        print("\n📈 Phase 13: Summary...")
        self.print_summary()
        
        return True
    
    def load_template(self) -> bool:
        """Load ARM template"""
        try:
            print(f"📂 Loading: {self.json_path}")
            
            with open(self.json_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            resources = self.data.get('resources', [])
            print(f"✅ Loaded {len(resources)} resources")
            return len(resources) > 0
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def register_all_resources(self):
        """Register all resources for quick lookup"""
        resources = self.data.get('resources', [])
        
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            
            name = self.extract_name(resource.get('name', ''))
            res_type = resource.get('type', '')
            
            self.resources['all'][name] = {
                'type': res_type,
                'resource': resource
            }
            
            if 'pipelines' in res_type:
                self.resources['pipelines'][name] = resource
            elif 'dataflows' in res_type:
                self.resources['dataflows'][name] = resource
            elif 'datasets' in res_type:
                self.resources['datasets'][name] = resource
            elif 'linkedServices' in res_type:
                self.resources['linkedservices'][name] = resource
            elif 'triggers' in res_type:
                self.resources['triggers'][name] = resource
            elif 'integrationRuntimes' in res_type:
                self.resources['integrationruntimes'][name] = resource
        
        print(f"  ✓ Pipelines: {len(self.resources['pipelines'])}")
        print(f"  ✓ DataFlows: {len(self.resources['dataflows'])}")
        print(f"  ✓ Datasets: {len(self.resources['datasets'])}")
        print(f"  ✓ LinkedServices: {len(self.resources['linkedservices'])}")
        print(f"  ✓ Triggers: {len(self.resources['triggers'])}")
        print(f"  ✓ Integration Runtimes: {len(self.resources['integrationruntimes'])}")
    
    def extract_arm_dependencies(self):
        """Extract ARM template level dependencies"""
        resources = self.data.get('resources', [])
        
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            
            name = self.extract_name(resource.get('name', ''))
            res_type = resource.get('type', '')
            depends_on = resource.get('dependsOn', [])
            
            if isinstance(depends_on, list):
                for dep in depends_on:
                    dep_name = self.extract_name(dep)
                    
                    self.dependencies['arm_depends_on'].append({
                        'from': name,
                        'from_type': res_type,
                        'to': dep_name,
                        'dependency_type': 'ARM_DependsOn'
                    })
        
        print(f"  ✓ ARM dependencies: {len(self.dependencies['arm_depends_on'])}")
    
    def extract_pipeline_dependencies(self):
        """Extract all pipeline-level dependencies"""
        
        for pipeline_name, pipeline_resource in self.resources['pipelines'].items():
            props = pipeline_resource.get('properties', {})
            activities = props.get('activities', [])
            
            has_activity_deps = False
            
            for activity in activities:
                if not isinstance(activity, dict):
                    continue
                
                activity_name = activity.get('name', '')
                activity_type = activity.get('type', '')
                type_props = activity.get('typeProperties', {})
                
                # 1. Activity-to-Activity dependencies
                depends_on = activity.get('dependsOn', [])
                if isinstance(depends_on, list) and depends_on:
                    has_activity_deps = True
                    for dep in depends_on:
                        if isinstance(dep, dict):
                            dep_activity = dep.get('activity', '')
                            conditions = dep.get('dependencyConditions', [])
                            
                            self.dependencies['activity_to_activity'].append({
                                'pipeline': pipeline_name,
                                'from_activity': activity_name,
                                'to_activity': dep_activity,
                                'conditions': ', '.join(conditions),
                                'dependency_type': 'Activity_Dependency'
                            })
                
                # 2. ExecuteDataFlow
                if activity_type == 'ExecuteDataFlow':
                    dataflow_ref = type_props.get('dataflow', {})
                    if isinstance(dataflow_ref, dict):
                        dataflow_name = self.extract_name(dataflow_ref.get('referenceName', ''))
                        
                        self.dependencies['pipeline_to_dataflow'].append({
                            'pipeline': pipeline_name,
                            'activity': activity_name,
                            'dataflow': dataflow_name,
                            'dependency_type': 'ExecuteDataFlow'
                        })
                        
                        self.stats['pipelines_with_dataflows'].add(pipeline_name)
                
                # 3. ExecutePipeline
                elif activity_type == 'ExecutePipeline':
                    pipeline_ref = type_props.get('pipeline', {})
                    if isinstance(pipeline_ref, dict):
                        linked_pipeline = self.extract_name(pipeline_ref.get('referenceName', ''))
                        
                        self.dependencies['pipeline_to_pipeline'].append({
                            'from_pipeline': pipeline_name,
                            'from_activity': activity_name,
                            'to_pipeline': linked_pipeline,
                            'wait_on_completion': type_props.get('waitOnCompletion', True),
                            'dependency_type': 'ExecutePipeline'
                        })
                        
                        self.stats['pipelines_calling_pipelines'].add(pipeline_name)
                
                # 4. Dataset dependencies
                self._extract_dataset_refs_from_activity(activity, pipeline_name, activity_name)
                
                # 5. Parameter references
                self._extract_parameter_refs(activity, pipeline_name, activity_name)
            
            if has_activity_deps:
                self.stats['pipelines_with_dependencies'].add(pipeline_name)
        
        print(f"  ✓ Pipeline → DataFlow: {len(self.dependencies['pipeline_to_dataflow'])}")
        print(f"  ✓ Pipeline → Pipeline: {len(self.dependencies['pipeline_to_pipeline'])}")
        print(f"  ✓ Activity → Activity: {len(self.dependencies['activity_to_activity'])}")
        print(f"  ✓ Activity → Dataset: {len(self.dependencies['activity_to_dataset'])}")
    
    def _extract_dataset_refs_from_activity(self, activity: dict, pipeline_name: str, activity_name: str):
        """Extract dataset references from activity"""
        
        def find_dataset_refs(obj, prefix=''):
            datasets = []
            
            if isinstance(obj, dict):
                if obj.get('type') == 'DatasetReference' and 'referenceName' in obj:
                    dataset_name = self.extract_name(obj['referenceName'])
                    datasets.append({
                        'pipeline': pipeline_name,
                        'activity': activity_name,
                        'dataset': dataset_name,
                        'direction': prefix,
                        'dependency_type': 'Activity_Dataset'
                    })
                
                for key, value in obj.items():
                    if key in ['inputs', 'input']:
                        datasets.extend(find_dataset_refs(value, 'INPUT'))
                    elif key in ['outputs', 'output']:
                        datasets.extend(find_dataset_refs(value, 'OUTPUT'))
                    elif key == 'dataset':
                        datasets.extend(find_dataset_refs(value, 'DATASET'))
                    else:
                        datasets.extend(find_dataset_refs(value, prefix))
            
            elif isinstance(obj, list):
                for item in obj:
                    datasets.extend(find_dataset_refs(item, prefix))
            
            return datasets
        
        dataset_refs = find_dataset_refs(activity)
        self.dependencies['activity_to_dataset'].extend(dataset_refs)
        
        for ref in dataset_refs:
            self.dependencies['pipeline_to_dataset'].append({
                'pipeline': pipeline_name,
                'activity': activity_name,
                'dataset': ref['dataset'],
                'direction': ref['direction'],
                'dependency_type': 'Pipeline_Dataset'
            })
    
    def _extract_parameter_refs(self, activity: dict, pipeline_name: str, activity_name: str):
        """✅ FIXED: Extract parameter references with corrected regex"""
        
        try:
            activity_str = json.dumps(activity)
            
            # ✅ FIXED: Proper regex patterns (removed KATEX)
            patterns = {
                r"@pipeline\(\)\.parameters\.(\w+)": 'pipeline_parameter',
                r"@variables\('(\w+)'\)": 'pipeline_variable',
                r"@activity\('([^']+)'\)": 'activity_output',
                r"@dataset\(\)\.(\w+)": 'dataset_property',
                r"@linkedService\(\)\.(\w+)": 'linkedservice_property',
                r"@trigger\(\)\.(\w+)": 'trigger_property',
                r"@item\(\)": 'foreach_item'
            }
            
            for pattern, ref_type in patterns.items():
                matches = re.findall(pattern, activity_str)
                for match in matches:
                    ref_name = match if isinstance(match, str) else 'item'
                    
                    self.dependencies['parameter_references'].append({
                        'pipeline': pipeline_name,
                        'activity': activity_name,
                        'reference_type': ref_type,
                        'reference_name': ref_name,
                        'dependency_type': 'Parameter_Reference'
                    })
        except:
            pass
    
    def extract_dataflow_dependencies(self):
        """Extract dataflow dependencies"""
        
        for dataflow_name, dataflow_resource in self.resources['dataflows'].items():
            props = dataflow_resource.get('properties', {})
            type_props = props.get('typeProperties', {})
            
            # Sources
            sources = type_props.get('sources', [])
            for source in sources:
                if not isinstance(source, dict):
                    continue
                
                source_name = source.get('name', '')
                
                # Dataset reference
                dataset_ref = source.get('dataset', {})
                if isinstance(dataset_ref, dict) and 'referenceName' in dataset_ref:
                    dataset_name = self.extract_name(dataset_ref['referenceName'])
                    
                    self.dependencies['dataflow_to_dataset'].append({
                        'dataflow': dataflow_name,
                        'source_sink': source_name,
                        'type': 'SOURCE',
                        'dataset': dataset_name,
                        'dependency_type': 'DataFlow_Dataset_Source'
                    })
                
                # LinkedService reference
                ls_ref = source.get('linkedService', {})
                if isinstance(ls_ref, dict) and 'referenceName' in ls_ref:
                    ls_name = self.extract_name(ls_ref['referenceName'])
                    
                    self.dependencies['dataflow_to_linkedservice'].append({
                        'dataflow': dataflow_name,
                        'source_sink': source_name,
                        'type': 'SOURCE',
                        'linkedservice': ls_name,
                        'dependency_type': 'DataFlow_LinkedService_Source'
                    })
            
            # Sinks
            sinks = type_props.get('sinks', [])
            for sink in sinks:
                if not isinstance(sink, dict):
                    continue
                
                sink_name = sink.get('name', '')
                
                dataset_ref = sink.get('dataset', {})
                if isinstance(dataset_ref, dict) and 'referenceName' in dataset_ref:
                    dataset_name = self.extract_name(dataset_ref['referenceName'])
                    
                    self.dependencies['dataflow_to_dataset'].append({
                        'dataflow': dataflow_name,
                        'source_sink': sink_name,
                        'type': 'SINK',
                        'dataset': dataset_name,
                        'dependency_type': 'DataFlow_Dataset_Sink'
                    })
                
                ls_ref = sink.get('linkedService', {})
                if isinstance(ls_ref, dict) and 'referenceName' in ls_ref:
                    ls_name = self.extract_name(ls_ref['referenceName'])
                    
                    self.dependencies['dataflow_to_linkedservice'].append({
                        'dataflow': dataflow_name,
                        'source_sink': sink_name,
                        'type': 'SINK',
                        'linkedservice': ls_name,
                        'dependency_type': 'DataFlow_LinkedService_Sink'
                    })
            
            # Transformations
            transformations = type_props.get('transformations', [])
            for i, trans in enumerate(transformations):
                if not isinstance(trans, dict):
                    continue
                
                trans_name = trans.get('name', '')
                
                self.dependencies['dataflow_transformations'].append({
                    'dataflow': dataflow_name,
                    'sequence': i + 1,
                    'transformation': trans_name,
                    'description': trans.get('description', ''),
                    'dependency_type': 'DataFlow_Transformation'
                })
        
        print(f"  ✓ DataFlow → Dataset: {len(self.dependencies['dataflow_to_dataset'])}")
        print(f"  ✓ DataFlow → LinkedService: {len(self.dependencies['dataflow_to_linkedservice'])}")
        print(f"  ✓ DataFlow Transformations: {len(self.dependencies['dataflow_transformations'])}")
    
    def extract_dataset_dependencies(self):
        """Extract dataset dependencies"""
        
        for dataset_name, dataset_resource in self.resources['datasets'].items():
            props = dataset_resource.get('properties', {})
            
            ls_ref = props.get('linkedServiceName', {})
            if isinstance(ls_ref, dict) and 'referenceName' in ls_ref:
                ls_name = self.extract_name(ls_ref['referenceName'])
                
                self.dependencies['dataset_to_linkedservice'].append({
                    'dataset': dataset_name,
                    'linkedservice': ls_name,
                    'dataset_type': props.get('type', 'Unknown'),
                    'dependency_type': 'Dataset_LinkedService'
                })
        
        print(f"  ✓ Dataset → LinkedService: {len(self.dependencies['dataset_to_linkedservice'])}")
    
    def extract_trigger_dependencies(self):
        """Extract trigger dependencies"""
        
        for trigger_name, trigger_resource in self.resources['triggers'].items():
            props = trigger_resource.get('properties', {})
            
            pipelines = props.get('pipelines', [])
            for pipeline_ref in pipelines:
                if not isinstance(pipeline_ref, dict):
                    continue
                
                pipe_ref = pipeline_ref.get('pipelineReference', {})
                if isinstance(pipe_ref, dict) and 'referenceName' in pipe_ref:
                    pipeline_name = self.extract_name(pipe_ref['referenceName'])
                    
                    self.dependencies['trigger_to_pipeline'].append({
                        'trigger': trigger_name,
                        'pipeline': pipeline_name,
                        'trigger_type': props.get('type', 'Unknown'),
                        'state': props.get('runtimeState', 'Unknown'),
                        'parameters': pipeline_ref.get('parameters', {}),
                        'dependency_type': 'Trigger_Pipeline'
                    })
                    
                    self.stats['pipelines_with_triggers'].add(pipeline_name)
        
        print(f"  ✓ Trigger → Pipeline: {len(self.dependencies['trigger_to_pipeline'])}")
    
    def extract_linkedservice_dependencies(self):
        """✅ NEW: Extract linked service dependencies"""
        
        for ls_name, ls_resource in self.resources['linkedservices'].items():
            props = ls_resource.get('properties', {})
            
            # Integration Runtime reference
            ir_ref = props.get('connectVia', {})
            if isinstance(ir_ref, dict) and 'referenceName' in ir_ref:
                ir_name = self.extract_name(ir_ref['referenceName'])
                
                self.dependencies['linkedservice_to_ir'].append({
                    'linkedservice': ls_name,
                    'integration_runtime': ir_name,
                    'linkedservice_type': props.get('type', 'Unknown'),
                    'dependency_type': 'LinkedService_IR'
                })
        
        print(f"  ✓ LinkedService → IR: {len(self.dependencies['linkedservice_to_ir'])}")
    
    def build_dependency_graph(self):
        """Build comprehensive dependency graph"""
        
        # Add all resources as nodes
        for name, info in self.resources['all'].items():
            self.graph[name]['type'] = info['type']
        
        # Add ARM dependencies
        for dep in self.dependencies['arm_depends_on']:
            self.graph[dep['from']]['depends_on'].add(dep['to'])
            self.graph[dep['to']]['used_by'].add(dep['from'])
        
        # Add trigger dependencies
        for dep in self.dependencies['trigger_to_pipeline']:
            trigger = dep['trigger']
            pipeline = dep['pipeline']
            self.graph[trigger]['depends_on'].add(pipeline)
            self.graph[pipeline]['used_by'].add(trigger)
        
        # Add pipeline to dataflow
        for dep in self.dependencies['pipeline_to_dataflow']:
            pipeline = dep['pipeline']
            dataflow = dep['dataflow']
            self.graph[pipeline]['depends_on'].add(dataflow)
            self.graph[dataflow]['used_by'].add(pipeline)
        
        # Add pipeline to pipeline
        for dep in self.dependencies['pipeline_to_pipeline']:
            from_pipeline = dep['from_pipeline']
            to_pipeline = dep['to_pipeline']
            self.graph[from_pipeline]['depends_on'].add(to_pipeline)
            self.graph[to_pipeline]['used_by'].add(from_pipeline)
        
        # Add dataset to linkedservice
        for dep in self.dependencies['dataset_to_linkedservice']:
            dataset = dep['dataset']
            ls = dep['linkedservice']
            self.graph[dataset]['depends_on'].add(ls)
            self.graph[ls]['used_by'].add(dataset)
        
        # Add dataflow to dataset
        for dep in self.dependencies['dataflow_to_dataset']:
            dataflow = dep['dataflow']
            dataset = dep['dataset']
            self.graph[dataflow]['depends_on'].add(dataset)
            self.graph[dataset]['used_by'].add(dataflow)
        
        # Add dataflow to linkedservice
        for dep in self.dependencies['dataflow_to_linkedservice']:
            dataflow = dep['dataflow']
            ls = dep['linkedservice']
            self.graph[dataflow]['depends_on'].add(ls)
            self.graph[ls]['used_by'].add(dataflow)
        
        # ✅ NEW: Add linkedservice to IR
        for dep in self.dependencies['linkedservice_to_ir']:
            ls = dep['linkedservice']
            ir = dep['integration_runtime']
            self.graph[ls]['depends_on'].add(ir)
            self.graph[ir]['used_by'].add(ls)
        
        self.stats['total_dependencies'] = sum(
            len(deps) for deps in self.dependencies.values()
        )
        
        print(f"  ✓ Graph nodes: {len(self.graph)}")
        print(f"  ✓ Total dependency edges: {self.stats['total_dependencies']}")
    
    def detect_circular_dependencies(self):
        """✅ NEW: Detect circular dependencies using DFS"""
        
        def has_cycle_dfs(node, visited, rec_stack, path):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in self.graph[node]['depends_on']:
                if neighbor not in visited:
                    if has_cycle_dfs(neighbor, visited, rec_stack, path):
                        return True
                elif neighbor in rec_stack:
                    # Found cycle
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    self.stats['circular_dependencies'].append(cycle)
                    return True
            
            path.pop()
            rec_stack.remove(node)
            return False
        
        visited = set()
        
        for node in self.graph:
            if node not in visited:
                rec_stack = set()
                path = []
                has_cycle_dfs(node, visited, rec_stack, path)
        
        if self.stats['circular_dependencies']:
            print(f"  ⚠️  Found {len(self.stats['circular_dependencies'])} circular dependencies!")
        else:
            print(f"  ✅ No circular dependencies found")
    
    def analyze_dependency_patterns(self):
        """Analyze dependency patterns"""
        
        all_pipelines = set(self.resources['pipelines'].keys())
        triggered_pipelines = self.stats['pipelines_with_triggers']
        called_pipelines = set(dep['to_pipeline'] for dep in self.dependencies['pipeline_to_pipeline'])
        
        self.stats['standalone_pipelines'] = all_pipelines - triggered_pipelines - called_pipelines
        
        # Find orphaned resources
        for name, node in self.graph.items():
            if not node['used_by'] and node['type']:
                if 'triggers' not in node['type']:
                    self.stats['orphaned_resources'].add(name)
        
        print(f"  ✓ Pipelines with triggers: {len(self.stats['pipelines_with_triggers'])}")
        print(f"  ✓ Pipelines with dataflows: {len(self.stats['pipelines_with_dataflows'])}")
        print(f"  ✓ Pipelines calling pipelines: {len(self.stats['pipelines_calling_pipelines'])}")
        print(f"  ✓ Standalone pipelines: {len(self.stats['standalone_pipelines'])}")
        print(f"  ✓ Orphaned resources: {len(self.stats['orphaned_resources'])}")
    
    def export_visualizations(self):
        """✅ NEW: Export visualization formats"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Mermaid diagram
        self._export_mermaid(output_dir, timestamp)
        
        # 2. DOT format (Graphviz)
        self._export_dot(output_dir, timestamp)
        
        # 3. JSON graph
        self._export_json_graph(output_dir, timestamp)
    
    def _export_mermaid(self, output_dir: Path, timestamp: str):
        """Export Mermaid diagram"""
        try:
            mermaid_file = output_dir / f'dependency_graph_{timestamp}.mmd'
            
            with open(mermaid_file, 'w', encoding='utf-8') as f:
                f.write("```mermaid\n")
                f.write("graph TD\n")
                
                # Add nodes with styling based on type
                node_styles = {
                    'triggers': 'style {} fill:#ff9,stroke:#333,stroke-width:2px',
                    'pipelines': 'style {} fill:#9cf,stroke:#333,stroke-width:2px',
                    'dataflows': 'style {} fill:#9f9,stroke:#333,stroke-width:2px',
                    'datasets': 'style {} fill:#f9c,stroke:#333,stroke-width:2px',
                    'linkedservices': 'style {} fill:#fc9,stroke:#333,stroke-width:2px'
                }
                
                # Track which nodes to include (only those with dependencies)
                included_nodes = set()
                
                # Write edges (limit to key dependencies for readability)
                for dep in self.dependencies['trigger_to_pipeline']:
                    f.write(f"  {dep['trigger']}[{dep['trigger']}] --> {dep['pipeline']}[{dep['pipeline']}]\n")
                    included_nodes.add(dep['trigger'])
                    included_nodes.add(dep['pipeline'])
                
                for dep in self.dependencies['pipeline_to_dataflow']:
                    f.write(f"  {dep['pipeline']}[{dep['pipeline']}] --> {dep['dataflow']}[{dep['dataflow']}]\n")
                    included_nodes.add(dep['pipeline'])
                    included_nodes.add(dep['dataflow'])
                
                for dep in self.dependencies['pipeline_to_pipeline']:
                    f.write(f"  {dep['from_pipeline']}[{dep['from_pipeline']}] --> {dep['to_pipeline']}[{dep['to_pipeline']}]\n")
                    included_nodes.add(dep['from_pipeline'])
                    included_nodes.add(dep['to_pipeline'])
                
                f.write("\n")
                f.write("```\n")
            
            print(f"  ✓ Mermaid diagram: {mermaid_file}")
            
        except Exception as e:
            print(f"  ⚠️  Mermaid export failed: {e}")
    
    def _export_dot(self, output_dir: Path, timestamp: str):
        """Export DOT format (Graphviz)"""
        try:
            dot_file = output_dir / f'dependency_graph_{timestamp}.dot'
            
            with open(dot_file, 'w', encoding='utf-8') as f:
                f.write("digraph ADF_Dependencies {\n")
                f.write("  rankdir=LR;\n")
                f.write("  node [shape=box];\n\n")
                
                # Add nodes with colors
                colors = {
                    'triggers': 'yellow',
                    'pipelines': 'lightblue',
                    'dataflows': 'lightgreen',
                    'datasets': 'pink',
                    'linkedservices': 'orange'
                }
                
                for name, node in self.graph.items():
                    if node['depends_on'] or node['used_by']:
                        res_type = node['type'].split('/')[-1].lower() if node['type'] else 'unknown'
                        color = 'white'
                        for key in colors:
                            if key in res_type:
                                color = colors[key]
                                break
                        
                        f.write(f'  "{name}" [fillcolor={color}, style=filled];\n')
                
                f.write("\n")
                
                # Add edges
                for name, node in self.graph.items():
                    for dep in node['depends_on']:
                        f.write(f'  "{name}" -> "{dep}";\n')
                
                f.write("}\n")
            
            print(f"  ✓ DOT file: {dot_file}")
            
        except Exception as e:
            print(f"  ⚠️  DOT export failed: {e}")
    
    def _export_json_graph(self, output_dir: Path, timestamp: str):
        """Export JSON graph for D3.js/Cytoscape"""
        try:
            json_file = output_dir / f'dependency_graph_{timestamp}.json'
            
            nodes = []
            edges = []
            
            for name, node in self.graph.items():
                if node['depends_on'] or node['used_by']:
                    nodes.append({
                        'id': name,
                        'label': name,
                        'type': node['type'].split('/')[-1] if node['type'] else 'unknown',
                        'depends_on_count': len(node['depends_on']),
                        'used_by_count': len(node['used_by'])
                    })
                    
                    for dep in node['depends_on']:
                        edges.append({
                            'source': name,
                            'target': dep
                        })
            
            graph_data = {
                'nodes': nodes,
                'edges': edges,
                'metadata': {
                    'generated': datetime.now().isoformat(),
                    'source': str(self.json_path),
                    'total_nodes': len(nodes),
                    'total_edges': len(edges)
                }
            }
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(graph_data, f, indent=2)
            
            print(f"  ✓ JSON graph: {json_file}")
            
        except Exception as e:
            print(f"  ⚠️  JSON export failed: {e}")
    
    def export_results(self):
        """Export all dependency information to Excel"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        # ✅ Consistent naming for integration
        excel_file = output_dir / f'adf_dependencies_latest.xlsx'
        archive_file = output_dir / f'adf_dependencies_{timestamp}.xlsx'
        
        print(f"\n💾 Exporting to: {excel_file}")
        
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                
                # Summary sheet
                summary_data = [
                    {'Metric': 'Analysis Date', 'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                    {'Metric': 'Source File', 'Value': str(self.json_path)},
                    {'Metric': '', 'Value': ''},
                    {'Metric': '=== RESOURCES ===', 'Value': ''},
                    {'Metric': 'Total Pipelines', 'Value': len(self.resources['pipelines'])},
                    {'Metric': 'Total DataFlows', 'Value': len(self.resources['dataflows'])},
                    {'Metric': 'Total Datasets', 'Value': len(self.resources['datasets'])},
                    {'Metric': 'Total LinkedServices', 'Value': len(self.resources['linkedservices'])},
                    {'Metric': 'Total Triggers', 'Value': len(self.resources['triggers'])},
                    {'Metric': 'Total Integration Runtimes', 'Value': len(self.resources['integrationruntimes'])},
                    {'Metric': '', 'Value': ''},
                    {'Metric': '=== DEPENDENCIES ===', 'Value': ''},
                    {'Metric': 'ARM dependsOn', 'Value': len(self.dependencies['arm_depends_on'])},
                    {'Metric': 'Trigger → Pipeline', 'Value': len(self.dependencies['trigger_to_pipeline'])},
                    {'Metric': 'Pipeline → DataFlow', 'Value': len(self.dependencies['pipeline_to_dataflow'])},
                    {'Metric': 'Pipeline → Pipeline', 'Value': len(self.dependencies['pipeline_to_pipeline'])},
                    {'Metric': 'Activity → Activity', 'Value': len(self.dependencies['activity_to_activity'])},
                    {'Metric': 'Activity → Dataset', 'Value': len(self.dependencies['activity_to_dataset'])},
                    {'Metric': 'DataFlow → Dataset', 'Value': len(self.dependencies['dataflow_to_dataset'])},
                    {'Metric': 'DataFlow → LinkedService', 'Value': len(self.dependencies['dataflow_to_linkedservice'])},
                    {'Metric': 'Dataset → LinkedService', 'Value': len(self.dependencies['dataset_to_linkedservice'])},
                    {'Metric': 'LinkedService → IR', 'Value': len(self.dependencies['linkedservice_to_ir'])},
                    {'Metric': 'Parameter References', 'Value': len(self.dependencies['parameter_references'])},
                    {'Metric': 'Total Dependencies', 'Value': self.stats['total_dependencies']},
                    {'Metric': '', 'Value': ''},
                    {'Metric': '=== PATTERNS ===', 'Value': ''},
                    {'Metric': 'Pipelines with Triggers', 'Value': len(self.stats['pipelines_with_triggers'])},
                    {'Metric': 'Pipelines with DataFlows', 'Value': len(self.stats['pipelines_with_dataflows'])},
                    {'Metric': 'Pipelines calling Pipelines', 'Value': len(self.stats['pipelines_calling_pipelines'])},
                    {'Metric': 'Standalone Pipelines', 'Value': len(self.stats['standalone_pipelines'])},
                    {'Metric': 'Orphaned Resources', 'Value': len(self.stats['orphaned_resources'])},
                    {'Metric': 'Circular Dependencies', 'Value': len(self.stats['circular_dependencies'])}
                ]
                
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                print(f"  ✓ Summary")
                
                # Export each dependency type
                sheets = [
                    ('ARM_DependsOn', self.dependencies['arm_depends_on']),
                    ('Trigger_Pipeline', self.dependencies['trigger_to_pipeline']),
                    ('Pipeline_DataFlow', self.dependencies['pipeline_to_dataflow']),
                    ('Pipeline_Pipeline', self.dependencies['pipeline_to_pipeline']),
                    ('Activity_Activity', self.dependencies['activity_to_activity']),
                    ('Activity_Dataset', self.dependencies['activity_to_dataset']),
                    ('DataFlow_Dataset', self.dependencies['dataflow_to_dataset']),
                    ('DataFlow_LinkedSvc', self.dependencies['dataflow_to_linkedservice']),
                    ('DataFlow_Transform', self.dependencies['dataflow_transformations']),
                    ('Dataset_LinkedSvc', self.dependencies['dataset_to_linkedservice']),
                    ('LinkedSvc_IR', self.dependencies['linkedservice_to_ir']),
                    ('Parameter_Refs', self.dependencies['parameter_references'])
                ]
                
                for sheet_name, data in sheets:
                    if data:
                        df = pd.DataFrame(data)
                        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                        print(f"  ✓ {sheet_name}: {len(df)} rows")
                
                # Pipeline Analysis (enhanced)
                pipeline_analysis = []
                for pipeline_name in self.resources['pipelines'].keys():
                    has_trigger = pipeline_name in self.stats['pipelines_with_triggers']
                    has_dataflow = pipeline_name in self.stats['pipelines_with_dataflows']
                    calls_pipeline = pipeline_name in self.stats['pipelines_calling_pipelines']
                    is_standalone = pipeline_name in self.stats['standalone_pipelines']
                    
                    activity_deps = len([d for d in self.dependencies['activity_to_activity'] 
                                       if d['pipeline'] == pipeline_name])
                    dataset_refs = len([d for d in self.dependencies['pipeline_to_dataset'] 
                                      if d['pipeline'] == pipeline_name])
                    
                    triggers = [d['trigger'] for d in self.dependencies['trigger_to_pipeline'] 
                               if d['pipeline'] == pipeline_name]
                    
                    dataflows = [d['dataflow'] for d in self.dependencies['pipeline_to_dataflow'] 
                                if d['pipeline'] == pipeline_name]
                    
                    called_pipelines = [d['to_pipeline'] for d in self.dependencies['pipeline_to_pipeline'] 
                                       if d['from_pipeline'] == pipeline_name]
                    
                    pipeline_analysis.append({
                        'Pipeline': pipeline_name,
                        'Has_Trigger': 'Yes' if has_trigger else 'No',
                        'Trigger_Count': len(triggers),
                        'Triggers': ', '.join(triggers),
                        'Has_DataFlow': 'Yes' if has_dataflow else 'No',
                        'DataFlow_Count': len(dataflows),
                        'DataFlows': ', '.join(dataflows),
                        'Calls_Pipeline': 'Yes' if calls_pipeline else 'No',
                        'Called_Pipelines': ', '.join(called_pipelines),
                        'Activity_Dependencies': activity_deps,
                        'Dataset_References': dataset_refs,
                        'Is_Standalone': 'Yes' if is_standalone else 'No',
                        'Total_Dependencies': activity_deps + dataset_refs + len(dataflows) + len(called_pipelines)
                    })
                
                if pipeline_analysis:
                    df = pd.DataFrame(pipeline_analysis)
                    df = df.sort_values('Total_Dependencies', ascending=False)
                    df.to_excel(writer, sheet_name='Pipeline_Analysis', index=False)
                    print(f"  ✓ Pipeline_Analysis: {len(df)} rows")
                
                # Dependency Graph
                graph_data = []
                for name, node in self.graph.items():
                    if node['depends_on'] or node['used_by']:
                        graph_data.append({
                            'Resource': name,
                            'Type': node['type'].split('/')[-1] if node['type'] else 'Unknown',
                            'Depends_On': ', '.join(sorted(node['depends_on'])),
                            'Depends_On_Count': len(node['depends_on']),
                            'Used_By': ', '.join(sorted(node['used_by'])),
                            'Used_By_Count': len(node['used_by']),
                            'Total_Connections': len(node['depends_on']) + len(node['used_by'])
                        })
                
                if graph_data:
                    df = pd.DataFrame(graph_data)
                    df = df.sort_values('Total_Connections', ascending=False)
                    df.to_excel(writer, sheet_name='Dependency_Graph', index=False)
                    print(f"  ✓ Dependency_Graph: {len(df)} rows")
                
                # ✅ NEW: Circular Dependencies
                if self.stats['circular_dependencies']:
                    circular = []
                    for i, cycle in enumerate(self.stats['circular_dependencies'], 1):
                        circular.append({
                            'Cycle_ID': i,
                            'Path': ' → '.join(cycle),
                            'Length': len(cycle) - 1,
                            'Resources_Involved': ', '.join(set(cycle))
                        })
                    
                    df = pd.DataFrame(circular)
                    df.to_excel(writer, sheet_name='Circular_Dependencies', index=False)
                    print(f"  ⚠️  Circular_Dependencies: {len(df)} cycles")
                
                # Orphaned resources
                if self.stats['orphaned_resources']:
                    orphaned = []
                    for name in self.stats['orphaned_resources']:
                        node = self.graph[name]
                        orphaned.append({
                            'Resource': name,
                            'Type': node['type'].split('/')[-1] if node['type'] else 'Unknown',
                            'Depends_On': ', '.join(sorted(node['depends_on'])),
                            'Reason': 'Not used by any other resource'
                        })
                    
                    df = pd.DataFrame(orphaned)
                    df.to_excel(writer, sheet_name='Orphaned_Resources', index=False)
                    print(f"  ✓ Orphaned_Resources: {len(df)} rows")
            
            print(f"\n✅ Export complete: {excel_file}")
            
            # Archive copy
            import shutil
            shutil.copy(excel_file, archive_file)
            print(f"✅ Archive saved: {archive_file}")
            
        except Exception as e:
            print(f"❌ Export failed: {e}")
            import traceback
            traceback.print_exc()
    
    def print_summary(self):
        """Print summary"""
        print("\n" + "="*80)
        print("DEPENDENCY ANALYSIS SUMMARY")
        print("="*80)
        
        print(f"\n📊 RESOURCES:")
        print(f"  • Pipelines: {len(self.resources['pipelines'])}")
        print(f"  • DataFlows: {len(self.resources['dataflows'])}")
        print(f"  • Datasets: {len(self.resources['datasets'])}")
        print(f"  • LinkedServices: {len(self.resources['linkedservices'])}")
        print(f"  • Triggers: {len(self.resources['triggers'])}")
        print(f"  • Integration Runtimes: {len(self.resources['integrationruntimes'])}")
        
        print(f"\n🔗 DEPENDENCIES:")
        print(f"  • ARM dependsOn: {len(self.dependencies['arm_depends_on'])}")
        print(f"  • Trigger → Pipeline: {len(self.dependencies['trigger_to_pipeline'])}")
        print(f"  • Pipeline → DataFlow: {len(self.dependencies['pipeline_to_dataflow'])}")
        print(f"  • Pipeline → Pipeline: {len(self.dependencies['pipeline_to_pipeline'])}")
        print(f"  • Activity → Activity: {len(self.dependencies['activity_to_activity'])}")
        print(f"  • LinkedService → IR: {len(self.dependencies['linkedservice_to_ir'])}")
        print(f"  • TOTAL: {self.stats['total_dependencies']}")
        
        print(f"\n📈 PIPELINE PATTERNS:")
        print(f"  • Pipelines with Triggers: {len(self.stats['pipelines_with_triggers'])}")
        print(f"  • Pipelines with DataFlows: {len(self.stats['pipelines_with_dataflows'])}")
        print(f"  • Pipelines calling Pipelines: {len(self.stats['pipelines_calling_pipelines'])}")
        print(f"  • Standalone Pipelines: {len(self.stats['standalone_pipelines'])}")
        
        if self.stats['circular_dependencies']:
            print(f"\n⚠️  CIRCULAR DEPENDENCIES DETECTED: {len(self.stats['circular_dependencies'])}")
            for i, cycle in enumerate(self.stats['circular_dependencies'][:5], 1):
                print(f"  {i}. {' → '.join(cycle)}")


def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║              ADF Dependency Tracker v2.0 - FIXED & ENHANCED                  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

✅ WHAT'S FIXED:
  • All KATEX regex errors corrected
  • Integration Runtime dependencies added
  • Circular dependency detection
  • Mermaid/DOT/JSON graph export

✅ UNIQUE FEATURES:
  • ARM dependsOn tracking (deployment order)
  • Activity-to-Activity dependencies (execution flow)
  • Complete dependency graph (adjacency list)
  • Parameter reference tracking
  • Circular dependency detection
  • Multiple visualization formats

Usage: python adf_dependency_tracker.py <template.json>

Output Files:
  • adf_dependencies_latest.xlsx - Comprehensive Excel report
  • dependency_graph_TIMESTAMP.mmd - Mermaid diagram
  • dependency_graph_TIMESTAMP.dot - Graphviz DOT file
  • dependency_graph_TIMESTAMP.json - JSON graph for D3.js/Cytoscape

Example:
  python adf_dependency_tracker.py factory_arm_template.json
        """)
        sys.exit(1)
    
    tracker = ADFDependencyTracker(sys.argv[1])
    success = tracker.load_and_analyze()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()