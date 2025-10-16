"""
ULTIMATE Enterprise Azure Data Factory Parser v8.0
Fully Integrated: Parsing + Dependency Tracking + Pattern Discovery
Optimized for Large-Scale Environments (350+ Pipelines, 9000+ Dependencies)
Handles Excel sheet limits with automatic splitting
"""

import json
import sys
import re
import unicodedata
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional, Tuple, Set
import pandas as pd
import warnings
import gc
warnings.filterwarnings('ignore')

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# ============================================================================
# MIXIN CLASSES
# ============================================================================

class PatternDiscoveryMixin:
    """Pattern discovery capabilities"""
    
    def discover_patterns(self):
        """Auto-discover patterns in the template"""
        print("\n🔍 Auto-discovering patterns...")
        
        self.discovered_patterns = {
            'resource_types': Counter(),
            'property_paths': defaultdict(set),
            'expression_functions': Counter()
        }
        
        resources = self.data.get('resources', [])
        
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            
            res_type = resource.get('type', '')
            category = self._extract_category(res_type)
            self.discovered_patterns['resource_types'][category] += 1
            
            self._discover_expressions(resource)
        
        print(f"✅ Discovered {len(self.discovered_patterns['resource_types'])} resource types")
    
    def _extract_category(self, res_type: str) -> str:
        """Extract category from resource type"""
        if not res_type:
            return 'unknown'
        parts = res_type.split('/')
        return parts[-1] if parts else 'unknown'
    
    def _discover_expressions(self, obj: Any):
        """Discover ADF expression functions"""
        if isinstance(obj, str):
            # Find function calls - FIXED REGEX
            functions = re.findall(r'@(\w+)\s*KATEX_INLINE_OPEN', obj)
            for func in functions:
                self.discovered_patterns['expression_functions'][func] += 1
        elif isinstance(obj, dict):
            for value in obj.values():
                self._discover_expressions(value)
        elif isinstance(obj, list):
            for item in obj[:50]:
                self._discover_expressions(item)


class DependencyTrackerMixin:
    """Comprehensive dependency tracking"""
    
    def initialize_dependency_tracking(self):
        """Initialize dependency tracking structures"""
        self.dependencies = {
            'arm_depends_on': [],
            'trigger_to_pipeline': [],
            'pipeline_to_dataflow': [],
            'pipeline_to_pipeline': [],
            'activity_to_activity': [],
            'activity_to_dataset': [],
            'dataflow_to_dataset': [],
            'dataflow_to_linkedservice': [],
            'dataset_to_linkedservice': [],
            'linkedservice_to_ir': [],
            'parameter_references': []
        }
        
        self.dep_stats = {
            'pipelines_with_triggers': set(),
            'pipelines_with_dataflows': set(),
            'pipelines_calling_pipelines': set(),
            'standalone_pipelines': set(),
            'orphaned_resources': set()
        }
        
        self.graph = defaultdict(lambda: {
            'depends_on': set(),
            'used_by': set(),
            'type': ''
        })
    
    def extract_all_dependencies(self):
        """Extract all dependencies"""
        print("\n🔗 Extracting dependencies...")
        
        # ARM dependencies
        self._extract_arm_dependencies()
        
        # Trigger dependencies
        self._extract_trigger_dependencies()
        
        # Activity dependencies
        self._extract_activity_dependencies()
        
        # Build graph
        self._build_dependency_graph()
        
        # Analyze patterns
        self._analyze_dependency_patterns()
        
        total_deps = sum(len(d) for d in self.dependencies.values())
        print(f"✅ Extracted {total_deps} dependencies")
    
    def _extract_arm_dependencies(self):
        """Extract ARM template dependencies"""
        resources = self.data.get('resources', [])
        
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            
            name = self.extract_name(resource.get('name', ''))
            depends_on = resource.get('dependsOn', [])
            
            if isinstance(depends_on, list):
                for dep in depends_on:
                    dep_name = self.extract_name(dep)
                    self.dependencies['arm_depends_on'].append({
                        'from': name,
                        'to': dep_name
                    })
    
    def _extract_trigger_dependencies(self):
        """Extract trigger dependencies"""
        for detail in self.results.get('trigger_details', []):
            trigger = detail['Trigger']
            pipeline = detail['Pipeline']
            
            self.dependencies['trigger_to_pipeline'].append({
                'trigger': trigger,
                'pipeline': pipeline
            })
            
            self.dep_stats['pipelines_with_triggers'].add(pipeline)
    
    def _extract_activity_dependencies(self):
        """Extract activity-level dependencies"""
        for activity in self.results.get('activities', []):
            pipeline = activity['Pipeline']
            
            # DataFlow dependencies
            if activity.get('DataFlow'):
                dataflow = activity['DataFlow']
                self.dependencies['pipeline_to_dataflow'].append({
                    'pipeline': pipeline,
                    'activity': activity['Activity'],
                    'dataflow': dataflow
                })
                self.dep_stats['pipelines_with_dataflows'].add(pipeline)
            
            # ExecutePipeline dependencies
            if activity.get('LinkedPipeline'):
                linked_pipeline = activity['LinkedPipeline']
                self.dependencies['pipeline_to_pipeline'].append({
                    'from_pipeline': pipeline,
                    'to_pipeline': linked_pipeline
                })
                self.dep_stats['pipelines_calling_pipelines'].add(pipeline)
    
    def _build_dependency_graph(self):
        """Build dependency graph"""
        # Add nodes
        for name in self.resources.get('all', {}).keys():
            self.graph[name]['type'] = self.resources['all'][name]['type']
        
        # Add edges
        for dep in self.dependencies['arm_depends_on']:
            self.graph[dep['from']]['depends_on'].add(dep['to'])
            self.graph[dep['to']]['used_by'].add(dep['from'])
        
        for dep in self.dependencies['trigger_to_pipeline']:
            self.graph[dep['trigger']]['depends_on'].add(dep['pipeline'])
            self.graph[dep['pipeline']]['used_by'].add(dep['trigger'])
        
        for dep in self.dependencies['pipeline_to_dataflow']:
            self.graph[dep['pipeline']]['depends_on'].add(dep['dataflow'])
            self.graph[dep['dataflow']]['used_by'].add(dep['pipeline'])
        
        for dep in self.dependencies['pipeline_to_pipeline']:
            self.graph[dep['from_pipeline']]['depends_on'].add(dep['to_pipeline'])
            self.graph[dep['to_pipeline']]['used_by'].add(dep['from_pipeline'])
    
    def _analyze_dependency_patterns(self):
        """Analyze dependency patterns"""
        all_pipelines = set(self.resources.get('pipelines', {}).keys())
        triggered = self.dep_stats['pipelines_with_triggers']
        called = set(dep['to_pipeline'] for dep in self.dependencies['pipeline_to_pipeline'])
        
        self.dep_stats['standalone_pipelines'] = all_pipelines - triggered - called
        
        # Find orphaned resources
        for name, node in self.graph.items():
            if not node['used_by'] and node['type']:
                if 'triggers' not in node['type']:
                    self.dep_stats['orphaned_resources'].add(name)
    
    def generate_pipeline_analysis(self) -> List[Dict]:
        """Generate pipeline analysis with all dependencies"""
        pipeline_analysis = []
        
        for pipeline_name in self.resources.get('pipelines', {}).keys():
            triggers = [d['trigger'] for d in self.dependencies['trigger_to_pipeline'] 
                       if d['pipeline'] == pipeline_name]
            dataflows = [d['dataflow'] for d in self.dependencies['pipeline_to_dataflow'] 
                        if d['pipeline'] == pipeline_name]
            called_pipelines = [d['to_pipeline'] for d in self.dependencies['pipeline_to_pipeline'] 
                               if d['from_pipeline'] == pipeline_name]
            
            pipeline_analysis.append({
                'Pipeline': pipeline_name,
                'Has_Trigger': 'Yes' if triggers else 'No',
                'Trigger_Count': len(triggers),
                'Triggers': ', '.join(triggers),
                'Has_DataFlow': 'Yes' if dataflows else 'No',
                'DataFlow_Count': len(dataflows),
                'DataFlows': ', '.join(dataflows),
                'Calls_Pipeline': 'Yes' if called_pipelines else 'No',
                'Called_Pipeline_Count': len(called_pipelines),
                'Called_Pipelines': ', '.join(called_pipelines),
                'Is_Standalone': 'Yes' if pipeline_name in self.dep_stats['standalone_pipelines'] else 'No',
                'Is_Orphaned': 'Yes' if pipeline_name in self.dep_stats['orphaned_resources'] else 'No'
            })
        
        return pipeline_analysis


# ============================================================================
# MAIN PARSER CLASS
# ============================================================================

class UltimateEnterpriseADFParser(PatternDiscoveryMixin, DependencyTrackerMixin):
    """
    Complete Enterprise ADF Parser
    Combines parsing, dependency tracking, and pattern discovery
    """
    
    EXCEL_MAX_ROWS = 1048576  # Excel row limit
    SHEET_SPLIT_THRESHOLD = 500000  # Split sheets at 500k rows
    
    def __init__(self, json_path: str, enable_discovery: bool = True):
        self.json_path = json_path
        self.data = None
        self.enable_discovery = enable_discovery
        
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
        
        # Results storage
        self.results = {
            'activities': [],
            'pipelines': [],
            'datasets': [],
            'linked_services': [],
            'triggers': [],
            'trigger_details': [],
            'integration_runtimes': [],
            'dataflows': [],
            'dataflow_lineage': [],
            'dataflow_transformations': [],
            'data_lineage': [],
            'errors': []
        }
        
        # Metrics
        self.metrics = {
            'activity_types': Counter(),
            'dataset_types': Counter(),
            'trigger_types': Counter(),
            'linked_service_types': Counter(),
            'dataflow_types': Counter(),
            'transformation_types': Counter()
        }
        
        # Initialize dependency tracking
        self.initialize_dependency_tracking()
        
        # Discovery patterns
        self.discovered_patterns = {}
        
        print(f"🚀 Ultimate Enterprise ADF Parser v8.0")
        print(f"📁 Input: {json_path}")
    
    def run(self) -> bool:
        """Main execution pipeline"""
        print("\n" + "="*80)
        print("ENTERPRISE ADF ANALYSIS - FULLY INTEGRATED")
        print("="*80)
        
        try:
            # Phase 1: Load
            if not self.load_template():
                return False
            
            # Phase 2: Register resources
            print("\n📋 Registering resources...")
            self.register_all_resources()
            
            # Phase 3: Discover patterns (if enabled)
            if self.enable_discovery:
                self.discover_patterns()
            
            # Phase 4: Parse all resources
            print("\n🔍 Parsing resources...")
            self.parse_all_resources()
            
            # Phase 5: Extract dependencies
            self.extract_all_dependencies()
            
            # Phase 6: Extract relationships
            print("\n🔗 Extracting data lineage...")
            self.extract_relationships()
            
            # Phase 7: Export
            print("\n💾 Exporting results...")
            self.export_to_excel()
            
            # Phase 8: Summary
            print("\n📈 Summary...")
            self.print_comprehensive_summary()
            
            # Cleanup
            gc.collect()
            
            return True
            
        except Exception as e:
            print(f"\n❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def sanitize_value(self, value: Any, max_length: int = 32767) -> str:
        """Sanitize value for Excel (optimized)"""
        if value is None:
            return ''
        
        # Convert to string
        if isinstance(value, (dict, list)):
            try:
                text = json.dumps(value, default=str)[:max_length]
            except:
                text = str(value)[:max_length]
        else:
            text = str(value)[:max_length]
        
        # Quick clean
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text[:max_length]
    
    def extract_name(self, name_expr: str) -> str:
        """Extract clean name from ARM expression"""
        if not name_expr:
            return ''
        
        name_expr = str(name_expr)
        
        # Fast path
        if "concat" not in name_expr and "/" not in name_expr:
            return name_expr.strip("[]'\"")
        
        # Handle concat expressions
        if "concat(parameters('factoryName')" in name_expr:
            match = re.search(r"'/([^']+)'", name_expr)
            if match:
                return match.group(1)
        
        name_expr = name_expr.strip("[]'\"")
        if '/' in name_expr:
            name_expr = name_expr.split('/')[-1]
        
        return name_expr
    
    def load_template(self) -> bool:
        """Load ARM template"""
        try:
            print("\n📂 Loading template...")
            
            file_size = Path(self.json_path).stat().st_size
            print(f"  Size: {file_size/1024/1024:.2f} MB")
            
            with open(self.json_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            resources = self.data.get('resources', [])
            print(f"✅ Loaded {len(resources)} resources")
            
            return len(resources) > 0
            
        except Exception as e:
            print(f"❌ Load error: {e}")
            return False
    
    def register_all_resources(self):
        """Register all resources for quick lookup"""
        resources = self.data.get('resources', [])
        resource_counts = Counter()
        
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            
            name = self.extract_name(resource.get('name', ''))
            res_type = resource.get('type', '')
            category = res_type.split('/')[-1] if res_type else 'unknown'
            
            resource_counts[category] += 1
            
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
        
        for category, count in resource_counts.most_common(15):
            print(f"  • {category:30} : {count:4d}")
        
        print(f"\n✅ Registered {len(self.resources['all'])} resources")
    
    def parse_all_resources(self):
        """Parse all resources in optimized phases"""
        
        # Phase 1: Infrastructure
        print("  Phase 1/6: Infrastructure...")
        for name, resource in self.resources['integrationruntimes'].items():
            try:
                self.parse_integration_runtime(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 2: Linked Services
        print("  Phase 2/6: Linked Services...")
        for name, resource in self.resources['linkedservices'].items():
            try:
                self.parse_linked_service(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 3: Datasets
        print("  Phase 3/6: Datasets...")
        for name, resource in self.resources['datasets'].items():
            try:
                self.parse_dataset(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 4: DataFlows
        print("  Phase 4/6: DataFlows...")
        for name, resource in self.resources['dataflows'].items():
            try:
                self.parse_dataflow(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 5: Pipelines (with progress bar)
        print("  Phase 5/6: Pipelines...")
        pipeline_items = list(self.resources['pipelines'].items())
        
        if HAS_TQDM and len(pipeline_items) > 10:
            pipeline_items = tqdm(pipeline_items, desc="  Parsing")
        
        for name, resource in pipeline_items:
            try:
                self.parse_pipeline(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 6: Triggers
        print("  Phase 6/6: Triggers...")
        for name, resource in self.resources['triggers'].items():
            try:
                self.parse_trigger(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        print(f"\n✅ Parsed {len(self.results['activities'])} activities from {len(self.results['pipelines'])} pipelines")
    
    # ========================================================================
    # PARSING METHODS (Simplified versions - full code continues in Part 2)
    # ========================================================================
    
    def parse_pipeline(self, resource: dict):
        """Parse pipeline"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            activities = props.get('activities', [])
            
            self.results['pipelines'].append({
                'Pipeline': self.sanitize_value(name),
                'Folder': self.sanitize_value(self.get_nested(props, 'folder.name')),
                'Description': self.sanitize_value(props.get('description', '')),
                'Activities': len(activities) if isinstance(activities, list) else 0,
                'Parameters': self.sanitize_value(self.format_dict(props.get('parameters', {}))),
                'Variables': self.sanitize_value(self.format_dict(props.get('variables', {})))
            })
            
            if isinstance(activities, list):
                for seq, activity in enumerate(activities, 1):
                    try:
                        self.parse_activity(activity, name, seq)
                    except Exception as e:
                        self.log_error(activity, f"Activity: {e}")
        
        except Exception as e:
            self.log_error(resource, f"Pipeline: {e}")
    
    def parse_activity(self, activity: dict, pipeline: str, seq: int):
        """Parse activity (simplified)"""
        if not isinstance(activity, dict):
            return
        
        activity_type = activity.get('type', 'Unknown')
        activity_name = activity.get('name', '')
        type_props = activity.get('typeProperties', {})
        
        self.metrics['activity_types'][activity_type] += 1
        
        rec = {
            'Pipeline': self.sanitize_value(pipeline),
            'Sequence': seq,
            'Activity': self.sanitize_value(activity_name),
            'Activity Type': self.sanitize_value(activity_type),
            'Role': self.get_activity_role(activity_type),
            'Dataset': '',
            'DataFlow': '',
            'LinkedPipeline': '',
            'SQL': '',
            'Values Info': '',
            'Note': self.sanitize_value(activity.get('description', ''))
        }
        
        # Type-specific handling
        if activity_type == 'ExecuteDataFlow':
            dataflow = type_props.get('dataflow', {})
            if isinstance(dataflow, dict):
                rec['DataFlow'] = self.sanitize_value(self.extract_name(dataflow.get('referenceName', '')))
        
        elif activity_type == 'ExecutePipeline':
            pipeline_ref = type_props.get('pipeline', {})
            if isinstance(pipeline_ref, dict):
                rec['LinkedPipeline'] = self.sanitize_value(self.extract_name(pipeline_ref.get('referenceName', '')))
        
        # Extract datasets
        self.extract_datasets(activity, rec)
        
        # Extract SQL
        self.extract_sql(type_props, rec)
        
        self.results['activities'].append(rec)
    
    def get_activity_role(self, activity_type: str) -> str:
        """Get activity role"""
        roles = {
            'Copy': 'Data Movement',
            'ExecuteDataFlow': 'Data Flow',
            'ExecutePipeline': 'Pipeline',
            'Lookup': 'Query',
            'GetMetadata': 'Metadata',
            'Delete': 'Cleanup',
            'ForEach': 'Loop',
            'IfCondition': 'Condition',
            'SetVariable': 'Set Var',
            'WebActivity': 'Web Call'
        }
        return roles.get(activity_type, 'Process')
    
    def extract_datasets(self, activity: dict, rec: dict):
        """Extract datasets"""
        datasets = []
        
        def find_refs(obj, prefix=''):
            if isinstance(obj, dict):
                if obj.get('type') == 'DatasetReference' and 'referenceName' in obj:
                    datasets.append(f"{prefix}{self.extract_name(obj['referenceName'])}")
                else:
                    for key, value in obj.items():
                        new_prefix = 'IN:' if key in ['inputs', 'input'] else 'OUT:' if key in ['outputs', 'output'] else prefix
                        find_refs(value, new_prefix)
            elif isinstance(obj, list):
                for item in obj:
                    find_refs(item, prefix)
        
        find_refs(activity)
        rec['Dataset'] = self.sanitize_value(' | '.join(datasets))
    
    def extract_sql(self, type_props: dict, rec: dict):
        """Extract SQL"""
        sql_keys = ['sqlReaderQuery', 'query', 'text', 'script']
        
        for key in sql_keys:
            if key in type_props:
                sql_text = str(type_props[key])[:500]
                rec['SQL'] = self.sanitize_value(sql_text)
                break
    
    def parse_dataflow(self, resource: dict):
        """Parse dataflow (simplified)"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            flow_type = props.get('type', 'MappingDataFlow')
            type_props = props.get('typeProperties', {})
            
            self.metrics['dataflow_types'][flow_type] += 1
            
            sources = type_props.get('sources', [])
            sinks = type_props.get('sinks', [])
            transformations = type_props.get('transformations', [])
            
            self.results['dataflows'].append({
                'DataFlow': self.sanitize_value(name),
                'Type': self.sanitize_value(flow_type),
                'Sources': len(sources) if isinstance(sources, list) else 0,
                'Sinks': len(sinks) if isinstance(sinks, list) else 0,
                'Transformations': len(transformations) if isinstance(transformations, list) else 0,
                'Folder': self.sanitize_value(self.get_nested(props, 'folder.name')),
                'Description': self.sanitize_value(props.get('description', ''))
            })
        
        except Exception as e:
            self.log_error(resource, f"DataFlow: {e}")
    
    def parse_dataset(self, resource: dict):
        """Parse dataset"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            ds_type = props.get('type', 'Unknown')
            
            self.metrics['dataset_types'][ds_type] += 1
            
            ls = props.get('linkedServiceName', {})
            ls_name = ''
            if isinstance(ls, dict):
                ls_name = self.extract_name(ls.get('referenceName', ''))
            
            self.results['datasets'].append({
                'Dataset': self.sanitize_value(name),
                'Type': self.sanitize_value(ds_type),
                'LinkedService': self.sanitize_value(ls_name),
                'Folder': self.sanitize_value(self.get_nested(props, 'folder.name')),
                'Description': self.sanitize_value(props.get('description', ''))
            })
        
        except Exception as e:
            self.log_error(resource, f"Dataset: {e}")
    
    def parse_linked_service(self, resource: dict):
        """Parse linked service"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            ls_type = props.get('type', 'Unknown')
            
            self.metrics['linked_service_types'][ls_type] += 1
            
            self.results['linked_services'].append({
                'LinkedService': self.sanitize_value(name),
                'Type': self.sanitize_value(ls_type),
                'Description': self.sanitize_value(props.get('description', ''))
            })
        
        except Exception as e:
            self.log_error(resource, f"LinkedService: {e}")
    
    def parse_trigger(self, resource: dict):
        """Parse trigger"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            trigger_type = props.get('type', 'Unknown')
            
            self.metrics['trigger_types'][trigger_type] += 1
            
            pipelines = props.get('pipelines', [])
            pipeline_names = []
            
            if isinstance(pipelines, list):
                for p in pipelines:
                    if isinstance(p, dict):
                        ref = p.get('pipelineReference', {})
                        if isinstance(ref, dict):
                            pname = self.extract_name(ref.get('referenceName', ''))
                            if pname:
                                pipeline_names.append(pname)
                                self.results['trigger_details'].append({
                                    'Trigger': name,
                                    'Pipeline': pname,
                                    'TriggerType': trigger_type
                                })
            
            self.results['triggers'].append({
                'Trigger': self.sanitize_value(name),
                'Type': self.sanitize_value(trigger_type),
                'State': self.sanitize_value(props.get('runtimeState', 'Unknown')),
                'Pipelines': self.sanitize_value(', '.join(pipeline_names[:10])),
                'Description': self.sanitize_value(props.get('description', ''))
            })
        
        except Exception as e:
            self.log_error(resource, f"Trigger: {e}")
    
    def parse_integration_runtime(self, resource: dict):
        """Parse integration runtime"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            ir_type = props.get('type', 'Unknown')
            
            self.results['integration_runtimes'].append({
                'IntegrationRuntime': self.sanitize_value(name),
                'Type': self.sanitize_value(ir_type),
                'Description': self.sanitize_value(props.get('description', ''))
            })
        
        except Exception as e:
            self.log_error(resource, f"IR: {e}")
    
    def extract_relationships(self):
        """Extract data lineage relationships"""
        # Link triggers to activities
        trigger_pipelines = {}
        for detail in self.results['trigger_details']:
            trigger = detail['Trigger']
            pipeline = detail['Pipeline']
            
            if trigger not in trigger_pipelines:
                trigger_pipelines[trigger] = []
            trigger_pipelines[trigger].append(pipeline)
        
        # Add trigger info to activities
        for activity in self.results['activities']:
            pipeline = activity['Pipeline']
            triggers = []
            
            for trigger, pipelines in trigger_pipelines.items():
                if pipeline in pipelines:
                    triggers.append(trigger)
            
            if triggers:
                activity['Triggers'] = self.sanitize_value(', '.join(triggers))
        
        # Extract data lineage for Copy activities
        for activity in self.results['activities']:
            if activity['Activity Type'] == 'Copy':
                dataset = activity.get('Dataset', '')
                if 'IN:' in dataset and 'OUT:' in dataset:
                    parts = dataset.split(' | ')
                    source = next((p.replace('IN:', '') for p in parts if 'IN:' in p), '')
                    sink = next((p.replace('OUT:', '') for p in parts if 'OUT:' in p), '')
                    
                    if source and sink:
                        self.results['data_lineage'].append({
                            'Pipeline': activity['Pipeline'],
                            'Activity': activity['Activity'],
                            'Type': 'Copy',
                            'Source': source,
                            'Sink': sink
                        })
    
    def export_to_excel(self):
        """Export to Excel with automatic sheet splitting for large datasets"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        excel_file = output_dir / f'adf_analysis_enterprise_{timestamp}.xlsx'
        
        print(f"\n💾 Exporting to: {excel_file}")
        
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                
                # Summary
                self._write_summary(writer, timestamp)
                
                # Main data sheets (with auto-split)
                self._write_data_sheets(writer)
                
                # Dependency sheets
                self._write_dependency_sheets(writer)
                
                # Pipeline Analysis (KEY SHEET)
                self._write_pipeline_analysis(writer)
                
                # Statistics
                self._write_statistics(writer)
                
                # Errors
                if self.results['errors']:
                    df = pd.DataFrame(self.results['errors'])
                    df.to_excel(writer, sheet_name='Errors', index=False)
                    print(f"  ⚠ Errors: {len(df)} rows")
            
            print(f"\n✅ Export complete: {excel_file}")
            
        except Exception as e:
            print(f"❌ Export failed: {e}")
            import traceback
            traceback.print_exc()
    
    def _write_summary(self, writer, timestamp):
        """Write summary sheet"""
        summary = [
            {'Metric': 'Analysis Date', 'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
            {'Metric': 'Source File', 'Value': str(self.json_path)},
            {'Metric': '', 'Value': ''},
            {'Metric': '=== RESOURCES ===', 'Value': ''},
            {'Metric': 'Pipelines', 'Value': len(self.resources['pipelines'])},
            {'Metric': 'DataFlows', 'Value': len(self.resources['dataflows'])},
            {'Metric': 'Datasets', 'Value': len(self.resources['datasets'])},
            {'Metric': 'LinkedServices', 'Value': len(self.resources['linkedservices'])},
            {'Metric': 'Triggers', 'Value': len(self.resources['triggers'])},
            {'Metric': 'Integration Runtimes', 'Value': len(self.resources['integrationruntimes'])},
            {'Metric': '', 'Value': ''},
            {'Metric': '=== PARSED DATA ===', 'Value': ''},
            {'Metric': 'Total Activities', 'Value': len(self.results['activities'])},
            {'Metric': '', 'Value': ''},
            {'Metric': '=== DEPENDENCIES ===', 'Value': ''},
            {'Metric': 'Total Dependencies', 'Value': sum(len(d) for d in self.dependencies.values())},
            {'Metric': 'Pipelines with Triggers', 'Value': len(self.dep_stats['pipelines_with_triggers'])},
            {'Metric': 'Pipelines with DataFlows', 'Value': len(self.dep_stats['pipelines_with_dataflows'])},
            {'Metric': 'Pipelines calling Pipelines', 'Value': len(self.dep_stats['pipelines_calling_pipelines'])},
            {'Metric': 'Standalone Pipelines', 'Value': len(self.dep_stats['standalone_pipelines'])},
            {'Metric': 'Orphaned Resources', 'Value': len(self.dep_stats['orphaned_resources'])},
            {'Metric': '', 'Value': ''},
            {'Metric': 'Parse Errors', 'Value': len(self.results['errors'])}
        ]
        
        pd.DataFrame(summary).to_excel(writer, sheet_name='Summary', index=False)
        print(f"  ✓ Summary")
    
    def _write_data_sheets(self, writer):
        """Write data sheets with auto-split"""
        sheets = [
            ('Activities', self.results['activities']),
            ('Pipelines', self.results['pipelines']),
            ('DataFlows', self.results['dataflows']),
            ('Datasets', self.results['datasets']),
            ('LinkedServices', self.results['linked_services']),
            ('Triggers', self.results['triggers']),
            ('IntegrationRuntimes', self.results['integration_runtimes'])
        ]
        
        for sheet_name, data in sheets:
            if data:
                self._write_with_split(writer, sheet_name, data)
    
    def _write_with_split(self, writer, sheet_name: str, data: List[Dict]):
        """Write data with automatic splitting if too large"""
        if len(data) <= self.SHEET_SPLIT_THRESHOLD:
            # Normal write
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            print(f"  ✓ {sheet_name}: {len(df)} rows")
        else:
            # Split into multiple sheets
            num_parts = (len(data) // self.SHEET_SPLIT_THRESHOLD) + 1
            for i in range(num_parts):
                start_idx = i * self.SHEET_SPLIT_THRESHOLD
                end_idx = min((i + 1) * self.SHEET_SPLIT_THRESHOLD, len(data))
                
                part_data = data[start_idx:end_idx]
                part_sheet_name = f"{sheet_name}_P{i+1}"[:31]
                
                df = pd.DataFrame(part_data)
                df.to_excel(writer, sheet_name=part_sheet_name, index=False)
                print(f"  ✓ {part_sheet_name}: {len(df)} rows")
    
    def _write_dependency_sheets(self, writer):
        """Write dependency sheets"""
        dep_sheets = [
            ('ARM_DependsOn', self.dependencies['arm_depends_on']),
            ('Trigger_Pipeline', self.dependencies['trigger_to_pipeline']),
            ('Pipeline_DataFlow', self.dependencies['pipeline_to_dataflow']),
            ('Pipeline_Pipeline', self.dependencies['pipeline_to_pipeline'])
        ]
        
        for sheet_name, data in dep_sheets:
            if data:
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                print(f"  ✓ {sheet_name}: {len(df)} rows")
    
    def _write_pipeline_analysis(self, writer):
        """Write pipeline analysis sheet"""
        pipeline_analysis = self.generate_pipeline_analysis()
        
        if pipeline_analysis:
            df = pd.DataFrame(pipeline_analysis)
            df = df.sort_values(['Has_Trigger', 'Has_DataFlow'], ascending=[False, False])
            df.to_excel(writer, sheet_name='Pipeline_Analysis', index=False)
            print(f"  ✓ Pipeline_Analysis: {len(df)} rows (KEY SHEET)")
    
    def _write_statistics(self, writer):
        """Write statistics sheet"""
        stats_data = []
        
        for category, counter in [
            ('Activity', self.metrics['activity_types']),
            ('DataFlow', self.metrics['dataflow_types']),
            ('Dataset', self.metrics['dataset_types']),
            ('LinkedService', self.metrics['linked_service_types']),
            ('Trigger', self.metrics['trigger_types'])
        ]:
            for item_type, count in counter.most_common():
                stats_data.append({
                    'Category': category,
                    'Type': item_type,
                    'Count': count
                })
        
        if stats_data:
            df = pd.DataFrame(stats_data)
            df.to_excel(writer, sheet_name='Statistics', index=False)
            print(f"  ✓ Statistics")
    
    def print_comprehensive_summary(self):
        """Print comprehensive summary"""
        print("\n" + "="*80)
        print("ENTERPRISE ADF ANALYSIS COMPLETE")
        print("="*80)
        
        print(f"\n📊 RESOURCES:")
        print(f"  • Pipelines: {len(self.resources['pipelines'])}")
        print(f"  • DataFlows: {len(self.resources['dataflows'])}")
        print(f"  • Datasets: {len(self.resources['datasets'])}")
        print(f"  • LinkedServices: {len(self.resources['linkedservices'])}")
        print(f"  • Triggers: {len(self.resources['triggers'])}")
        print(f"  • Integration Runtimes: {len(self.resources['integrationruntimes'])}")
        
        print(f"\n🔗 DEPENDENCIES:")
        for dep_type, deps in self.dependencies.items():
            if deps:
                print(f"  • {dep_type:25} : {len(deps):5d}")
        print(f"  • {'TOTAL':25} : {sum(len(d) for d in self.dependencies.values()):5d}")
        
        print(f"\n📈 PATTERNS:")
        print(f"  • Pipelines with Triggers: {len(self.dep_stats['pipelines_with_triggers'])}")
        print(f"  • Pipelines with DataFlows: {len(self.dep_stats['pipelines_with_dataflows'])}")
        print(f"  • Pipelines calling Pipelines: {len(self.dep_stats['pipelines_calling_pipelines'])}")
        print(f"  • Standalone Pipelines: {len(self.dep_stats['standalone_pipelines'])}")
        print(f"  • Orphaned Resources: {len(self.dep_stats['orphaned_resources'])}")
        
        print(f"\n⚡ TOP ACTIVITY TYPES:")
        for activity_type, count in self.metrics['activity_types'].most_common(10):
            print(f"  • {activity_type:30} : {count:4d}")
        
        if self.results['errors']:
            print(f"\n⚠️  Parse Errors: {len(self.results['errors'])}")
    
    # Helper methods
    
    def get_nested(self, obj: dict, path: str, default: Any = '') -> Any:
        """Get nested value"""
        try:
            keys = path.split('.')
            value = obj
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return default
            return value if value is not None else default
        except:
            return default
    
    def format_dict(self, d: dict) -> str:
        """Format dictionary"""
        if not isinstance(d, dict):
            return ''
        
        items = [str(k) for k in list(d.keys())[:10]]
        result = ', '.join(items)
        if len(d) > 10:
            result += f" (+{len(d)-10} more)"
        
        return result
    
    def log_error(self, resource: Any, error: str):
        """Log error"""
        self.results['errors'].append({
            'Resource': self.sanitize_value(str(resource.get('name', 'Unknown'))[:100] if isinstance(resource, dict) else 'Unknown'),
            'Error': self.sanitize_value(error[:500])
        })


def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║    Ultimate Enterprise ADF Parser v8.0 - Fully Integrated                   ║
║    Parsing + Dependency Tracking + Pattern Discovery                        ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Usage: python adf_parser_enterprise_v8.py <template.json> [--no-discovery]

Features:
  ✅ Complete resource parsing (Pipelines, DataFlows, Datasets, etc.)
  ✅ Comprehensive dependency tracking (9000+ dependencies)
  ✅ Pipeline analysis (triggers, dataflows, calls)
  ✅ Auto-discovery of patterns
  ✅ Automatic sheet splitting for large outputs
  ✅ Orphaned resource detection
  ✅ Data lineage tracking

Key Output Sheets:
  • Summary - Overall statistics
  • Pipeline_Analysis - ⭐ Main sheet with all dependencies
  • Activities - All activities (auto-split if >500k rows)
  • Pipelines, DataFlows, Datasets, LinkedServices, Triggers
  • Dependency sheets (ARM, Trigger, Pipeline relationships)
  • Statistics - Type distributions

Example:
  python adf_parser_enterprise_v8.py factory_arm_template.json
  python adf_parser_enterprise_v8.py factory_arm_template.json --no-discovery
        """)
        sys.exit(1)
    
    json_path = sys.argv[1]
    enable_discovery = '--no-discovery' not in sys.argv
    
    parser = UltimateEnterpriseADFParser(json_path, enable_discovery=enable_discovery)
    success = parser.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()