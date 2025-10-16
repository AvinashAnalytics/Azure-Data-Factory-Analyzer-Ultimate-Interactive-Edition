"""
ULTIMATE Azure Data Factory ARM Template Parser v7.0
Enhanced with Auto-Discovery, DataFlow support, and Dynamic Pattern Learning
Handles 75MB+ files with 4000+ resources including DataFlows
Auto-discovers new patterns and generates parsing logic dynamically
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
warnings.filterwarnings('ignore')

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class PatternDiscoveryMixin:
    """Mixin for pattern discovery capabilities"""
    
    def discover_patterns(self):
        """Auto-discover patterns in the template"""
        print("\n🔍 Auto-discovering patterns...")
        
        self.discovered_patterns = {
            'resource_types': Counter(),
            'property_paths': defaultdict(set),
            'reference_types': Counter(),
            'expression_functions': Counter(),
            'nested_structures': defaultdict(dict)
        }
        
        resources = self.data.get('resources', [])
        
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            
            res_type = resource.get('type', '')
            category = self._extract_category(res_type)
            self.discovered_patterns['resource_types'][category] += 1
            
            # Discover property paths
            self._discover_property_paths(resource, f"{category}", category)
            
            # Discover expressions
            self._discover_expressions(resource)
        
        # Generate dynamic parsers for unknown types
        self._generate_dynamic_parsers()
        
        print(f"✅ Discovered {len(self.discovered_patterns['resource_types'])} resource types")
    
    def _extract_category(self, res_type: str) -> str:
        """Extract category from resource type"""
        if not res_type:
            return 'unknown'
        parts = res_type.split('/')
        return parts[-1] if parts else 'unknown'
    
    def _discover_property_paths(self, obj: Any, path: str, category: str, depth: int = 0):
        """Discover property paths in resources"""
        if depth > 5 or not isinstance(obj, dict):
            return
        
        for key, value in obj.items():
            current_path = f"{path}.{key}"
            self.discovered_patterns['property_paths'][category].add(current_path)
            
            if isinstance(value, dict):
                self._discover_property_paths(value, current_path, category, depth + 1)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                self._discover_property_paths(value[0], f"{current_path}[]", category, depth + 1)
    
    def _discover_expressions(self, obj: Any):
        """Discover ADF expression functions"""
        if isinstance(obj, str):
            # Find function calls
            functions = re.findall(r'@(\w+)\s*KATEX_INLINE_OPEN', obj)
            for func in functions:
                self.discovered_patterns['expression_functions'][func] += 1
        elif isinstance(obj, dict):
            for value in obj.values():
                self._discover_expressions(value)
        elif isinstance(obj, list):
            for item in obj[:50]:
                self._discover_expressions(item)
    
    def _generate_dynamic_parsers(self):
        """Generate dynamic parsers for discovered but unhandled types"""
        known_types = {
            'pipelines', 'dataflows', 'datasets', 'linkedServices',
            'triggers', 'integrationRuntimes', 'managedVirtualNetworks',
            'managedPrivateEndpoints', 'credentials'
        }
        
        unknown_types = set(self.discovered_patterns['resource_types'].keys()) - known_types
        
        if unknown_types:
            print(f"\n🆕 Found {len(unknown_types)} unknown resource types:")
            for utype in sorted(unknown_types):
                count = self.discovered_patterns['resource_types'][utype]
                print(f"  • {utype}: {count}")
                
                # Create dynamic parser
                self._create_dynamic_parser(utype)
    
    def _create_dynamic_parser(self, resource_type: str):
        """Dynamically create parser for unknown resource type"""
        def dynamic_parser(resource: dict):
            try:
                name = self.extract_name(resource.get('name', ''))
                props = resource.get('properties', {})
                
                rec = {
                    'ResourceType': self.sanitize_value(resource_type),
                    'Name': self.sanitize_value(name),
                    'Type': self.sanitize_value(props.get('type', '')),
                    'Description': self.sanitize_value(props.get('description', '')),
                    'Properties': self.sanitize_value(json.dumps(props, default=str)[:1000])
                }
                
                # Store in dynamic results
                result_key = f'dynamic_{resource_type.lower()}'
                if result_key not in self.results:
                    self.results[result_key] = []
                self.results[result_key].append(rec)
                
            except Exception as e:
                self.log_error(resource, f"Dynamic_{resource_type}: {e}")
        
        # Register the dynamic parser
        setattr(self, f'parse_dynamic_{resource_type.lower()}', dynamic_parser)


class UltimateADFParser(PatternDiscoveryMixin):
    """Complete ADF ARM Template Parser with Auto-Discovery and DataFlow enhancements"""
    
    # ARM Template Schema versions supported
    SUPPORTED_SCHEMAS = [
        "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
        "https://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
        "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#"
    ]
    
    def __init__(self, json_path: str, enable_discovery: bool = True):
        self.json_path = json_path
        self.data = None
        self.enable_discovery = enable_discovery
        
        # Results storage - Enhanced with more categories
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
            'dataflow_transformations': [],  # NEW: Detailed transformation info
            'parameters': [],
            'dependencies': [],
            'data_lineage': [],
            'managed_virtual_networks': [],
            'managed_private_endpoints': [],
            'credentials': [],
            'statistics': {},
            'errors': []
        }
        
        # Metrics - Enhanced
        self.metrics = {
            'activity_types': Counter(),
            'dataset_types': Counter(),
            'trigger_types': Counter(),
            'linked_service_types': Counter(),
            'dataflow_types': Counter(),
            'transformation_types': Counter(),
            'source_types': Counter(),
            'sink_types': Counter()
        }
        
        # References tracking
        self.dataflow_references = {}
        self.dataset_references = {}
        self.linkedservice_references = {}
        self.pipeline_references = {}
        
        # Discovery patterns (if enabled)
        self.discovered_patterns = {}
        
        print(f"🚀 Ultimate ADF Parser v7.0 - With Auto-Discovery & DataFlow Support")
        print(f"📁 Input: {json_path}")
    
    def sanitize_value(self, value: Any, max_length: int = 32767) -> str:
        """Sanitize any value for Excel export"""
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
        
        # Remove illegal characters
        text = ''.join(char if char.isprintable() or char in '\n\r\t' else ' ' for char in text)
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', text)
        
        # Clean unicode
        try:
            text = unicodedata.normalize('NFKD', text)
            text = text.encode('ascii', 'ignore').decode('ascii')
        except:
            text = re.sub(r'[^\x20-\x7E\n\r\t]', ' ', text)
        
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text[:max_length]
    
    def run(self) -> bool:
        """Main execution"""
        print("\n" + "="*80)
        print("AZURE DATA FACTORY ARM TEMPLATE ANALYSIS")
        print("="*80)
        
        try:
            # Load
            if not self.load_template():
                return False
            
            # Auto-discover patterns (if enabled)
            if self.enable_discovery:
                self.discover_patterns()
            
            # Parse
            self.parse_all_resources()
            
            # Analyze
            self.extract_relationships()
            
            # Export
            self.export_to_excel()
            
            # Summary
            self.print_summary()
            
            return True
            
        except Exception as e:
            print(f"\n❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def load_template(self) -> bool:
        """Load and validate ARM template"""
        try:
            print("\n📂 Loading template...")
            
            file_size = Path(self.json_path).stat().st_size
            print(f"  Size: {file_size/1024/1024:.2f} MB")
            
            with open(self.json_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            # Validate schema
            schema = self.data.get('$schema', '')
            if schema in self.SUPPORTED_SCHEMAS:
                print(f"✅ Schema validated: {schema.split('/')[-2]}")
            else:
                print(f"⚠️  Unknown schema: {schema}")
            
            resources = self.data.get('resources', [])
            print(f"✅ Loaded {len(resources)} resources")
            
            return len(resources) > 0
            
        except Exception as e:
            print(f"❌ Load error: {e}")
            return False
    
    def parse_all_resources(self):
        """Parse all resources with multi-phase processing"""
        print("\n🔍 Parsing resources...")
        
        resources = self.data.get('resources', [])
        
        # Count types
        type_counts = Counter()
        for res in resources:
            if isinstance(res, dict):
                res_type = res.get('type', '').split('/')[-1]
                type_counts[res_type] += 1
        
        print("\n📊 Resource distribution:")
        for res_type, count in type_counts.most_common(15):
            print(f"  • {res_type:30} : {count:4d}")
        
        # Multi-phase parsing
        print("\n⚙️ Processing in phases...")
        
        # Phase 1: Parse infrastructure (Integration Runtimes, VNets, etc.)
        print("  Phase 1: Infrastructure...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'integrationRuntimes' in res_type:
                        self.parse_integration_runtime(resource)
                    elif 'managedVirtualNetworks' in res_type:
                        self.parse_managed_virtual_network(resource)
                    elif 'credentials' in res_type:
                        self.parse_credential(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 2: Parse linked services
        print("  Phase 2: Linked Services...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'linkedServices' in res_type:
                        self.parse_linked_service(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 3: Parse datasets
        print("  Phase 3: Datasets...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'datasets' in res_type:
                        self.parse_dataset(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 4: Parse dataflows
        print("  Phase 4: DataFlows...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'dataflows' in res_type:
                        self.parse_dataflow(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 5: Parse pipelines
        print("  Phase 5: Pipelines...")
        iterator = [r for r in resources if isinstance(r, dict) and 'pipelines' in r.get('type', '')]
        if HAS_TQDM and len(iterator) > 10:
            iterator = tqdm(iterator, desc="Parsing Pipelines")
        
        for resource in iterator:
            try:
                self.parse_pipeline(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 6: Parse triggers
        print("  Phase 6: Triggers...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'triggers' in res_type:
                        self.parse_trigger(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 7: Parse unknown/dynamic types
        if self.enable_discovery and self.discovered_patterns:
            print("  Phase 7: Dynamic Resources...")
            for resource in resources:
                try:
                    if isinstance(resource, dict):
                        res_type = resource.get('type', '')
                        category = self._extract_category(res_type)
                        
                        # Check if we have a dynamic parser
                        parser_method = getattr(self, f'parse_dynamic_{category.lower()}', None)
                        if parser_method and callable(parser_method):
                            parser_method(resource)
                except Exception as e:
                    self.log_error(resource, str(e))
        
        print(f"\n✅ Parsing complete")
    
    def parse_dataflow(self, resource: dict):
        """Parse data flow with complete transformation extraction"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            flow_type = props.get('type', 'MappingDataFlow')
            type_props = props.get('typeProperties', {})
            
            # Count type
            self.metrics['dataflow_types'][flow_type] += 1
            
            # Track this dataflow for later reference
            self.dataflow_references[name] = resource
            
            # Parse sources
            sources = type_props.get('sources', [])
            source_info = []
            for source in sources if isinstance(sources, list) else []:
                if isinstance(source, dict):
                    source_name = source.get('name', '')
                    
                    # Linked service
                    linked_service = source.get('linkedService', {})
                    ls_name = ''
                    if isinstance(linked_service, dict):
                        ls_name = self.extract_name(linked_service.get('referenceName', ''))
                    
                    # Dataset
                    dataset = source.get('dataset', {})
                    ds_name = ''
                    if isinstance(dataset, dict):
                        ds_name = self.extract_name(dataset.get('referenceName', ''))
                    
                    # Schema info
                    schema_linked_service = source.get('schemaLinkedService', {})
                    schema_ls = ''
                    if isinstance(schema_linked_service, dict):
                        schema_ls = self.extract_name(schema_linked_service.get('referenceName', ''))
                    
                    source_info.append({
                        'name': source_name,
                        'linkedService': ls_name,
                        'dataset': ds_name,
                        'schemaLinkedService': schema_ls
                    })
                    
                    # Track source type
                    self.metrics['source_types'][source_name] += 1
            
            # Parse sinks
            sinks = type_props.get('sinks', [])
            sink_info = []
            for sink in sinks if isinstance(sinks, list) else []:
                if isinstance(sink, dict):
                    sink_name = sink.get('name', '')
                    
                    # Linked service
                    linked_service = sink.get('linkedService', {})
                    ls_name = ''
                    if isinstance(linked_service, dict):
                        ls_name = self.extract_name(linked_service.get('referenceName', ''))
                    
                    # Dataset
                    dataset = sink.get('dataset', {})
                    ds_name = ''
                    if isinstance(dataset, dict):
                        ds_name = self.extract_name(dataset.get('referenceName', ''))
                    
                    # Schema linked service
                    schema_linked_service = sink.get('schemaLinkedService', {})
                    schema_ls = ''
                    if isinstance(schema_linked_service, dict):
                        schema_ls = self.extract_name(schema_linked_service.get('referenceName', ''))
                    
                    sink_info.append({
                        'name': sink_name,
                        'linkedService': ls_name,
                        'dataset': ds_name,
                        'schemaLinkedService': schema_ls
                    })
                    
                    # Track sink type
                    self.metrics['sink_types'][sink_name] += 1
            
            # Parse transformations
            transformations = type_props.get('transformations', [])
            transformation_details = []
            
            for trans in transformations if isinstance(transformations, list) else []:
                if isinstance(trans, dict):
                    trans_name = trans.get('name', '')
                    trans_desc = trans.get('description', '')
                    
                    transformation_details.append({
                        'dataflow': name,
                        'name': trans_name,
                        'description': trans_desc
                    })
            
            # Parse script lines to extract transformation types
            script_lines = type_props.get('scriptLines', [])
            script_text = '\n'.join(str(line) for line in script_lines[:500]) if isinstance(script_lines, list) else ''
            
            transformation_types = []
            if script_text:
                # Enhanced transformation pattern detection
                trans_patterns = {
                    r'\bsource\s*KATEX_INLINE_OPEN': 'Source',
                    r'\bsink\s*KATEX_INLINE_OPEN': 'Sink',
                    r'\bselect\s*KATEX_INLINE_OPEN': 'Select',
                    r'\bderive\s*KATEX_INLINE_OPEN': 'DerivedColumn',
                    r'\baggregate\s*KATEX_INLINE_OPEN': 'Aggregate',
                    r'\bjoin\s*KATEX_INLINE_OPEN': 'Join',
                    r'\bfilter\s*KATEX_INLINE_OPEN': 'Filter',
                    r'\bsort\s*KATEX_INLINE_OPEN': 'Sort',
                    r'\bsplit\s*KATEX_INLINE_OPEN': 'ConditionalSplit',
                    r'\bunion\s*KATEX_INLINE_OPEN': 'Union',
                    r'\bpivot\s*KATEX_INLINE_OPEN': 'Pivot',
                    r'\bunpivot\s*KATEX_INLINE_OPEN': 'Unpivot',
                    r'\bwindow\s*KATEX_INLINE_OPEN': 'Window',
                    r'\brank\s*KATEX_INLINE_OPEN': 'Rank',
                    r'\blookup\s*KATEX_INLINE_OPEN': 'Lookup',
                    r'\bexists\s*KATEX_INLINE_OPEN': 'Exists',
                    r'\balter\s*KATEX_INLINE_OPEN': 'AlterRow',
                    r'\bflatten\s*KATEX_INLINE_OPEN': 'Flatten',
                    r'\bparse\s*KATEX_INLINE_OPEN': 'Parse',
                    r'\bsurrogateKey\s*KATEX_INLINE_OPEN': 'SurrogateKey',
                    r'\bassert\s*KATEX_INLINE_OPEN': 'Assert'
                }
                
                for pattern, trans_type in trans_patterns.items():
                    if re.search(pattern, script_text, re.IGNORECASE):
                        transformation_types.append(trans_type)
                        self.metrics['transformation_types'][trans_type] += 1
            
            # Create dataflow record
            dataflow_rec = {
                'DataFlow': self.sanitize_value(name),
                'Type': self.sanitize_value(flow_type),
                'Sources': len(sources) if isinstance(sources, list) else 0,
                'Sinks': len(sinks) if isinstance(sinks, list) else 0,
                'Transformations': len(transformations) if isinstance(transformations, list) else 0,
                'ScriptLines': len(script_lines) if isinstance(script_lines, list) else 0,
                'SourceNames': self.sanitize_value(', '.join([s['name'] for s in source_info])),
                'SourceLinkedServices': self.sanitize_value(', '.join([s['linkedService'] for s in source_info if s['linkedService']])),
                'SourceDatasets': self.sanitize_value(', '.join([s['dataset'] for s in source_info if s['dataset']])),
                'SinkNames': self.sanitize_value(', '.join([s['name'] for s in sink_info])),
                'SinkLinkedServices': self.sanitize_value(', '.join([s['linkedService'] for s in sink_info if s['linkedService']])),
                'SinkDatasets': self.sanitize_value(', '.join([s['dataset'] for s in sink_info if s['dataset']])),
                'TransformationNames': self.sanitize_value(', '.join([t['name'] for t in transformation_details])),
                'TransformationTypes': self.sanitize_value(', '.join(set(transformation_types))),
                'Description': self.sanitize_value(props.get('description', '')),
                'Folder': self.sanitize_value(self.get_nested(props, 'folder.name')),
                'Annotations': self.sanitize_value(', '.join(str(a) for a in props.get('annotations', [])))
            }
            
            self.results['dataflows'].append(dataflow_rec)
            
            # Store transformation details
            for trans_detail in transformation_details:
                self.results['dataflow_transformations'].append({
                    'DataFlow': name,
                    'TransformationName': trans_detail['name'],
                    'Description': trans_detail['description']
                })
            
            # Create dataflow lineage records
            for source in source_info:
                for sink in sink_info:
                    self.results['dataflow_lineage'].append({
                        'DataFlow': name,
                        'SourceName': source['name'],
                        'SourceLinkedService': source['linkedService'],
                        'SourceDataset': source['dataset'],
                        'SinkName': sink['name'],
                        'SinkLinkedService': sink['linkedService'],
                        'SinkDataset': sink['dataset'],
                        'TransformationCount': len(transformations),
                        'TransformationTypes': ', '.join(set(transformation_types))
                    })
            
        except Exception as e:
            self.log_error(resource, f"DataFlow: {e}")
    
    def parse_pipeline(self, resource: dict):
        """Parse pipeline with complete activity extraction"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            activities = props.get('activities', [])
            
            # Track pipeline
            self.pipeline_references[name] = resource
            
            # Pipeline record
            pipeline_rec = {
                'Pipeline': self.sanitize_value(name),
                'Folder': self.sanitize_value(self.get_nested(props, 'folder.name')),
                'Description': self.sanitize_value(props.get('description', '')),
                'Activities': len(activities) if isinstance(activities, list) else 0,
                'Parameters': self.sanitize_value(self.format_dict(props.get('parameters', {}))),
                'Variables': self.sanitize_value(self.format_dict(props.get('variables', {}))),
                'Concurrency': props.get('concurrency', 'Default'),
                'Annotations': self.sanitize_value(', '.join(str(a) for a in props.get('annotations', []))),
                'Policy': self.sanitize_value(json.dumps(props.get('policy', {}), default=str) if props.get('policy') else '')
            }
            
            self.results['pipelines'].append(pipeline_rec)
            
            # Parse activities
            if isinstance(activities, list):
                for seq, activity in enumerate(activities, 1):
                    try:
                        self.parse_activity(activity, name, seq)
                    except Exception as e:
                        self.log_error(activity, f"Activity: {e}")
            
        except Exception as e:
            self.log_error(resource, f"Pipeline: {e}")
    
    def parse_activity(self, activity: dict, pipeline: str, seq: int):
        """Parse activity with dynamic value extraction and DataFlow linking"""
        if not isinstance(activity, dict):
            return
        
        activity_type = activity.get('type', 'Unknown')
        activity_name = activity.get('name', '')
        type_props = activity.get('typeProperties', {})
        
        # Count type
        self.metrics['activity_types'][activity_type] += 1
        
        # Dynamic role detection
        role = self.get_dynamic_role(activity)
        
        # Initialize record
        rec = {
            'Pipeline': self.sanitize_value(pipeline),
            'Sequence': seq,
            'Activity': self.sanitize_value(activity_name),
            'Activity Type': self.sanitize_value(activity_type),
            'Role': self.sanitize_value(role),
            'Dataset': '',
            'DataFlow': '',
            'LinkedPipeline': '',  # NEW: Track ExecutePipeline
            'SQL': '',
            'Tables': '',
            'Columns': '',
            'Dataset File': '',
            'Parameters': '',
            'Triggers': '',
            'Values Info': '',
            'Note': self.sanitize_value(activity.get('description', ''))
        }
        
        # Special handling for ExecuteDataFlow activities
        if activity_type == 'ExecuteDataFlow':
            dataflow = type_props.get('dataflow', {})
            if isinstance(dataflow, dict):
                dataflow_name = self.extract_name(dataflow.get('referenceName', ''))
                rec['DataFlow'] = self.sanitize_value(dataflow_name)
                rec['Role'] = f"DataFlow: {dataflow_name[:30]}"
                
                # Compute properties
                compute = type_props.get('compute', {})
                if isinstance(compute, dict):
                    compute_type = compute.get('computeType', '')
                    core_count = compute.get('coreCount', '')
                    if compute_type or core_count:
                        rec['Values Info'] = self.sanitize_value(f"Compute: {compute_type} ({core_count} cores)")
                
                # Staging info
                staging = type_props.get('staging', {})
                if isinstance(staging, dict):
                    linked_service = staging.get('linkedService', {})
                    if isinstance(linked_service, dict):
                        staging_ls = self.extract_name(linked_service.get('referenceName', ''))
                        folder = staging.get('folderPath', '')
                        if staging_ls:
                            staging_info = f"Staging: {staging_ls}"
                            if folder:
                                staging_info += f" ({folder})"
                            if rec['Values Info']:
                                rec['Values Info'] += ' | ' + self.sanitize_value(staging_info)
                            else:
                                rec['Values Info'] = self.sanitize_value(staging_info)
                
                # Integration runtime
                ir = type_props.get('integrationRuntime', {})
                if isinstance(ir, dict):
                    ir_name = self.extract_name(ir.get('referenceName', ''))
                    if ir_name:
                        ir_info = f"IR: {ir_name}"
                        if rec['Values Info']:
                            rec['Values Info'] += ' | ' + self.sanitize_value(ir_info)
                        else:
                            rec['Values Info'] = self.sanitize_value(ir_info)
        
        # Special handling for ExecutePipeline
        elif activity_type == 'ExecutePipeline':
            pipeline_ref = type_props.get('pipeline', {})
            if isinstance(pipeline_ref, dict):
                linked_pipeline = self.extract_name(pipeline_ref.get('referenceName', ''))
                rec['LinkedPipeline'] = self.sanitize_value(linked_pipeline)
                rec['Role'] = f"Execute: {linked_pipeline[:30]}"
                
                # Wait on completion
                wait = type_props.get('waitOnCompletion', True)
                rec['Values Info'] = self.sanitize_value(f"WaitOnCompletion: {wait}")
        
        # Extract datasets dynamically
        self.extract_datasets_dynamic(activity, rec)
        
        # Extract SQL dynamically
        self.extract_sql_dynamic(activity, type_props, rec)
        
        # Extract files dynamically
        self.extract_files_dynamic(type_props, rec)
        
        # Extract type-specific values
        self.extract_values_dynamic(activity_type, type_props, rec)
        
        # Extract parameters
        self.extract_parameters_dynamic(activity, rec)
        
        # Extract dependencies
        self.extract_dependencies_dynamic(activity, rec)
        
        self.results['activities'].append(rec)
    
    def get_dynamic_role(self, activity: dict) -> str:
        """Dynamically determine activity role"""
        activity_type = activity.get('type', '')
        type_props = activity.get('typeProperties', {})
        
        # Base roles
        roles = {
            'Copy': 'Data Movement',
            'Delete': 'Data Cleanup',
            'GetMetadata': 'Metadata',
            'Lookup': 'Query',
            'Script': 'SQL Script',
            'SqlServerStoredProcedure': 'Stored Proc',
            'ExecutePipeline': 'Pipeline',
            'ForEach': 'Loop',
            'IfCondition': 'Condition',
            'Switch': 'Switch',
            'Until': 'Until',
            'Wait': 'Wait',
            'SetVariable': 'Set Var',
            'AppendVariable': 'Append Var',
            'Filter': 'Filter',
            'WebActivity': 'Web Call',
            'WebHook': 'WebHook',
            'DatabricksNotebook': 'Databricks',
            'DatabricksSparkJar': 'Databricks Jar',
            'DatabricksSparkPython': 'Databricks Python',
            'ExecuteDataFlow': 'Data Flow',
            'AzureFunctionActivity': 'Azure Function',
            'AzureMLBatchExecution': 'ML Batch',
            'AzureMLUpdateResource': 'ML Update',
            'AzureMLExecutePipeline': 'ML Pipeline',
            'Fail': 'Fail',
            'Validation': 'Validate'
        }
        
        role = roles.get(activity_type, 'Process')
        
        # Enhance based on properties
        if activity_type == 'Copy' and isinstance(type_props, dict):
            source = type_props.get('source', {})
            sink = type_props.get('sink', {})
            if isinstance(source, dict) and isinstance(sink, dict):
                source_type = source.get('type', '?')
                sink_type = sink.get('type', '?')
                role = f"{source_type}→{sink_type}"
        
        elif activity_type == 'WebActivity' and isinstance(type_props, dict):
            method = type_props.get('method', 'GET')
            role = f"Web {method}"
        
        elif activity_type == 'ExecutePipeline' and isinstance(type_props, dict):
            pipeline = type_props.get('pipeline', {})
            if isinstance(pipeline, dict):
                pname = self.extract_name(pipeline.get('referenceName', ''))
                if pname:
                    role = f"Execute: {pname[:20]}"
        
        elif activity_type == 'ExecuteDataFlow' and isinstance(type_props, dict):
            dataflow = type_props.get('dataflow', {})
            if isinstance(dataflow, dict):
                dfname = self.extract_name(dataflow.get('referenceName', ''))
                if dfname:
                    role = f"DataFlow: {dfname[:20]}"
        
        return role
    
    def extract_datasets_dynamic(self, activity: dict, rec: dict):
        """Extract datasets from any location dynamically"""
        datasets = []
        
        # Search everywhere for dataset references
        def find_datasets(obj, prefix=''):
            if isinstance(obj, dict):
                # Direct dataset reference
                if 'referenceName' in obj and 'type' in obj:
                    if obj.get('type') == 'DatasetReference':
                        datasets.append(f"{prefix}{self.extract_name(obj['referenceName'])}")
                
                # Recursive search
                for key, value in obj.items():
                    if key in ['inputs', 'input']:
                        find_datasets(value, 'IN:')
                    elif key in ['outputs', 'output']:
                        find_datasets(value, 'OUT:')
                    elif key == 'dataset':
                        find_datasets(value, '')
                    else:
                        find_datasets(value, prefix)
            elif isinstance(obj, list):
                for item in obj:
                    find_datasets(item, prefix)
        
        find_datasets(activity)
        rec['Dataset'] = self.sanitize_value(' | '.join(datasets))
    
    def extract_sql_dynamic(self, activity: dict, type_props: dict, rec: dict):
        """Extract SQL from any location dynamically"""
        # Fixed regex patterns (removed KATEX errors)
        sql_keys = [
            'sqlReaderQuery', 'query', 'text', 'sqlQuery', 'script',
            'preCopyScript', 'postCopyScript', 'sqlWriterQuery'
        ]
        
        sql_text = ''
        
        # Search in type properties
        if isinstance(type_props, dict):
            for key in sql_keys:
                value = self.search_nested(type_props, key)
                if value:
                    sql_text = self.extract_value(value)
                    break
        
        # Search for stored procedure
        if not sql_text:
            sp_name = self.search_nested(type_props, 'storedProcedureName')
            if sp_name:
                sp_text = self.extract_value(sp_name)
                sp_params = self.search_nested(type_props, 'storedProcedureParameters')
                
                if sp_params and isinstance(sp_params, dict):
                    params = ', '.join([f"@{k}" for k in list(sp_params.keys())[:10]])
                    sql_text = f"EXEC {sp_text} {params}"
                else:
                    sql_text = f"EXEC {sp_text}"
        
        if sql_text:
            rec['SQL'] = self.sanitize_value(sql_text, 500)
            
            # Parse SQL for tables and columns
            tables, columns = self.parse_sql_dynamic(sql_text)
            rec['Tables'] = self.sanitize_value(', '.join(tables[:10]))
            rec['Columns'] = self.sanitize_value(', '.join(columns[:20]))
    
    def extract_files_dynamic(self, type_props: dict, rec: dict):
        """Extract file paths from any location"""
        paths = []
        
        # Search for file-related keys
        file_keys = [
            'fileName', 'folderPath', 'container', 'directory',
            'wildcardFileName', 'wildcardFolderPath', 'filePath',
            'notebookPath', 'scriptPath', 'pythonFile', 'jarFile',
            'relativePath', 'prefix'
        ]
        
        for key in file_keys:
            value = self.search_nested(type_props, key)
            if value:
                paths.append(self.extract_value(value))
        
        if paths:
            rec['Dataset File'] = self.sanitize_value(' | '.join(paths))
    
    def extract_values_dynamic(self, activity_type: str, type_props: dict, rec: dict):
        """Extract additional values based on activity type"""
        values = []
        
        # Generic value extraction
        value_keys = {
            'firstRowOnly': lambda v: f"FirstRow:{v}",
            'isSequential': lambda v: f"Sequential:{v}",
            'batchCount': lambda v: f"Batch:{v}",
            'waitTimeInSeconds': lambda v: f"Wait:{v}s",
            'waitOnCompletion': lambda v: f"WaitComplete:{v}",
            'enableStaging': lambda v: f"Staging:{v}",
            'parallelCopies': lambda v: f"Parallel:{v}",
            'method': lambda v: f"Method:{v}",
            'recursive': lambda v: f"Recursive:{v}",
            'maxConcurrentConnections': lambda v: f"MaxConn:{v}",
            'retryInterval': lambda v: f"RetryInterval:{v}",
            'timeout': lambda v: f"Timeout:{v}",
            'enableSkipIncompatibleRow': lambda v: f"SkipIncompat:{v}",
            'dataIntegrationUnits': lambda v: f"DIU:{v}"
        }
        
        for key, formatter in value_keys.items():
            value = self.search_nested(type_props, key)
            if value is not None:
                values.append(formatter(value))
        
        # Special cases
        if activity_type in ['SetVariable', 'AppendVariable']:
            var_name = self.search_nested(type_props, 'variableName')
            var_value = self.search_nested(type_props, 'value')
            if var_name:
                values.append(f"{var_name}={self.extract_value(var_value)[:50]}")
        
        elif activity_type == 'WebActivity':
            url = self.search_nested(type_props, 'url')
            if url:
                values.append(f"URL:{self.extract_value(url)[:50]}")
        
        elif activity_type == 'ForEach':
            items = self.search_nested(type_props, 'items')
            if items:
                values.append(f"Items:{self.extract_value(items)[:50]}")
        
        elif activity_type == 'ExecuteDataFlow':
            # Extract DataFlow specific properties
            compute = type_props.get('compute', {})
            if isinstance(compute, dict):
                compute_type = compute.get('computeType', '')
                core_count = compute.get('coreCount', '')
                if compute_type:
                    values.append(f"ComputeType:{compute_type}")
                if core_count:
                    values.append(f"Cores:{core_count}")
        
        if values:
            if rec['Values Info']:
                rec['Values Info'] += ' | ' + self.sanitize_value(' | '.join(values))
            else:
                rec['Values Info'] = self.sanitize_value(' | '.join(values))
    
    def extract_parameters_dynamic(self, activity: dict, rec: dict):
        """Extract parameters from activity (FIXED REGEX)"""
        params = set()
        
        # Convert to string and find patterns
        try:
            activity_str = json.dumps(activity)
            
            # FIXED: Proper regex patterns (removed KATEX errors)
            patterns = [
                (r"@pipelineKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.parameters\.(\w+)", "P:{}"),
                (r"@variablesKATEX_INLINE_OPEN'(\w+)'KATEX_INLINE_CLOSE", "V:{}"),
                (r"@activityKATEX_INLINE_OPEN'([^']+)'KATEX_INLINE_CLOSE", "Act:{}"),
                (r"@datasetKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", "DS:{}"),
                (r"@linkedServiceKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", "LS:{}"),
                (r"@triggerKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", "Trg:{}"),
                (r"@dataflowKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", "DF:{}")
            ]
            
            for pattern, formatter in patterns:
                matches = re.findall(pattern, activity_str)
                for match in matches:
                    params.add(formatter.format(match))
            
            # Check for @item()
            if "@item()" in activity_str:
                params.add("Item")
            
        except:
            pass
        
        if params:
            rec['Parameters'] = self.sanitize_value(', '.join(sorted(list(params)[:20])))
    
    def extract_dependencies_dynamic(self, activity: dict, rec: dict):
        """Extract activity dependencies"""
        deps = []
        
        depends_on = activity.get('dependsOn', [])
        if isinstance(depends_on, list):
            for dep in depends_on:
                if isinstance(dep, dict):
                    dep_name = dep.get('activity', '')
                    conditions = dep.get('dependencyConditions', [])
                    
                    if conditions:
                        deps.append(f"{dep_name}({','.join(conditions)})")
                    else:
                        deps.append(dep_name)
        
        if deps:
            if rec['Values Info']:
                rec['Values Info'] += ' | ' + self.sanitize_value(f"Deps:{','.join(deps)}")
            else:
                rec['Values Info'] = self.sanitize_value(f"Deps:{','.join(deps)}")
    
    def parse_sql_dynamic(self, sql: str) -> Tuple[List[str], List[str]]:
        """Parse SQL dynamically for tables and columns (FIXED REGEX)"""
        tables = []
        columns = []
        
        if not sql:
            return tables, columns
        
        sql_upper = sql.upper()
        
        # FIXED: Proper regex patterns for table extraction
        table_patterns = [
            r'FROM\s+(```math?[\w\.]+```?)',
            r'JOIN\s+(```math?[\w\.]+```?)',
            r'INTO\s+(```math?[\w\.]+```?)',
            r'UPDATE\s+(```math?[\w\.]+```?)',
            r'DELETE\s+FROM\s+(```math?[\w\.]+```?)',
            r'MERGE\s+(```math?[\w\.]+```?)',
            r'TRUNCATE\s+TABLE\s+(```math?[\w\.]+```?)',
            r'INSERT\s+INTO\s+(```math?[\w\.]+```?)',
            r'EXEC\s+(```math?[\w\.]+```?)'
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, sql_upper)
            for match in matches:
                table = match.strip('[]').strip()
                if table and not table.startswith('@') and not table.startswith('('):
                    # Handle schema.table format
                    if '.' in table:
                        parts = table.split('.')
                        if len(parts) == 2:
                            tables.append(table)
                        else:
                            tables.append(parts[-1])
                    else:
                        tables.append(table)
        
        # FIXED: Proper regex for column extraction
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_upper, re.DOTALL)
        if select_match:
            select_part = select_match.group(1)
            
            if '*' in select_part:
                columns.append('*')
            else:
                # Clean and split
                select_part = re.sub(r'/\*.*?\*/', '', select_part)  # Remove comments
                select_part = re.sub(r'--.*?$', '', select_part, flags=re.MULTILINE)
                
                # Split by comma (handling nested functions)
                parts = []
                depth = 0
                current = []
                for char in select_part:
                    if char == '(':
                        depth += 1
                    elif char == ')':
                        depth -= 1
                    elif char == ',' and depth == 0:
                        parts.append(''.join(current))
                        current = []
                        continue
                    current.append(char)
                if current:
                    parts.append(''.join(current))
                
                for part in parts[:30]:
                    col = part.strip()
                    
                    # Remove brackets
                    col = re.sub(r'```math|```', '', col)
                    
                    # Handle AS alias
                    if ' AS ' in col:
                        col = col.split(' AS ')[-1].strip()
                    
                    # Handle table.column
                    if '.' in col:
                        col = col.split('.')[-1]
                    
                    # Remove functions but keep column name
                    func_match = re.match(r'\w+\s*KATEX_INLINE_OPEN([^)]+)KATEX_INLINE_CLOSE', col)
                    if func_match:
                        col = func_match.group(1)
                    
                    col = col.strip()
                    if col and len(col) < 50 and not col.startswith('@') and col not in ['DISTINCT', 'TOP', 'NULL']:
                        columns.append(col)
        
        # Remove duplicates while preserving order
        tables = list(dict.fromkeys(tables))[:10]
        columns = list(dict.fromkeys(columns))[:20]
        
        return tables, columns
    
    def parse_dataset(self, resource: dict):
        """Parse dataset"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            ds_type = props.get('type', 'Unknown')
            type_props = props.get('typeProperties', {})
            
            # Count type
            self.metrics['dataset_types'][ds_type] += 1
            
            # Track dataset
            self.dataset_references[name] = resource
            
            rec = {
                'Dataset': self.sanitize_value(name),
                'Type': self.sanitize_value(ds_type),
                'LinkedService': '',
                'Location': '',
                'Schema': '',
                'Parameters': '',
                'Folder': self.sanitize_value(self.get_nested(props, 'folder.name')),
                'Description': self.sanitize_value(props.get('description', ''))
            }
            
            # Linked service
            ls = props.get('linkedServiceName', {})
            if isinstance(ls, dict):
                rec['LinkedService'] = self.sanitize_value(self.extract_name(ls.get('referenceName', '')))
            
            # Location - dynamic extraction
            location_keys = ['tableName', 'table', 'fileName', 'folderPath', 'container', 
                           'collection', 'relativeUrl', 'key', 'path']
            
            location_parts = []
            for key in location_keys:
                value = self.search_nested(type_props, key)
                if value:
                    location_parts.append(f"{key}:{self.extract_value(value)}")
            
            if location_parts:
                rec['Location'] = self.sanitize_value(' | '.join(location_parts[:5]))
            
            # Schema info
            schema_def = props.get('schema') or props.get('structure')
            if isinstance(schema_def, list):
                columns = []
                for col in schema_def[:20]:
                    if isinstance(col, dict):
                        col_name = col.get('name', '')
                        col_type = col.get('type', '')
                        if col_name:
                            columns.append(f"{col_name}:{col_type}" if col_type else col_name)
                rec['Schema'] = self.sanitize_value(f"{len(schema_def)} cols: {', '.join(columns[:10])}")
            elif schema_def:
                rec['Schema'] = 'Dynamic'
            
            # Parameters
            params = props.get('parameters', {})
            if isinstance(params, dict):
                rec['Parameters'] = self.sanitize_value(', '.join(list(params.keys())[:10]))
            
            self.results['datasets'].append(rec)
            
        except Exception as e:
            self.log_error(resource, f"Dataset: {e}")
    
    def parse_linked_service(self, resource: dict):
        """Parse linked service"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            ls_type = props.get('type', 'Unknown')
            type_props = props.get('typeProperties', {})
            
            # Count type
            self.metrics['linked_service_types'][ls_type] += 1
            
            # Track linked service
            self.linkedservice_references[name] = resource
            
            rec = {
                'LinkedService': self.sanitize_value(name),
                'Type': self.sanitize_value(ls_type),
                'ConnectVia': '',
                'Authentication': '',
                'Connection': '',
                'Description': self.sanitize_value(props.get('description', '')),
                'Annotations': self.sanitize_value(', '.join(str(a) for a in props.get('annotations', [])))
            }
            
            # Integration runtime
            connect = props.get('connectVia', {})
            if isinstance(connect, dict):
                rec['ConnectVia'] = self.sanitize_value(self.extract_name(connect.get('referenceName', 'Default')))
            
            # Dynamic auth detection
            rec['Authentication'] = self.sanitize_value(self.detect_auth_dynamic(type_props))
            
            # Dynamic connection extraction
            rec['Connection'] = self.sanitize_value(self.extract_connection_dynamic(ls_type, type_props))
            
            self.results['linked_services'].append(rec)
            
        except Exception as e:
            self.log_error(resource, f"LinkedService: {e}")
    
    def parse_trigger(self, resource: dict):
        """Parse trigger with complete schedule extraction"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            trigger_type = props.get('type', 'Unknown')
            type_props = props.get('typeProperties', {})
            
            # Count type
            self.metrics['trigger_types'][trigger_type] += 1
            
            rec = {
                'Trigger': self.sanitize_value(name),
                'Type': self.sanitize_value(trigger_type),
                'State': self.sanitize_value(props.get('runtimeState', 'Unknown')),
                'Frequency': '',
                'Interval': '',
                'Schedule': '',
                'StartTime': '',
                'EndTime': '',
                'TimeZone': '',
                'Pipelines': '',
                'Description': self.sanitize_value(props.get('description', ''))
            }
            
            # Extract schedule based on type
            if trigger_type == 'ScheduleTrigger':
                recurrence = type_props.get('recurrence', {})
                if isinstance(recurrence, dict):
                    freq = recurrence.get('frequency', '')
                    interval = recurrence.get('interval', 1)
                    
                    rec['Frequency'] = self.sanitize_value(freq)
                    rec['Interval'] = str(interval)
                    
                    # Build human-readable schedule
                    schedule_parts = []
                    
                    if freq == 'Minute':
                        schedule_parts.append(f"Every {interval} minute{'s' if interval > 1 else ''}")
                    elif freq == 'Hour':
                        schedule_parts.append(f"Every {interval} hour{'s' if interval > 1 else ''}")
                    elif freq == 'Day':
                        schedule_parts.append(f"Daily" if interval == 1 else f"Every {interval} days")
                    elif freq == 'Week':
                        schedule_parts.append(f"Weekly" if interval == 1 else f"Every {interval} weeks")
                        weekdays = recurrence.get('weekDays', [])
                        if weekdays:
                            schedule_parts.append(f"on {', '.join(weekdays)}")
                    elif freq == 'Month':
                        schedule_parts.append(f"Monthly" if interval == 1 else f"Every {interval} months")
                        month_days = recurrence.get('monthDays', [])
                        if month_days:
                            schedule_parts.append(f"on day(s) {', '.join(map(str, month_days))}")
                    
                    # Time details
                    schedule = recurrence.get('schedule', {})
                    if isinstance(schedule, dict):
                        hours = schedule.get('hours', [])
                        minutes = schedule.get('minutes', [])
                        
                        if hours and minutes:
                            times = []
                            for h in hours[:5]:
                                for m in minutes[:5]:
                                    times.append(f"{h:02d}:{m:02d}")
                            if times:
                                schedule_parts.append(f"at {', '.join(times[:10])}")
                                if len(times) > 10:
                                    schedule_parts.append(f"(+{len(times)-10} more times)")
                        elif hours:
                            schedule_parts.append(f"at hour(s): {', '.join(map(str, hours[:10]))}")
                        elif minutes:
                            schedule_parts.append(f"at minute(s): {', '.join(map(str, minutes[:10]))}")
                    
                    rec['Schedule'] = self.sanitize_value(' '.join(schedule_parts))
                    
                    # Times
                    start = recurrence.get('startTime', '')
                    end = recurrence.get('endTime', '')
                    tz = recurrence.get('timeZone', 'UTC')
                    
                    if start:
                        rec['StartTime'] = self.sanitize_value(start[:19])
                    if end:
                        rec['EndTime'] = self.sanitize_value(end[:19])
                    rec['TimeZone'] = self.sanitize_value(tz)
            
            elif trigger_type == 'TumblingWindowTrigger':
                freq = type_props.get('frequency', '')
                interval = type_props.get('interval', 1)
                
                rec['Frequency'] = self.sanitize_value(freq)
                rec['Interval'] = str(interval)
                rec['Schedule'] = self.sanitize_value(f"Tumbling window: Every {interval} {freq.lower()}")
                
                start = type_props.get('startTime', '')
                end = type_props.get('endTime', '')
                
                if start:
                    rec['StartTime'] = self.sanitize_value(start[:19])
                if end:
                    rec['EndTime'] = self.sanitize_value(end[:19])
            
            elif trigger_type == 'BlobEventsTrigger':
                rec['Schedule'] = 'Blob events'
                folder = type_props.get('folderPath', '')
                events = type_props.get('events', [])
                
                if folder:
                    rec['Schedule'] = self.sanitize_value(f"Blob events in {folder}")
                if events:
                    rec['Schedule'] += self.sanitize_value(f" on {', '.join(events)}")
            
            # Get pipelines
            pipelines = props.get('pipelines', [])
            if isinstance(pipelines, list):
                pipeline_names = []
                for p in pipelines:
                    if isinstance(p, dict):
                        ref = p.get('pipelineReference', {})
                        if isinstance(ref, dict):
                            pname = self.extract_name(ref.get('referenceName', ''))
                            if pname:
                                pipeline_names.append(pname)
                
                rec['Pipelines'] = self.sanitize_value(', '.join(pipeline_names[:10]))
                
                # Store trigger details
                for pname in pipeline_names:
                    self.results['trigger_details'].append({
                        'Trigger': name,
                        'Pipeline': pname,
                        'TriggerType': trigger_type,
                        'Schedule': rec['Schedule']
                    })
            
            self.results['triggers'].append(rec)
            
        except Exception as e:
            self.log_error(resource, f"Trigger: {e}")
    
    def parse_integration_runtime(self, resource: dict):
        """Parse integration runtime"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            ir_type = props.get('type', 'Unknown')
            type_props = props.get('typeProperties', {})
            
            rec = {
                'IntegrationRuntime': self.sanitize_value(name),
                'Type': self.sanitize_value(ir_type),
                'Location': '',
                'NodeSize': '',
                'Description': self.sanitize_value(props.get('description', ''))
            }
            
            if ir_type == 'Managed':
                compute = type_props.get('computeProperties', {})
                if isinstance(compute, dict):
                    rec['Location'] = self.sanitize_value(compute.get('location', 'AutoResolve'))
                    rec['NodeSize'] = self.sanitize_value(compute.get('dataFlowProperties', {}).get('computeType', ''))
            elif ir_type == 'SelfHosted':
                rec['Location'] = 'On-Premises'
            
            self.results['integration_runtimes'].append(rec)
            
        except Exception as e:
            self.log_error(resource, f"IR: {e}")
    
    def parse_managed_virtual_network(self, resource: dict):
        """Parse managed virtual network"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            
            rec = {
                'ManagedVirtualNetwork': self.sanitize_value(name),
                'Type': 'ManagedVNet',
                'Description': self.sanitize_value(props.get('description', ''))
            }
            
            self.results['managed_virtual_networks'].append(rec)
            
        except Exception as e:
            self.log_error(resource, f"ManagedVNet: {e}")
    
    def parse_credential(self, resource: dict):
        """Parse credential"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            cred_type = props.get('type', 'Unknown')
            
            rec = {
                'Credential': self.sanitize_value(name),
                'Type': self.sanitize_value(cred_type),
                'Description': self.sanitize_value(props.get('description', ''))
            }
            
            self.results['credentials'].append(rec)
            
        except Exception as e:
            self.log_error(resource, f"Credential: {e}")
    
    def extract_relationships(self):
        """Extract relationships and dependencies including DataFlow relationships"""
        print("\n🔗 Extracting relationships...")
        
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
                            'Sink': sink,
                            'Transformation': activity.get('Role', 'Copy')
                        })
        
        # Extract data lineage for DataFlow activities
        for activity in self.results['activities']:
            if activity['Activity Type'] == 'ExecuteDataFlow':
                dataflow_name = activity.get('DataFlow', '')
                if dataflow_name:
                    # Find the corresponding dataflow lineage
                    for df_lineage in self.results['dataflow_lineage']:
                        if df_lineage['DataFlow'] == dataflow_name:
                            self.results['data_lineage'].append({
                                'Pipeline': activity['Pipeline'],
                                'Activity': activity['Activity'],
                                'Type': 'DataFlow',
                                'Source': f"{df_lineage['SourceName']} ({df_lineage['SourceLinkedService'] or df_lineage['SourceDataset']})",
                                'Sink': f"{df_lineage['SinkName']} ({df_lineage['SinkLinkedService'] or df_lineage['SinkDataset']})",
                                'Transformation': f"DataFlow:{dataflow_name}"
                            })
        
        print(f"✅ Relationships extracted: {len(self.results['data_lineage'])} lineage records")
    
    def export_to_excel(self):
        """Export results to Excel including all discovered resources"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        excel_file = output_dir / f'adf_analysis_{timestamp}.xlsx'
        
        print(f"\n📊 Exporting to: {excel_file}")
        
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                
                # Summary sheet
                summary = [
                    {'Metric': 'Analysis Date', 'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                    {'Metric': 'Source File', 'Value': str(self.json_path)},
                    {'Metric': '---', 'Value': '---'},
                    {'Metric': 'Total Activities', 'Value': len(self.results['activities'])},
                    {'Metric': 'Total Pipelines', 'Value': len(self.results['pipelines'])},
                    {'Metric': 'Total DataFlows', 'Value': len(self.results['dataflows'])},
                    {'Metric': 'Total Datasets', 'Value': len(self.results['datasets'])},
                    {'Metric': 'Total Linked Services', 'Value': len(self.results['linked_services'])},
                    {'Metric': 'Total Triggers', 'Value': len(self.results['triggers'])},
                    {'Metric': 'Total Integration Runtimes', 'Value': len(self.results['integration_runtimes'])},
                    {'Metric': '---', 'Value': '---'},
                    {'Metric': 'Unique Activity Types', 'Value': len(self.metrics['activity_types'])},
                    {'Metric': 'Unique DataFlow Types', 'Value': len(self.metrics['dataflow_types'])},
                    {'Metric': 'Unique Dataset Types', 'Value': len(self.metrics['dataset_types'])},
                    {'Metric': 'Unique Trigger Types', 'Value': len(self.metrics['trigger_types'])},
                    {'Metric': 'Unique Transformation Types', 'Value': len(self.metrics['transformation_types'])},
                    {'Metric': '---', 'Value': '---'},
                    {'Metric': 'Data Lineage Records', 'Value': len(self.results['data_lineage'])},
                    {'Metric': 'Parse Errors', 'Value': len(self.results['errors'])}
                ]
                
                if self.enable_discovery:
                    summary.extend([
                        {'Metric': '---', 'Value': '---'},
                        {'Metric': 'Discovered Resource Types', 'Value': len(self.discovered_patterns.get('resource_types', {}))},
                        {'Metric': 'Discovered Expression Functions', 'Value': len(self.discovered_patterns.get('expression_functions', {}))}
                    ])
                
                pd.DataFrame(summary).to_excel(writer, sheet_name='Summary', index=False)
                print(f"  ✓ Summary")
                
                # Export all main sheets
                main_sheets = [
                    ('Activities', self.results['activities']),
                    ('Pipelines', self.results['pipelines']),
                    ('DataFlows', self.results['dataflows']),
                    ('DataFlowLineage', self.results['dataflow_lineage']),
                    ('DataFlowTransformations', self.results['dataflow_transformations']),
                    ('Datasets', self.results['datasets']),
                    ('LinkedServices', self.results['linked_services']),
                    ('Triggers', self.results['triggers']),
                    ('TriggerDetails', self.results['trigger_details']),
                    ('IntegrationRuntimes', self.results['integration_runtimes']),
                    ('DataLineage', self.results['data_lineage'])
                ]
                
                for sheet_name, data in main_sheets:
                    if data:
                        df = pd.DataFrame(data)
                        # Truncate sheet name if too long
                        safe_sheet_name = sheet_name[:31]
                        df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                        print(f"  ✓ {sheet_name}: {len(df)} rows")
                
                # Export optional sheets
                optional_sheets = [
                    ('ManagedVNets', self.results['managed_virtual_networks']),
                    ('Credentials', self.results['credentials'])
                ]
                
                for sheet_name, data in optional_sheets:
                    if data:
                        df = pd.DataFrame(data)
                        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                        print(f"  ✓ {sheet_name}: {len(df)} rows")
                
                # Statistics - Enhanced
                if any(self.metrics.values()):
                    stats = []
                    
                    for atype, count in self.metrics['activity_types'].most_common():
                        stats.append({'Category': 'Activity', 'Type': atype, 'Count': count})
                    
                    for dtype, count in self.metrics['dataflow_types'].most_common():
                        stats.append({'Category': 'DataFlow', 'Type': dtype, 'Count': count})
                    
                    for ttype, count in self.metrics['transformation_types'].most_common():
                        stats.append({'Category': 'Transformation', 'Type': ttype, 'Count': count})
                    
                    for dtype, count in self.metrics['dataset_types'].most_common():
                        stats.append({'Category': 'Dataset', 'Type': dtype, 'Count': count})
                    
                    for lstype, count in self.metrics['linked_service_types'].most_common():
                        stats.append({'Category': 'LinkedService', 'Type': lstype, 'Count': count})
                    
                    for ttype, count in self.metrics['trigger_types'].most_common():
                        stats.append({'Category': 'Trigger', 'Type': ttype, 'Count': count})
                    
                    if stats:
                        pd.DataFrame(stats).to_excel(writer, sheet_name='Statistics', index=False)
                        print(f"  ✓ Statistics")
                
                # Discovery patterns (if enabled)
                if self.enable_discovery and self.discovered_patterns:
                    disc_stats = []
                    
                    for res_type, count in self.discovered_patterns.get('resource_types', {}).most_common():
                        disc_stats.append({'Category': 'Resource Type', 'Name': res_type, 'Count': count})
                    
                    for func, count in self.discovered_patterns.get('expression_functions', {}).most_common(50):
                        disc_stats.append({'Category': 'Expression Function', 'Name': func, 'Count': count})
                    
                    if disc_stats:
                        pd.DataFrame(disc_stats).to_excel(writer, sheet_name='Discovered Patterns', index=False)
                        print(f"  ✓ Discovered Patterns")
                
                # Errors
                if self.results['errors']:
                    pd.DataFrame(self.results['errors']).to_excel(writer, sheet_name='Errors', index=False)
                    print(f"  ⚠ Errors: {len(self.results['errors'])}")
            
            print(f"\n✅ Export complete: {excel_file}")
            
        except Exception as e:
            print(f"\n❌ Export failed: {e}")
            import traceback
            traceback.print_exc()
    
    def print_summary(self):
        """Print comprehensive summary"""
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)
        
        print(f"\n📈 Results:")
        print(f"  • Activities: {len(self.results['activities'])}")
        print(f"  • Pipelines: {len(self.results['pipelines'])}")
        print(f"  • DataFlows: {len(self.results['dataflows'])}")
        print(f"  • Datasets: {len(self.results['datasets'])}")
        print(f"  • Linked Services: {len(self.results['linked_services'])}")
        print(f"  • Triggers: {len(self.results['triggers'])}")
        print(f"  • Integration Runtimes: {len(self.results['integration_runtimes'])}")
        
        if self.metrics['activity_types']:
            print(f"\n⚡ Top Activities:")
            for atype, count in self.metrics['activity_types'].most_common(5):
                print(f"  • {atype:30} : {count:4d}")
        
        if self.metrics['dataflow_types']:
            print(f"\n🌊 DataFlow Types:")
            for dtype, count in self.metrics['dataflow_types'].items():
                print(f"  • {dtype:30} : {count:4d}")
        
        if self.metrics['transformation_types']:
            print(f"\n🔄 Top Transformations:")
            for ttype, count in self.metrics['transformation_types'].most_common(5):
                print(f"  • {ttype:30} : {count:4d}")
        
        if self.metrics['trigger_types']:
            print(f"\n⏰ Trigger Types:")
            for ttype, count in self.metrics['trigger_types'].items():
                print(f"  • {ttype:30} : {count:4d}")
        
        # DataFlow usage
        dataflow_activities = [a for a in self.results['activities'] if a.get('DataFlow')]
        if dataflow_activities:
            print(f"\n🔄 DataFlow Usage:")
            print(f"  • ExecuteDataFlow activities: {len(dataflow_activities)}")
            unique_dataflows = set(a['DataFlow'] for a in dataflow_activities if a.get('DataFlow'))
            print(f"  • Unique DataFlows referenced: {len(unique_dataflows)}")
        
        # Data Lineage
        if self.results['data_lineage']:
            print(f"\n🔗 Data Lineage:")
            print(f"  • Total lineage records: {len(self.results['data_lineage'])}")
            copy_lineage = [l for l in self.results['data_lineage'] if l.get('Type') == 'Copy']
            df_lineage = [l for l in self.results['data_lineage'] if l.get('Type') == 'DataFlow']
            print(f"  • Copy activity lineage: {len(copy_lineage)}")
            print(f"  • DataFlow lineage: {len(df_lineage)}")
        
        if self.enable_discovery and self.discovered_patterns:
            print(f"\n🔍 Pattern Discovery:")
            print(f"  • Resource types discovered: {len(self.discovered_patterns.get('resource_types', {}))}")
            print(f"  • Expression functions found: {len(self.discovered_patterns.get('expression_functions', {}))}")
        
        if self.results['errors']:
            print(f"\n⚠️  Parse errors: {len(self.results['errors'])}")
    
    # Helper methods
    
    def extract_name(self, name: str) -> str:
        """Extract clean name"""
        if not name:
            return ''
        
        name = str(name)
        
        if "concat(parameters('factoryName')" in name:
            match = re.search(r"'/([^']+)'", name)
            if match:
                return match.group(1)
        
        name = name.strip("[]'\"")
        
        if '/' in name:
            name = name.split('/')[-1]
        
        return name
    
    def extract_value(self, value: Any) -> str:
        """Extract value from any format"""
        if value is None:
            return ''
        
        if isinstance(value, str):
            return value
        
        if isinstance(value, (int, float, bool)):
            return str(value)
        
        if isinstance(value, dict):
            if 'value' in value:
                return self.extract_value(value['value'])
            
            if value.get('type') == 'SecureString':
                return '[SECURE]'
            
            if value.get('type') == 'AzureKeyVaultSecret':
                return f"[KV:{value.get('secretName', '')}]"
            
            if 'expression' in value:
                return self.extract_value(value['expression'])
        
        if isinstance(value, list) and value:
            return str(value[0])
        
        return str(value)[:100]
    
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
    
    def search_nested(self, obj: Any, key: str) -> Any:
        """Search for key in nested structure"""
        if not isinstance(obj, (dict, list)):
            return None
        
        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            
            for v in obj.values():
                result = self.search_nested(v, key)
                if result is not None:
                    return result
        
        elif isinstance(obj, list):
            for item in obj:
                result = self.search_nested(item, key)
                if result is not None:
                    return result
        
        return None
    
    def format_dict(self, d: dict) -> str:
        """Format dictionary for display"""
        if not isinstance(d, dict):
            return ''
        
        items = []
        for k, v in list(d.items())[:10]:
            if isinstance(v, dict):
                type_val = v.get('type', 'String')
                items.append(f"{k}({type_val})")
            else:
                items.append(str(k))
        
        result = ', '.join(items)
        if len(d) > 10:
            result += f" (+{len(d)-10} more)"
        
        return result
    
    def detect_auth_dynamic(self, type_props: dict) -> str:
        """Detect authentication dynamically"""
        auth_checks = [
            ('authenticationType', lambda v: v),
            ('servicePrincipalId', lambda v: 'ServicePrincipal'),
            ('accountKey', lambda v: 'KeyVault' if isinstance(v, dict) and v.get('type') == 'AzureKeyVaultSecret' else 'AccountKey'),
            ('connectionString', lambda v: 'KeyVault' if isinstance(v, dict) and v.get('type') == 'AzureKeyVaultSecret' else 'ConnectionString'),
            ('sasUri', lambda v: 'SAS'),
            ('sasToken', lambda v: 'SAS'),
            ('credential', lambda v: 'ManagedIdentity'),
            ('useManagedIdentity', lambda v: 'ManagedIdentity' if v else None)
        ]
        
        for key, detector in auth_checks:
            value = self.search_nested(type_props, key)
            if value is not None:
                auth = detector(value)
                if auth:
                    return auth
        
        return 'Default'
    
    def extract_connection_dynamic(self, ls_type: str, type_props: dict) -> str:
        """Extract connection info dynamically"""
        conn_keys = [
            'baseUrl', 'url', 'endpoint', 'accountEndpoint', 'serviceEndpoint',
            'domain', 'server', 'host', 'connectionString', 'accountName'
        ]
        
        for key in conn_keys:
            value = self.search_nested(type_props, key)
            if value:
                conn_val = self.extract_value(value)
                
                # Extract server from connection string
                if 'connectionString' in key and 'Server=' in conn_val:
                    match = re.search(r'(?:Server|Data Source)=([^;]+)', conn_val)
                    if match:
                        return match.group(1).strip()[:50]
                
                return conn_val[:50]
        
        return ls_type
    
    def log_error(self, resource: Any, error: str):
        """Log error"""
        self.results['errors'].append({
            'Resource': self.sanitize_value(str(resource.get('name', 'Unknown'))[:100] if isinstance(resource, dict) else 'Unknown'),
            'Type': self.sanitize_value(str(resource.get('type', 'Unknown'))[:100] if isinstance(resource, dict) else 'Unknown'),
            'Error': self.sanitize_value(error[:500])
        })


def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║     Ultimate ADF Parser v7.0 - With Auto-Discovery & DataFlow Support       ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Usage: python adf_parser_v7.py <template.json> [--no-discovery]

Arguments:
  template.json    : Path to your ARM template JSON file
  --no-discovery   : Disable auto-discovery (faster parsing)

Features:
  ✅ Complete DataFlow support with lineage tracking
  ✅ Auto-discovery of new resource types
  ✅ Comprehensive activity parsing
  ✅ SQL parsing and table/column extraction
  ✅ Parameter and dependency tracking
  ✅ Multi-phase parsing for optimal performance
  ✅ Detailed Excel reports with multiple sheets

Example:
  python adf_parser_v7.py factory_arm_template.json
  python adf_parser_v7.py factory_arm_template.json --no-discovery
        """)
        sys.exit(1)
    
    json_path = sys.argv[1]
    enable_discovery = '--no-discovery' not in sys.argv
    
    parser = UltimateADFParser(json_path, enable_discovery=enable_discovery)
    success = parser.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()