"""
ULTIMATE Azure Data Factory ARM Template Parser v8.0
✅ Meeting Requirements Implemented:
- Integration Runtime columns added
- Table/File names in source/sink for Copy & DataFlow
- Enhanced SQL extraction with max text capture
- Stored Procedure activity support
- Orphaned resource detection (Pipelines, Triggers, LinkedServices, Datasets)
- Impact Analysis
- Fixed regex patterns
- Reordered sheets (Pipeline first)
- Automated Streamlit output naming
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
            
            self._discover_property_paths(resource, f"{category}", category)
            self._discover_expressions(resource)
        
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
            # FIXED: Proper regex pattern
            functions = re.findall(r'@(\w+)\s*\(', obj)
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
                
                result_key = f'dynamic_{resource_type.lower()}'
                if result_key not in self.results:
                    self.results[result_key] = []
                self.results[result_key].append(rec)
                
            except Exception as e:
                self.log_error(resource, f"Dynamic_{resource_type}: {e}")
        
        setattr(self, f'parse_dynamic_{resource_type.lower()}', dynamic_parser)


class UltimateADFParser(PatternDiscoveryMixin):
    """Complete ADF ARM Template Parser with Impact Analysis"""
    
    SUPPORTED_SCHEMAS = [
        "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
        "https://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
        "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#"
    ]
    
    def __init__(self, json_path: str, enable_discovery: bool = True):
        self.json_path = json_path
        self.data = None
        self.enable_discovery = enable_discovery
        
        # ✅ NEW: Enhanced results storage with reordered sheets
        self.results = {
            # Core sheets (reordered - Pipeline first per feedback)
            'pipelines': [],
            'activities': [],
            'dataflows': [],
            'dataflow_lineage': [],
            'dataflow_transformations': [],
            'datasets': [],
            'linked_services': [],
            'triggers': [],
            'trigger_details': [],
            'integration_runtimes': [],
            
            # ✅ NEW: Impact Analysis sheets
            'orphaned_pipelines': [],
            'orphaned_datasets': [],
            'orphaned_linked_services': [],
            'orphaned_triggers': [],
            'impact_analysis': [],
            'activity_count': [],  # ✅ NEW: Activity usage summary
            
            # Additional
            'parameters': [],
            'dependencies': [],
            'data_lineage': [],
            'managed_virtual_networks': [],
            'managed_private_endpoints': [],
            'credentials': [],
            'statistics': {},
            'errors': []
        }
        
        # Metrics
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
        
        # ✅ NEW: Enhanced reference tracking for impact analysis
        self.dataflow_references = {}
        self.dataset_references = {}
        self.linkedservice_references = {}
        self.pipeline_references = {}
        self.trigger_references = {}
        
        # ✅ NEW: Usage tracking
        self.dataset_usage = defaultdict(set)  # dataset -> set of pipelines using it
        self.linkedservice_usage = defaultdict(set)
        self.dataflow_usage = defaultdict(set)
        self.pipeline_usage = defaultdict(set)  # pipeline -> set of pipelines calling it
        
        self.discovered_patterns = {}
        
        print(f"🚀 Ultimate ADF Parser v8.0 - Impact Analyzer Edition")
        print(f"📁 Input: {json_path}")
    
    def sanitize_value(self, value: Any, max_length: int = 32767) -> str:
        """Sanitize any value for Excel export"""
        if value is None:
            return ''
        
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
        
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text[:max_length]
    
    def run(self) -> bool:
        """Main execution"""
        print("\n" + "="*80)
        print("AZURE DATA FACTORY ARM TEMPLATE ANALYSIS")
        print("="*80)
        
        try:
            if not self.load_template():
                return False
            
            if self.enable_discovery:
                self.discover_patterns()
            
            self.parse_all_resources()
            self.extract_relationships()
            
            # ✅ NEW: Impact Analysis
            self.analyze_orphaned_resources()
            self.analyze_impact()
            self.calculate_activity_counts()
            
            self.export_to_excel()
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
        
        print("\n⚙️ Processing in phases...")
        
        # Phase 1: Infrastructure
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
        
        # Phase 2: Linked Services
        print("  Phase 2: Linked Services...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'linkedServices' in res_type:
                        self.parse_linked_service(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 3: Datasets
        print("  Phase 3: Datasets...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'datasets' in res_type:
                        self.parse_dataset(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 4: DataFlows
        print("  Phase 4: DataFlows...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'dataflows' in res_type:
                        self.parse_dataflow(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 5: Pipelines
        print("  Phase 5: Pipelines...")
        iterator = [r for r in resources if isinstance(r, dict) and 'pipelines' in r.get('type', '')]
        if HAS_TQDM and len(iterator) > 10:
            iterator = tqdm(iterator, desc="Parsing Pipelines")
        
        for resource in iterator:
            try:
                self.parse_pipeline(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        # Phase 6: Triggers
        print("  Phase 6: Triggers...")
        for resource in resources:
            try:
                if isinstance(resource, dict):
                    res_type = resource.get('type', '')
                    if 'triggers' in res_type:
                        self.parse_trigger(resource)
            except Exception as e:
                self.log_error(resource, str(e))
        
        print(f"\n✅ Parsing complete")
    
    def parse_dataflow(self, resource: dict):
        """✅ ENHANCED: Parse data flow with IR and sink table names"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            flow_type = props.get('type', 'MappingDataFlow')
            type_props = props.get('typeProperties', {})
            
            self.metrics['dataflow_types'][flow_type] += 1
            self.dataflow_references[name] = resource
            
            # Parse sources
            sources = type_props.get('sources', [])
            source_info = []
            for source in sources if isinstance(sources, list) else []:
                if isinstance(source, dict):
                    source_name = source.get('name', '')
                    
                    linked_service = source.get('linkedService', {})
                    ls_name = self.extract_name(linked_service.get('referenceName', '')) if isinstance(linked_service, dict) else ''
                    
                    dataset = source.get('dataset', {})
                    ds_name = self.extract_name(dataset.get('referenceName', '')) if isinstance(dataset, dict) else ''
                    
                    # ✅ NEW: Extract source table/file name
                    source_table = ''
                    if ds_name and ds_name in self.dataset_references:
                        ds_resource = self.dataset_references[ds_name]
                        source_table = self.extract_dataset_location(ds_resource)
                    
                    source_info.append({
                        'name': source_name,
                        'linkedService': ls_name,
                        'dataset': ds_name,
                        'table': source_table
                    })
                    
                    self.metrics['source_types'][source_name] += 1
            
            # Parse sinks
            sinks = type_props.get('sinks', [])
            sink_info = []
            for sink in sinks if isinstance(sinks, list) else []:
                if isinstance(sink, dict):
                    sink_name = sink.get('name', '')
                    
                    linked_service = sink.get('linkedService', {})
                    ls_name = self.extract_name(linked_service.get('referenceName', '')) if isinstance(linked_service, dict) else ''
                    
                    dataset = sink.get('dataset', {})
                    ds_name = self.extract_name(dataset.get('referenceName', '')) if isinstance(dataset, dict) else ''
                    
                    # ✅ NEW: Extract sink table/file name
                    sink_table = ''
                    if ds_name and ds_name in self.dataset_references:
                        ds_resource = self.dataset_references[ds_name]
                        sink_table = self.extract_dataset_location(ds_resource)
                    
                    sink_info.append({
                        'name': sink_name,
                        'linkedService': ls_name,
                        'dataset': ds_name,
                        'table': sink_table
                    })
                    
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
            
            # Parse script for transformation types
            script_lines = type_props.get('scriptLines', [])
            script_text = '\n'.join(str(line) for line in script_lines[:500]) if isinstance(script_lines, list) else ''
            
            transformation_types = []
            if script_text:
                # FIXED: Proper regex patterns
                trans_patterns = {
                    r'\bsource\s*\(': 'Source',
                    r'\bsink\s*\(': 'Sink',
                    r'\bselect\s*\(': 'Select',
                    r'\bderive\s*\(': 'DerivedColumn',
                    r'\baggregate\s*\(': 'Aggregate',
                    r'\bjoin\s*\(': 'Join',
                    r'\bfilter\s*\(': 'Filter',
                    r'\bsort\s*\(': 'Sort',
                    r'\bsplit\s*\(': 'ConditionalSplit',
                    r'\bunion\s*\(': 'Union',
                    r'\bpivot\s*\(': 'Pivot',
                    r'\bunpivot\s*\(': 'Unpivot',
                    r'\bwindow\s*\(': 'Window',
                    r'\brank\s*\(': 'Rank',
                    r'\blookup\s*\(': 'Lookup',
                    r'\bexists\s*\(': 'Exists',
                    r'\balter\s*\(': 'AlterRow',
                    r'\bflatten\s*\(': 'Flatten',
                    r'\bparse\s*\(': 'Parse',
                    r'\bsurrogateKey\s*\(': 'SurrogateKey',
                    r'\bassert\s*\(': 'Assert'
                }
                
                for pattern, trans_type in trans_patterns.items():
                    if re.search(pattern, script_text, re.IGNORECASE):
                        transformation_types.append(trans_type)
                        self.metrics['transformation_types'][trans_type] += 1
            
            # ✅ NEW: Get Integration Runtime
            ir_name = ''
            compute = type_props.get('compute', {})
            if isinstance(compute, dict):
                compute_ir = compute.get('integrationRuntime', {})
                if isinstance(compute_ir, dict):
                    ir_name = self.extract_name(compute_ir.get('referenceName', ''))
            
            # Create dataflow record
            dataflow_rec = {
                'DataFlow': self.sanitize_value(name),
                'Type': self.sanitize_value(flow_type),
                'IntegrationRuntime': self.sanitize_value(ir_name),  # ✅ NEW
                'Sources': len(sources) if isinstance(sources, list) else 0,
                'Sinks': len(sinks) if isinstance(sinks, list) else 0,
                'Transformations': len(transformations) if isinstance(transformations, list) else 0,
                'ScriptLines': len(script_lines) if isinstance(script_lines, list) else 0,
                'SourceNames': self.sanitize_value(', '.join([s['name'] for s in source_info])),
                'SourceTables': self.sanitize_value(', '.join([s['table'] for s in source_info if s['table']])),  # ✅ NEW
                'SourceLinkedServices': self.sanitize_value(', '.join([s['linkedService'] for s in source_info if s['linkedService']])),
                'SourceDatasets': self.sanitize_value(', '.join([s['dataset'] for s in source_info if s['dataset']])),
                'SinkNames': self.sanitize_value(', '.join([s['name'] for s in sink_info])),
                'SinkTables': self.sanitize_value(', '.join([s['table'] for s in sink_info if s['table']])),  # ✅ NEW
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
                        'SourceTable': source['table'],  # ✅ NEW
                        'SourceLinkedService': source['linkedService'],
                        'SourceDataset': source['dataset'],
                        'SinkName': sink['name'],
                        'SinkTable': sink['table'],  # ✅ NEW
                        'SinkLinkedService': sink['linkedService'],
                        'SinkDataset': sink['dataset'],
                        'TransformationCount': len(transformations),
                        'TransformationTypes': ', '.join(set(transformation_types))
                    })
            
        except Exception as e:
            self.log_error(resource, f"DataFlow: {e}")
    
    def extract_dataset_location(self, ds_resource: dict) -> str:
        """✅ NEW: Extract table/file name from dataset"""
        try:
            props = ds_resource.get('properties', {})
            type_props = props.get('typeProperties', {})
            
            # Try common location keys
            location_keys = [
                'tableName', 'table', 'schema.table',
                'fileName', 'folderPath', 'container',
                'collection', 'relativeUrl', 'key', 'path'
            ]
            
            for key in location_keys:
                value = self.search_nested(type_props, key)
                if value:
                    extracted = self.extract_value(value)
                    if extracted and not extracted.startswith('@'):
                        return extracted[:100]
            
            return ''
        except:
            return ''
    
    def parse_pipeline(self, resource: dict):
        """Parse pipeline with enhanced tracking"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            activities = props.get('activities', [])
            
            self.pipeline_references[name] = resource
            
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
        """✅ ENHANCED: Parse activity with IR, SP name, better SQL extraction"""
        if not isinstance(activity, dict):
            return
        
        activity_type = activity.get('type', 'Unknown')
        activity_name = activity.get('name', '')
        type_props = activity.get('typeProperties', {})
        
        self.metrics['activity_types'][activity_type] += 1
        
        role = self.get_dynamic_role(activity)
        
        # ✅ NEW: Extract Integration Runtime
        ir_name = ''
        linked_service_ir = type_props.get('linkedServiceName', {})
        if isinstance(linked_service_ir, dict):
            ls_ref = self.extract_name(linked_service_ir.get('referenceName', ''))
            if ls_ref and ls_ref in self.linkedservice_references:
                ls_resource = self.linkedservice_references[ls_ref]
                ls_props = ls_resource.get('properties', {})
                connect_via = ls_props.get('connectVia', {})
                if isinstance(connect_via, dict):
                    ir_name = self.extract_name(connect_via.get('referenceName', ''))
        
        # Also check activity-level IR
        activity_ir = type_props.get('integrationRuntime', {})
        if isinstance(activity_ir, dict) and not ir_name:
            ir_name = self.extract_name(activity_ir.get('referenceName', ''))
        
        rec = {
            'Pipeline': self.sanitize_value(pipeline),
            'Sequence': seq,
            'Activity': self.sanitize_value(activity_name),
            'Activity Type': self.sanitize_value(activity_type),
            'Role': self.sanitize_value(role),
            'IntegrationRuntime': self.sanitize_value(ir_name),  # ✅ NEW
            'Dataset': '',
            'DataFlow': '',
            'LinkedPipeline': '',
            'SourceTable': '',  # ✅ NEW
            'SinkTable': '',  # ✅ NEW
            'SQL': '',
            'Tables': '',
            'StoredProcedure': '',  # ✅ NEW
            'Columns': '',
            'Dataset File': '',
            'Parameters': '',
            'Triggers': '',
            'Values Info': '',
            'Note': self.sanitize_value(activity.get('description', ''))
        }
        
        # ExecuteDataFlow
        if activity_type == 'ExecuteDataFlow':
            dataflow = type_props.get('dataflow', {})
            if isinstance(dataflow, dict):
                dataflow_name = self.extract_name(dataflow.get('referenceName', ''))
                rec['DataFlow'] = self.sanitize_value(dataflow_name)
                rec['Role'] = f"DataFlow: {dataflow_name[:30]}"
                
                # ✅ Track usage
                self.dataflow_usage[dataflow_name].add(pipeline)
                
                compute = type_props.get('compute', {})
                if isinstance(compute, dict):
                    compute_type = compute.get('computeType', '')
                    core_count = compute.get('coreCount', '')
                    if compute_type or core_count:
                        rec['Values Info'] = self.sanitize_value(f"Compute: {compute_type} ({core_count} cores)")
                
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
        
        # ExecutePipeline
        elif activity_type == 'ExecutePipeline':
            pipeline_ref = type_props.get('pipeline', {})
            if isinstance(pipeline_ref, dict):
                linked_pipeline = self.extract_name(pipeline_ref.get('referenceName', ''))
                rec['LinkedPipeline'] = self.sanitize_value(linked_pipeline)
                rec['Role'] = f"Execute: {linked_pipeline[:30]}"
                
                # ✅ Track usage
                self.pipeline_usage[linked_pipeline].add(pipeline)
                
                wait = type_props.get('waitOnCompletion', True)
                rec['Values Info'] = self.sanitize_value(f"WaitOnCompletion: {wait}")
        
        # ✅ NEW: Stored Procedure Activity
        elif activity_type == 'SqlServerStoredProcedure':
            sp_name = self.search_nested(type_props, 'storedProcedureName')
            if sp_name:
                sp_text = self.extract_value(sp_name)
                rec['StoredProcedure'] = self.sanitize_value(sp_text)
                rec['Role'] = f"SP: {sp_text[:30]}"
                
                # SP Parameters
                sp_params = self.search_nested(type_props, 'storedProcedureParameters')
                if sp_params and isinstance(sp_params, dict):
                    params_str = ', '.join([f"@{k}" for k in list(sp_params.keys())[:10]])
                    rec['SQL'] = self.sanitize_value(f"EXEC {sp_text} {params_str}", 1000)  # ✅ Increased limit
        
        # ✅ ENHANCED: Extract datasets and table names for Copy activities
        if activity_type == 'Copy':
            self.extract_copy_details(activity, type_props, rec, pipeline)
        else:
            self.extract_datasets_dynamic(activity, rec)
        
        # ✅ ENHANCED: Extract SQL with maximum text capture
        self.extract_sql_enhanced(activity, type_props, rec)
        
        self.extract_files_dynamic(type_props, rec)
        self.extract_values_dynamic(activity_type, type_props, rec)
        self.extract_parameters_dynamic(activity, rec)
        self.extract_dependencies_dynamic(activity, rec)
        
        self.results['activities'].append(rec)
    
    def extract_copy_details(self, activity: dict, type_props: dict, rec: dict, pipeline: str):
        """✅ NEW: Extract source/sink details for Copy activities"""
        try:
            # Source
            source = type_props.get('source', {})
            source_dataset = None
            
            inputs = activity.get('inputs', [])
            if isinstance(inputs, list) and inputs:
                input_ref = inputs[0]
                if isinstance(input_ref, dict):
                    source_dataset = self.extract_name(input_ref.get('referenceName', ''))
                    rec['Dataset'] = f"IN:{source_dataset}"
                    
                    # ✅ Track usage
                    self.dataset_usage[source_dataset].add(pipeline)
                    
                    # Extract source table
                    if source_dataset in self.dataset_references:
                        ds_resource = self.dataset_references[source_dataset]
                        source_table = self.extract_dataset_location(ds_resource)
                        rec['SourceTable'] = self.sanitize_value(source_table)
            
            # Sink
            sink = type_props.get('sink', {})
            sink_dataset = None
            
            outputs = activity.get('outputs', [])
            if isinstance(outputs, list) and outputs:
                output_ref = outputs[0]
                if isinstance(output_ref, dict):
                    sink_dataset = self.extract_name(output_ref.get('referenceName', ''))
                    if rec['Dataset']:
                        rec['Dataset'] += f" | OUT:{sink_dataset}"
                    else:
                        rec['Dataset'] = f"OUT:{sink_dataset}"
                    
                    # ✅ Track usage
                    self.dataset_usage[sink_dataset].add(pipeline)
                    
                    # Extract sink table
                    if sink_dataset in self.dataset_references:
                        ds_resource = self.dataset_references[sink_dataset]
                        sink_table = self.extract_dataset_location(ds_resource)
                        rec['SinkTable'] = self.sanitize_value(sink_table)
        
        except Exception as e:
            pass
    
    def extract_sql_enhanced(self, activity: dict, type_props: dict, rec: dict):
        """✅ ENHANCED: Extract SQL with maximum text and better table extraction"""
        # Skip if already filled by SP
        if rec.get('SQL'):
            return
        
        # FIXED: Proper SQL key patterns
        sql_keys = [
            'sqlReaderQuery', 'query', 'text', 'sqlQuery', 'script',
            'preCopyScript', 'postCopyScript', 'sqlWriterQuery', 'sqlWriterStoredProcedureName'
        ]
        
        sql_text = ''
        max_sql_length = 5000  # ✅ Increased from 500 to capture maximum text
        
        # Search in type properties
        if isinstance(type_props, dict):
            for key in sql_keys:
                value = self.search_nested(type_props, key)
                if value:
                    sql_text = self.extract_value(value)
                    if sql_text:
                        break
        
        if sql_text:
            rec['SQL'] = self.sanitize_value(sql_text, max_sql_length)
            
            # ✅ ENHANCED: Parse SQL for tables and columns
            tables, columns = self.parse_sql_enhanced(sql_text)
            rec['Tables'] = self.sanitize_value(', '.join(tables))
            rec['Columns'] = self.sanitize_value(', '.join(columns[:30]))
    
    def parse_sql_enhanced(self, sql: str) -> Tuple[List[str], List[str]]:
        """✅ ENHANCED: Better SQL parsing for tables and columns"""
        tables = []
        columns = []
        
        if not sql:
            return tables, columns
        
        sql_upper = sql.upper()
        
        # FIXED: Proper regex patterns for table extraction
        table_patterns = [
            r'FROM\s+(?:\[)?(\w+(?:\.\w+)?(?:\.\w+)?)(?:\])?',
            r'JOIN\s+(?:\[)?(\w+(?:\.\w+)?(?:\.\w+)?)(?:\])?',
            r'INTO\s+(?:\[)?(\w+(?:\.\w+)?(?:\.\w+)?)(?:\])?',
            r'UPDATE\s+(?:\[)?(\w+(?:\.\w+)?(?:\.\w+)?)(?:\])?',
            r'DELETE\s+FROM\s+(?:\[)?(\w+(?:\.\w+)?(?:\.\w+)?)(?:\])?',
            r'MERGE\s+(?:\[)?(\w+(?:\.\w+)?(?:\.\w+)?)(?:\])?',
            r'TRUNCATE\s+TABLE\s+(?:\[)?(\w+(?:\.\w+)?(?:\.\w+)?)(?:\])?',
            r'INSERT\s+INTO\s+(?:\[)?(\w+(?:\.\w+)?(?:\.\w+)?)(?:\])?',
            r'EXEC(?:UTE)?\s+(?:\[)?(\w+(?:\.\w+)?)(?:\])?'
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, sql_upper)
            for match in matches:
                table = match.strip()
                if table and not table.startswith('@') and not table.startswith('('):
                    tables.append(table)
        
        # FIXED: Enhanced column extraction
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_upper, re.DOTALL)
        if select_match:
            select_part = select_match.group(1)
            
            if '*' in select_part:
                columns.append('*')
            else:
                # Remove comments
                select_part = re.sub(r'/\*.*?\*/', '', select_part, flags=re.DOTALL)
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
                
                for part in parts[:50]:  # ✅ Increased from 30
                    col = part.strip()
                    
                    # Remove brackets
                    col = re.sub(r'[\[\]]', '', col)
                    
                    # Handle AS alias
                    if ' AS ' in col:
                        col = col.split(' AS ')[-1].strip()
                    
                    # Handle table.column
                    if '.' in col:
                        col = col.split('.')[-1]
                    
                    # Remove functions but keep column name
                    func_match = re.match(r'\w+\s*\(([^)]+)\)', col)
                    if func_match:
                        col = func_match.group(1)
                    
                    col = col.strip()
                    if col and len(col) < 50 and not col.startswith('@') and col not in ['DISTINCT', 'TOP', 'NULL']:
                        columns.append(col)
        
        # Remove duplicates while preserving order
        tables = list(dict.fromkeys(tables))[:20]  # ✅ Increased from 10
        columns = list(dict.fromkeys(columns))[:50]  # ✅ Increased from 20
        
        return tables, columns
    
    def get_dynamic_role(self, activity: dict) -> str:
        """Dynamically determine activity role"""
        activity_type = activity.get('type', '')
        type_props = activity.get('typeProperties', {})
        
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
            'ExecuteDataFlow': 'Data Flow',
            'AzureFunctionActivity': 'Azure Function',
            'Fail': 'Fail',
            'Validation': 'Validate'
        }
        
        role = roles.get(activity_type, 'Process')
        
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
        
        return role
    
    def extract_datasets_dynamic(self, activity: dict, rec: dict):
        """Extract datasets from any location dynamically"""
        datasets = []
        
        def find_datasets(obj, prefix=''):
            if isinstance(obj, dict):
                if 'referenceName' in obj and 'type' in obj:
                    if obj.get('type') == 'DatasetReference':
                        datasets.append(f"{prefix}{self.extract_name(obj['referenceName'])}")
                
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
    
    def extract_files_dynamic(self, type_props: dict, rec: dict):
        """Extract file paths"""
        paths = []
        
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
        """Extract additional values"""
        values = []
        
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
        
        if values:
            if rec['Values Info']:
                rec['Values Info'] += ' | ' + self.sanitize_value(' | '.join(values))
            else:
                rec['Values Info'] = self.sanitize_value(' | '.join(values))
    
    def extract_parameters_dynamic(self, activity: dict, rec: dict):
        """✅ FIXED: Extract parameters with proper regex"""
        params = set()
        
        try:
            activity_str = json.dumps(activity)
            
            # FIXED: Proper regex patterns (removed KATEX errors)
            patterns = [
                (r"@pipeline\(\)\.parameters\.(\w+)", "P:{}"),
                (r"@variables\('(\w+)'\)", "V:{}"),
                (r"@activity\('([^']+)'\)", "Act:{}"),
                (r"@dataset\(\)\.(\w+)", "DS:{}"),
                (r"@linkedService\(\)\.(\w+)", "LS:{}"),
                (r"@trigger\(\)\.(\w+)", "Trg:{}"),
                (r"@dataflow\(\)\.(\w+)", "DF:{}")
            ]
            
            for pattern, formatter in patterns:
                matches = re.findall(pattern, activity_str)
                for match in matches:
                    params.add(formatter.format(match))
            
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
    
    def parse_dataset(self, resource: dict):
        """✅ ENHANCED: Parse dataset with IR column"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            ds_type = props.get('type', 'Unknown')
            type_props = props.get('typeProperties', {})
            
            self.metrics['dataset_types'][ds_type] += 1
            self.dataset_references[name] = resource
            
            # ✅ NEW: Get Integration Runtime
            ir_name = ''
            ls = props.get('linkedServiceName', {})
            if isinstance(ls, dict):
                ls_ref = self.extract_name(ls.get('referenceName', ''))
                if ls_ref and ls_ref in self.linkedservice_references:
                    ls_resource = self.linkedservice_references[ls_ref]
                    ls_props = ls_resource.get('properties', {})
                    connect_via = ls_props.get('connectVia', {})
                    if isinstance(connect_via, dict):
                        ir_name = self.extract_name(connect_via.get('referenceName', ''))
            
            rec = {
                'Dataset': self.sanitize_value(name),
                'Type': self.sanitize_value(ds_type),
                'LinkedService': self.sanitize_value(self.extract_name(ls.get('referenceName', '')) if isinstance(ls, dict) else ''),
                'IntegrationRuntime': self.sanitize_value(ir_name),  # ✅ NEW
                'Location': '',
                'Schema': '',
                'Parameters': '',
                'Folder': self.sanitize_value(self.get_nested(props, 'folder.name')),
                'Description': self.sanitize_value(props.get('description', ''))
            }
            
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
            
            self.metrics['linked_service_types'][ls_type] += 1
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
            
            rec['Authentication'] = self.sanitize_value(self.detect_auth_dynamic(type_props))
            rec['Connection'] = self.sanitize_value(self.extract_connection_dynamic(ls_type, type_props))
            
            self.results['linked_services'].append(rec)
            
        except Exception as e:
            self.log_error(resource, f"LinkedService: {e}")
    
    def parse_trigger(self, resource: dict):
        """Parse trigger"""
        try:
            name = self.extract_name(resource.get('name', ''))
            props = resource.get('properties', {})
            trigger_type = props.get('type', 'Unknown')
            type_props = props.get('typeProperties', {})
            
            self.metrics['trigger_types'][trigger_type] += 1
            self.trigger_references[name] = resource
            
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
            
            # Extract schedule
            if trigger_type == 'ScheduleTrigger':
                recurrence = type_props.get('recurrence', {})
                if isinstance(recurrence, dict):
                    freq = recurrence.get('frequency', '')
                    interval = recurrence.get('interval', 1)
                    
                    rec['Frequency'] = self.sanitize_value(freq)
                    rec['Interval'] = str(interval)
                    
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
                        elif hours:
                            schedule_parts.append(f"at hour(s): {', '.join(map(str, hours[:10]))}")
                        elif minutes:
                            schedule_parts.append(f"at minute(s): {', '.join(map(str, minutes[:10]))}")
                    
                    rec['Schedule'] = self.sanitize_value(' '.join(schedule_parts))
                    
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
        """Extract relationships and dependencies"""
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
                            'SourceTable': activity.get('SourceTable', ''),
                            'SinkTable': activity.get('SinkTable', ''),
                            'Transformation': activity.get('Role', 'Copy')
                        })
        
        # Extract data lineage for DataFlow activities
        for activity in self.results['activities']:
            if activity['Activity Type'] == 'ExecuteDataFlow':
                dataflow_name = activity.get('DataFlow', '')
                if dataflow_name:
                    for df_lineage in self.results['dataflow_lineage']:
                        if df_lineage['DataFlow'] == dataflow_name:
                            self.results['data_lineage'].append({
                                'Pipeline': activity['Pipeline'],
                                'Activity': activity['Activity'],
                                'Type': 'DataFlow',
                                'Source': f"{df_lineage['SourceName']} ({df_lineage['SourceLinkedService'] or df_lineage['SourceDataset']})",
                                'Sink': f"{df_lineage['SinkName']} ({df_lineage['SinkLinkedService'] or df_lineage['SinkDataset']})",
                                'SourceTable': df_lineage.get('SourceTable', ''),
                                'SinkTable': df_lineage.get('SinkTable', ''),
                                'Transformation': f"DataFlow:{dataflow_name}"
                            })
        
        print(f"✅ Relationships extracted: {len(self.results['data_lineage'])} lineage records")
    
    def analyze_orphaned_resources(self):
        """✅ NEW: Identify orphaned resources (pipelines, triggers, datasets, linked services)"""
        print("\n🔍 Analyzing orphaned resources...")
        
        # Orphaned Pipelines: Pipelines not referenced by any trigger or other pipeline
        all_pipelines = set(p['Pipeline'] for p in self.results['pipelines'])
        triggered_pipelines = set()
        
        for detail in self.results['trigger_details']:
            triggered_pipelines.add(detail['Pipeline'])
        
        for pipeline in self.pipeline_usage.keys():
            triggered_pipelines.add(pipeline)
        
        orphaned_pipelines = all_pipelines - triggered_pipelines
        
        for pipeline in orphaned_pipelines:
            self.results['orphaned_pipelines'].append({
                'Pipeline': pipeline,
                'Reason': 'Not referenced by any trigger or ExecutePipeline activity',
                'Type': 'Orphaned'
            })
        
        # Orphaned Datasets: Datasets not used by any pipeline
        all_datasets = set(d['Dataset'] for d in self.results['datasets'])
        used_datasets = set(self.dataset_usage.keys())
        
        orphaned_datasets = all_datasets - used_datasets
        
        for dataset in orphaned_datasets:
            self.results['orphaned_datasets'].append({
                'Dataset': dataset,
                'Reason': 'Not used by any pipeline activity',
                'Type': 'Orphaned'
            })
        
        # Orphaned Linked Services: Not used by any dataset or dataflow
        all_linked_services = set(ls['LinkedService'] for ls in self.results['linked_services'])
        used_linked_services = set()
        
        for ds in self.results['datasets']:
            if ds['LinkedService']:
                used_linked_services.add(ds['LinkedService'])
        
        for df_lineage in self.results['dataflow_lineage']:
            if df_lineage['SourceLinkedService']:
                used_linked_services.add(df_lineage['SourceLinkedService'])
            if df_lineage['SinkLinkedService']:
                used_linked_services.add(df_lineage['SinkLinkedService'])
        
        orphaned_linked_services = all_linked_services - used_linked_services
        
        for ls in orphaned_linked_services:
            self.results['orphaned_linked_services'].append({
                'LinkedService': ls,
                'Reason': 'Not used by any dataset or dataflow',
                'Type': 'Orphaned'
            })
        
        # Orphaned Triggers: Triggers that reference non-existent pipelines
        for detail in self.results['trigger_details']:
            if detail['Pipeline'] not in all_pipelines:
                self.results['orphaned_triggers'].append({
                    'Trigger': detail['Trigger'],
                    'Pipeline': detail['Pipeline'],
                    'Reason': 'References non-existent pipeline',
                    'Type': 'BrokenReference'
                })
        
        print(f"  • Orphaned Pipelines: {len(orphaned_pipelines)}")
        print(f"  • Orphaned Datasets: {len(orphaned_datasets)}")
        print(f"  • Orphaned Linked Services: {len(orphaned_linked_services)}")
        print(f"  • Broken Trigger References: {len(self.results['orphaned_triggers'])}")
    
    def analyze_impact(self):
        """✅ NEW: Analyze impact of deleting resources (upstream/downstream)"""
        print("\n📊 Analyzing impact...")
        
        # For each pipeline, determine impact of deletion
        for pipeline in self.results['pipelines']:
            pipeline_name = pipeline['Pipeline']
            
            # Upstream: What triggers this pipeline?
            upstream_triggers = [d['Trigger'] for d in self.results['trigger_details'] if d['Pipeline'] == pipeline_name]
            upstream_pipelines = [p for p, called in self.pipeline_usage.items() if pipeline_name in called]
            
            # Downstream: What does this pipeline call?
            downstream_pipelines = list(self.pipeline_usage.get(pipeline_name, set()))
            
            # What datasets does it use?
            used_datasets = [d for d, pipelines in self.dataset_usage.items() if pipeline_name in pipelines]
            
            # What dataflows does it use?
            used_dataflows = list(self.dataflow_usage.get(pipeline_name, set()))
            
            self.results['impact_analysis'].append({
                'Pipeline': pipeline_name,
                'UpstreamTriggers': ', '.join(upstream_triggers) if upstream_triggers else 'None',
                'UpstreamPipelines': ', '.join(upstream_pipelines) if upstream_pipelines else 'None',
                'DownstreamPipelines': ', '.join(downstream_pipelines) if downstream_pipelines else 'None',
                'UsedDatasets': ', '.join(used_datasets[:10]) if used_datasets else 'None',
                'UsedDataFlows': ', '.join(used_dataflows) if used_dataflows else 'None',
                'Impact': 'HIGH' if (upstream_triggers or upstream_pipelines or downstream_pipelines) else 'LOW'
            })
        
        print(f"✅ Impact analysis complete: {len(self.results['impact_analysis'])} pipelines analyzed")
    
    def calculate_activity_counts(self):
        """✅ NEW: Calculate activity usage summary"""
        print("\n📈 Calculating activity counts...")
        
        for activity_type, count in self.metrics['activity_types'].most_common():
            self.results['activity_count'].append({
                'ActivityType': activity_type,
                'Count': count,
                'Percentage': f"{count / len(self.results['activities']) * 100:.1f}%" if self.results['activities'] else '0%'
            })
        
        print(f"✅ Activity counts calculated: {len(self.results['activity_count'])} types")
    
    def export_to_excel(self):
        """✅ ENHANCED: Export with reordered sheets and consistent naming for Streamlit"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        # ✅ NEW: Consistent naming for Streamlit automation
        excel_file = output_dir / f'adf_analysis_latest.xlsx'  # Consistent name
        archive_file = output_dir / f'adf_analysis_{timestamp}.xlsx'  # Archive with timestamp
        
        print(f"\n📊 Exporting to: {excel_file}")
        
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                
                # Summary sheet
                summary = [
                    {'Metric': 'Analysis Date', 'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                    {'Metric': 'Source File', 'Value': str(self.json_path)},
                    {'Metric': '---', 'Value': '---'},
                    {'Metric': 'Total Pipelines', 'Value': len(self.results['pipelines'])},
                    {'Metric': 'Total Activities', 'Value': len(self.results['activities'])},
                    {'Metric': 'Total DataFlows', 'Value': len(self.results['dataflows'])},
                    {'Metric': 'Total Datasets', 'Value': len(self.results['datasets'])},
                    {'Metric': 'Total Linked Services', 'Value': len(self.results['linked_services'])},
                    {'Metric': 'Total Triggers', 'Value': len(self.results['triggers'])},
                    {'Metric': 'Total Integration Runtimes', 'Value': len(self.results['integration_runtimes'])},
                    {'Metric': '---', 'Value': '---'},
                    {'Metric': 'Orphaned Pipelines', 'Value': len(self.results['orphaned_pipelines'])},
                    {'Metric': 'Orphaned Datasets', 'Value': len(self.results['orphaned_datasets'])},
                    {'Metric': 'Orphaned Linked Services', 'Value': len(self.results['orphaned_linked_services'])},
                    {'Metric': 'Broken Trigger References', 'Value': len(self.results['orphaned_triggers'])},
                    {'Metric': '---', 'Value': '---'},
                    {'Metric': 'Data Lineage Records', 'Value': len(self.results['data_lineage'])},
                    {'Metric': 'Parse Errors', 'Value': len(self.results['errors'])}
                ]
                
                pd.DataFrame(summary).to_excel(writer, sheet_name='Summary', index=False)
                print(f"  ✓ Summary")
                
                # ✅ REORDERED: Pipeline first per feedback
                main_sheets = [
                    ('Pipelines', self.results['pipelines']),
                    ('Activities', self.results['activities']),
                    ('ActivityCount', self.results['activity_count']),  # ✅ NEW
                    ('DataFlows', self.results['dataflows']),
                    ('DataFlowLineage', self.results['dataflow_lineage']),
                    ('DataFlowTransformations', self.results['dataflow_transformations']),
                    ('Datasets', self.results['datasets']),
                    ('LinkedServices', self.results['linked_services']),
                    ('Triggers', self.results['triggers']),
                    ('TriggerDetails', self.results['trigger_details']),
                    ('IntegrationRuntimes', self.results['integration_runtimes']),
                    ('DataLineage', self.results['data_lineage']),
                    # ✅ NEW: Orphaned resource sheets
                    ('OrphanedPipelines', self.results['orphaned_pipelines']),
                    ('OrphanedDatasets', self.results['orphaned_datasets']),
                    ('OrphanedLinkedServices', self.results['orphaned_linked_services']),
                    ('OrphanedTriggers', self.results['orphaned_triggers']),
                    # ✅ NEW: Impact analysis
                    ('ImpactAnalysis', self.results['impact_analysis'])
                ]
                
                for sheet_name, data in main_sheets:
                    if data:
                        df = pd.DataFrame(data)
                        safe_sheet_name = sheet_name[:31]
                        df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                        print(f"  ✓ {sheet_name}: {len(df)} rows")
                
                # Optional sheets
                optional_sheets = [
                    ('ManagedVNets', self.results['managed_virtual_networks']),
                    ('Credentials', self.results['credentials'])
                ]
                
                for sheet_name, data in optional_sheets:
                    if data:
                        df = pd.DataFrame(data)
                        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                        print(f"  ✓ {sheet_name}: {len(df)} rows")
                
                # Statistics
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
                
                # Errors
                if self.results['errors']:
                    pd.DataFrame(self.results['errors']).to_excel(writer, sheet_name='Errors', index=False)
                    print(f"  ⚠ Errors: {len(self.results['errors'])}")
            
            print(f"\n✅ Export complete: {excel_file}")
            
            # ✅ NEW: Also save archive copy
            import shutil
            shutil.copy(excel_file, archive_file)
            print(f"✅ Archive saved: {archive_file}")
            
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
        print(f"  • Pipelines: {len(self.results['pipelines'])}")
        print(f"  • Activities: {len(self.results['activities'])}")
        print(f"  • DataFlows: {len(self.results['dataflows'])}")
        print(f"  • Datasets: {len(self.results['datasets'])}")
        print(f"  • Linked Services: {len(self.results['linked_services'])}")
        print(f"  • Triggers: {len(self.results['triggers'])}")
        print(f"  • Integration Runtimes: {len(self.results['integration_runtimes'])}")
        
        print(f"\n🔍 Orphaned Resources:")
        print(f"  • Pipelines: {len(self.results['orphaned_pipelines'])}")
        print(f"  • Datasets: {len(self.results['orphaned_datasets'])}")
        print(f"  • Linked Services: {len(self.results['orphaned_linked_services'])}")
        print(f"  • Broken Triggers: {len(self.results['orphaned_triggers'])}")
        
        if self.metrics['activity_types']:
            print(f"\n⚡ Top Activities:")
            for atype, count in self.metrics['activity_types'].most_common(5):
                print(f"  • {atype:30} : {count:4d}")
        
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
║      Ultimate ADF Parser v8.0 - Impact Analyzer Edition                     ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Usage: python adf_analyzer.py <template.json> [--no-discovery]

✅ NEW FEATURES (Meeting Requirements):
  • Integration Runtime columns in Activities, Datasets, DataFlows
  • Source/Sink table names for Copy activities and DataFlows
  • Enhanced SQL extraction (max 5000 chars) with better table parsing
  • Stored Procedure activity support with SP name field
  • Orphaned resource detection (Pipelines, Datasets, LinkedServices, Triggers)
  • Impact Analysis (Upstream/Downstream dependencies)
  • Activity Count summary
  • Reordered sheets (Pipeline first after Summary)
  • Consistent output naming (adf_analysis_latest.xlsx) for Streamlit automation
  • Fixed regex patterns (removed KATEX errors)

Example:
  python adf_analyzer.py factory_arm_template.json
        """)
        sys.exit(1)
    
    json_path = sys.argv[1]
    enable_discovery = '--no-discovery' not in sys.argv
    
    parser = UltimateADFParser(json_path, enable_discovery=enable_discovery)
    success = parser.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()