"""
Advanced ARM Template Pattern Discovery Engine v2.0
Automatically discovers ALL patterns, references, and relationships in ARM templates
Generates comprehensive parsing templates for dynamic parser enhancement
"""

import json
import sys
import re
from pathlib import Path
from collections import defaultdict, Counter
from typing import Any, Dict, List, Set, Tuple, Optional
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class ARMTemplatePatternDiscovery:
    """Advanced pattern discovery for ARM templates"""
    
    def __init__(self, json_path: str):
        self.json_path = json_path
        self.data = None
        
        # Pattern tracking
        self.resource_patterns = defaultdict(lambda: {
            'count': 0,
            'paths': set(),
            'properties': defaultdict(set),
            'type_properties': defaultdict(set),
            'references': defaultdict(list),
            'samples': []
        })
        
        # Reference patterns
        self.reference_patterns = {
            'dataset_refs': [],
            'pipeline_refs': [],
            'linkedservice_refs': [],
            'dataflow_refs': [],
            'trigger_refs': [],
            'ir_refs': [],
            'activity_refs': [],
            'parameter_refs': [],
            'variable_refs': []
        }
        
        # Expression patterns
        self.expression_patterns = defaultdict(list)
        
        # Dependency map
        self.dependencies = defaultdict(lambda: {
            'depends_on': set(),
            'referenced_by': set(),
            'uses': defaultdict(set)
        })
        
        # Structure discoveries
        self.discoveries = {
            'resource_types': Counter(),
            'activity_types': Counter(),
            'dataset_types': Counter(),
            'linkedservice_types': Counter(),
            'trigger_types': Counter(),
            'dataflow_types': Counter(),
            'transformation_types': Counter(),
            'source_sink_types': Counter(),
            'authentication_types': Counter(),
            'expression_functions': Counter(),
            'parameter_patterns': Counter(),
            'nested_structures': defaultdict(dict)
        }
        
        # Path mappings for parser generation
        self.parser_templates = {}
        
    def load_json(self) -> bool:
        """Load JSON file"""
        try:
            print(f"📂 Loading ARM Template: {self.json_path}")
            with open(self.json_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            file_size = Path(self.json_path).stat().st_size
            print(f"✅ Loaded: {file_size/1024/1024:.2f} MB")
            
            # Quick validation
            if 'resources' in self.data:
                print(f"   Found {len(self.data['resources'])} resources")
            
            return True
            
        except Exception as e:
            print(f"❌ Error loading: {e}")
            return False
    
    def discover_patterns(self):
        """Main discovery process"""
        print("\n🔍 Discovering Patterns...\n")
        
        # Phase 1: Discover all resource patterns
        self.discover_resource_patterns()
        
        # Phase 2: Discover references and links
        self.discover_references()
        
        # Phase 3: Discover expressions and parameters
        self.discover_expressions()
        
        # Phase 4: Build dependency graph
        self.build_dependency_graph()
        
        # Phase 5: Generate parser templates
        self.generate_parser_templates()
        
        print("\n✅ Pattern discovery complete!")
        self.print_discovery_summary()
    
    def discover_resource_patterns(self):
        """Discover all resource type patterns"""
        print("Phase 1: Discovering resource patterns...")
        
        resources = self.data.get('resources', [])
        
        for idx, resource in enumerate(resources):
            if not isinstance(resource, dict):
                continue
            
            # Get resource type
            res_type = resource.get('type', '')
            res_category = res_type.split('/')[-1] if res_type else 'unknown'
            
            # Track resource
            self.discoveries['resource_types'][res_category] += 1
            
            # Detailed analysis based on type
            if 'pipelines' in res_type.lower():
                self.analyze_pipeline_pattern(resource, idx)
            elif 'dataflows' in res_type.lower():
                self.analyze_dataflow_pattern(resource, idx)
            elif 'datasets' in res_type.lower():
                self.analyze_dataset_pattern(resource, idx)
            elif 'linkedservices' in res_type.lower():
                self.analyze_linkedservice_pattern(resource, idx)
            elif 'triggers' in res_type.lower():
                self.analyze_trigger_pattern(resource, idx)
            elif 'integrationruntimes' in res_type.lower():
                self.analyze_integration_runtime_pattern(resource, idx)
            else:
                # Generic pattern discovery for unknown types
                self.analyze_generic_pattern(resource, res_category, idx)
            
            # Store pattern
            pattern_key = res_category
            self.resource_patterns[pattern_key]['count'] += 1
            self.resource_patterns[pattern_key]['samples'].append(idx)
            
            # Discover nested structures
            self.discover_nested_structures(resource, f"resources[{idx}]", res_category)
    
    def analyze_pipeline_pattern(self, resource: dict, idx: int):
        """Analyze pipeline patterns"""
        props = resource.get('properties', {})
        activities = props.get('activities', [])
        
        # Track activity patterns
        for act_idx, activity in enumerate(activities):
            if isinstance(activity, dict):
                act_type = activity.get('type', 'Unknown')
                self.discoveries['activity_types'][act_type] += 1
                
                # Track type properties structure
                type_props = activity.get('typeProperties', {})
                self.discover_property_patterns(type_props, f"activity.{act_type}")
                
                # Special handling for ExecuteDataFlow
                if act_type == 'ExecuteDataFlow':
                    dataflow = type_props.get('dataflow', {})
                    if isinstance(dataflow, dict) and 'referenceName' in dataflow:
                        self.reference_patterns['dataflow_refs'].append({
                            'from': f"pipeline[{idx}].activity[{act_idx}]",
                            'to': dataflow.get('referenceName'),
                            'type': 'ExecuteDataFlow'
                        })
    
    def analyze_dataflow_pattern(self, resource: dict, idx: int):
        """Analyze dataflow patterns"""
        props = resource.get('properties', {})
        flow_type = props.get('type', 'MappingDataFlow')
        self.discoveries['dataflow_types'][flow_type] += 1
        
        type_props = props.get('typeProperties', {})
        
        # Analyze sources
        sources = type_props.get('sources', [])
        for src_idx, source in enumerate(sources):
            if isinstance(source, dict):
                # Track source patterns
                if 'linkedService' in source:
                    ls = source['linkedService']
                    if isinstance(ls, dict) and 'referenceName' in ls:
                        self.reference_patterns['linkedservice_refs'].append({
                            'from': f"dataflow[{idx}].source[{src_idx}]",
                            'to': ls.get('referenceName'),
                            'type': 'DataFlowSource'
                        })
                
                # Track source types from script if available
                self.analyze_dataflow_script(type_props.get('scriptLines', []))
        
        # Analyze sinks
        sinks = type_props.get('sinks', [])
        for sink_idx, sink in enumerate(sinks):
            if isinstance(sink, dict):
                if 'linkedService' in sink:
                    ls = sink['linkedService']
                    if isinstance(ls, dict) and 'referenceName' in ls:
                        self.reference_patterns['linkedservice_refs'].append({
                            'from': f"dataflow[{idx}].sink[{sink_idx}]",
                            'to': ls.get('referenceName'),
                            'type': 'DataFlowSink'
                        })
        
        # Analyze transformations
        transformations = type_props.get('transformations', [])
        for trans in transformations:
            if isinstance(trans, dict):
                trans_name = trans.get('name', '')
                # Try to determine transformation type from name or script
                self.discoveries['transformation_types'][trans_name] += 1
    
    def analyze_dataflow_script(self, script_lines: list):
        """Analyze dataflow script for patterns"""
        if not isinstance(script_lines, list):
            return
        
        script_text = '\n'.join(script_lines[:1000])  # Analyze first 1000 lines
        
        # Find transformation patterns
        trans_patterns = {
            r'~>\s*(\w+)': 'transformation_name',
            r'(\w+)\s*KATEX_INLINE_OPEN': 'function_call',
            r'sourceKATEX_INLINE_OPEN': 'source_definition',
            r'sinkKATEX_INLINE_OPEN': 'sink_definition',
            r'selectKATEX_INLINE_OPEN': 'select_transformation',
            r'deriveKATEX_INLINE_OPEN': 'derive_transformation',
            r'aggregateKATEX_INLINE_OPEN': 'aggregate_transformation',
            r'joinKATEX_INLINE_OPEN': 'join_transformation',
            r'filterKATEX_INLINE_OPEN': 'filter_transformation',
            r'sortKATEX_INLINE_OPEN': 'sort_transformation',
            r'splitKATEX_INLINE_OPEN': 'split_transformation',
            r'unionKATEX_INLINE_OPEN': 'union_transformation',
            r'windowKATEX_INLINE_OPEN': 'window_transformation',
            r'pivotKATEX_INLINE_OPEN': 'pivot_transformation',
            r'unpivotKATEX_INLINE_OPEN': 'unpivot_transformation',
            r'flattenKATEX_INLINE_OPEN': 'flatten_transformation',
            r'parseKATEX_INLINE_OPEN': 'parse_transformation',
            r'alterKATEX_INLINE_OPEN': 'alter_transformation'
        }
        
        for pattern, trans_type in trans_patterns.items():
            matches = re.findall(pattern, script_text, re.IGNORECASE)
            if matches:
                self.discoveries['transformation_types'][trans_type] += len(matches)
    
    def analyze_dataset_pattern(self, resource: dict, idx: int):
        """Analyze dataset patterns"""
        props = resource.get('properties', {})
        ds_type = props.get('type', 'Unknown')
        self.discoveries['dataset_types'][ds_type] += 1
        
        # Track linked service reference
        ls_ref = props.get('linkedServiceName', {})
        if isinstance(ls_ref, dict) and 'referenceName' in ls_ref:
            self.reference_patterns['linkedservice_refs'].append({
                'from': f"dataset[{idx}]",
                'to': ls_ref.get('referenceName'),
                'type': 'DatasetLinkedService'
            })
    
    def analyze_linkedservice_pattern(self, resource: dict, idx: int):
        """Analyze linked service patterns"""
        props = resource.get('properties', {})
        ls_type = props.get('type', 'Unknown')
        self.discoveries['linkedservice_types'][ls_type] += 1
        
        # Analyze authentication patterns
        type_props = props.get('typeProperties', {})
        auth_type = self.detect_authentication_pattern(type_props)
        if auth_type:
            self.discoveries['authentication_types'][auth_type] += 1
        
        # Track integration runtime reference
        ir_ref = props.get('connectVia', {})
        if isinstance(ir_ref, dict) and 'referenceName' in ir_ref:
            self.reference_patterns['ir_refs'].append({
                'from': f"linkedservice[{idx}]",
                'to': ir_ref.get('referenceName'),
                'type': 'LinkedServiceIR'
            })
    
    def analyze_trigger_pattern(self, resource: dict, idx: int):
        """Analyze trigger patterns"""
        props = resource.get('properties', {})
        trigger_type = props.get('type', 'Unknown')
        self.discoveries['trigger_types'][trigger_type] += 1
        
        # Track pipeline references
        pipelines = props.get('pipelines', [])
        for pipe_idx, pipeline in enumerate(pipelines):
            if isinstance(pipeline, dict):
                pipe_ref = pipeline.get('pipelineReference', {})
                if isinstance(pipe_ref, dict) and 'referenceName' in pipe_ref:
                    self.reference_patterns['pipeline_refs'].append({
                        'from': f"trigger[{idx}]",
                        'to': pipe_ref.get('referenceName'),
                        'type': 'TriggerPipeline'
                    })
    
    def analyze_integration_runtime_pattern(self, resource: dict, idx: int):
        """Analyze integration runtime patterns"""
        props = resource.get('properties', {})
        ir_type = props.get('type', 'Unknown')
        self.discoveries['integration_runtime_types'] = self.discoveries.get('integration_runtime_types', Counter())
        self.discoveries['integration_runtime_types'][ir_type] += 1
    
    def analyze_generic_pattern(self, resource: dict, category: str, idx: int):
        """Generic pattern analysis for unknown resource types"""
        # Store the structure for later analysis
        self.discoveries['nested_structures'][category][f"resource_{idx}"] = self.get_structure_template(resource)
    
    def discover_property_patterns(self, obj: dict, prefix: str):
        """Discover patterns in properties"""
        if not isinstance(obj, dict):
            return
        
        for key, value in obj.items():
            path = f"{prefix}.{key}"
            
            # Track property types
            if isinstance(value, dict):
                # Check for reference patterns
                if 'referenceName' in value and 'type' in value:
                    ref_type = value.get('type', '')
                    if 'Dataset' in ref_type:
                        self.reference_patterns['dataset_refs'].append({
                            'path': path,
                            'reference': value.get('referenceName')
                        })
                    elif 'Pipeline' in ref_type:
                        self.reference_patterns['pipeline_refs'].append({
                            'path': path,
                            'reference': value.get('referenceName')
                        })
                    elif 'LinkedService' in ref_type:
                        self.reference_patterns['linkedservice_refs'].append({
                            'path': path,
                            'reference': value.get('referenceName')
                        })
                
                # Recurse
                self.discover_property_patterns(value, path)
                
            elif isinstance(value, list):
                for item in value[:10]:  # Sample first 10 items
                    if isinstance(item, dict):
                        self.discover_property_patterns(item, f"{path}[]")
    
    def discover_nested_structures(self, obj: Any, path: str, category: str):
        """Discover nested structure patterns"""
        if isinstance(obj, dict):
            structure = {}
            for key, value in obj.items():
                if isinstance(value, dict):
                    structure[key] = 'object'
                    self.discover_nested_structures(value, f"{path}.{key}", category)
                elif isinstance(value, list):
                    structure[key] = 'array'
                    if value and isinstance(value[0], dict):
                        self.discover_nested_structures(value[0], f"{path}.{key}[0]", category)
                else:
                    structure[key] = type(value).__name__
            
            self.resource_patterns[category]['properties'][path] = structure
    
    def discover_references(self):
        """Discover all reference patterns"""
        print("Phase 2: Discovering references and links...")
        
        # Find all expressions with references
        def find_references(obj, path=''):
            if isinstance(obj, str):
                # Check for reference expressions
                ref_patterns = [
                    (r"@pipelineKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.parameters\.(\w+)", 'parameter'),
                    (r"@variablesKATEX_INLINE_OPEN'([^']+)'KATEX_INLINE_CLOSE", 'variable'),
                    (r"@activityKATEX_INLINE_OPEN'([^']+)'KATEX_INLINE_CLOSE", 'activity'),
                    (r"@datasetKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", 'dataset_param'),
                    (r"@linkedServiceKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", 'linkedservice_param'),
                    (r"@triggerKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", 'trigger_param'),
                    (r"concatKATEX_INLINE_OPENparametersKATEX_INLINE_OPEN'factoryName'KATEX_INLINE_CLOSE, '/([^']+)'KATEX_INLINE_CLOSE", 'resource_name')
                ]
                
                for pattern, ref_type in ref_patterns:
                    matches = re.findall(pattern, obj)
                    for match in matches:
                        self.expression_patterns[ref_type].append({
                            'path': path,
                            'expression': obj[:200],
                            'reference': match
                        })
                        
            elif isinstance(obj, dict):
                for key, value in obj.items():
                    find_references(value, f"{path}.{key}" if path else key)
            elif isinstance(obj, list):
                for idx, item in enumerate(obj[:100]):  # Limit to first 100 items
                    find_references(item, f"{path}[{idx}]" if path else f"[{idx}]")
        
        find_references(self.data.get('resources', []))
    
    def discover_expressions(self):
        """Discover expression patterns"""
        print("Phase 3: Discovering expressions and parameters...")
        
        # Find all expression functions used
        def find_expressions(obj):
            if isinstance(obj, str):
                # Common ADF expression functions
                functions = [
                    'concat', 'substring', 'replace', 'split', 'join',
                    'toLower', 'toUpper', 'trim', 'length', 'indexOf',
                    'contains', 'startsWith', 'endsWith', 'equals',
                    'greater', 'less', 'greaterOrEquals', 'lessOrEquals',
                    'if', 'and', 'or', 'not', 'coalesce',
                    'utcnow', 'addDays', 'addHours', 'formatDateTime',
                    'int', 'string', 'bool', 'array', 'json',
                    'pipeline', 'variables', 'activity', 'dataset',
                    'linkedService', 'trigger', 'item', 'items'
                ]
                
                for func in functions:
                    if f"@{func}(" in obj or f"{func}(" in obj:
                        self.discoveries['expression_functions'][func] += 1
                        
            elif isinstance(obj, dict):
                for value in obj.values():
                    find_expressions(value)
            elif isinstance(obj, list):
                for item in obj[:100]:
                    find_expressions(item)
        
        find_expressions(self.data)
    
    def detect_authentication_pattern(self, type_props: dict) -> str:
        """Detect authentication pattern in linked service"""
        auth_indicators = {
            'authenticationType': lambda v: v,
            'servicePrincipalId': 'ServicePrincipal',
            'accountKey': 'AccountKey',
            'connectionString': 'ConnectionString',
            'sasUri': 'SAS',
            'sasToken': 'SAS',
            'credential': 'ManagedIdentity',
            'useManagedIdentity': 'ManagedIdentity',
            'username': 'Basic',
            'password': 'Basic',
            'accessToken': 'OAuth',
            'clientId': 'OAuth/ServicePrincipal'
        }
        
        for key, auth_type in auth_indicators.items():
            if key in type_props:
                if callable(auth_type):
                    return auth_type(type_props[key])
                return auth_type
        
        return 'Unknown'
    
    def build_dependency_graph(self):
        """Build complete dependency graph"""
        print("Phase 4: Building dependency graph...")
        
        # Process all references to build graph
        for ref_list in self.reference_patterns.values():
            for ref in ref_list:
                if isinstance(ref, dict):
                    from_node = ref.get('from', ref.get('path', ''))
                    to_node = ref.get('to', ref.get('reference', ''))
                    ref_type = ref.get('type', 'unknown')
                    
                    if from_node and to_node:
                        self.dependencies[from_node]['uses'][ref_type].add(to_node)
                        self.dependencies[to_node]['referenced_by'].add(from_node)
    
    def generate_parser_templates(self):
        """Generate parser templates for discovered patterns"""
        print("Phase 5: Generating parser templates...")
        
        for res_type, pattern in self.resource_patterns.items():
            if pattern['count'] > 0:
                template = {
                    'resource_type': res_type,
                    'count': pattern['count'],
                    'common_properties': self.get_common_properties(pattern['properties']),
                    'parsing_paths': self.generate_parsing_paths(pattern['properties'])
                }
                self.parser_templates[res_type] = template
    
    def get_common_properties(self, properties: dict) -> dict:
        """Extract common properties from patterns"""
        common = {}
        for path, structure in properties.items():
            if isinstance(structure, dict):
                for key, value_type in structure.items():
                    if key not in common:
                        common[key] = {'types': set(), 'frequency': 0}
                    common[key]['types'].add(value_type)
                    common[key]['frequency'] += 1
        
        # Convert sets to lists for JSON serialization
        for key in common:
            common[key]['types'] = list(common[key]['types'])
        
        return common
    
    def generate_parsing_paths(self, properties: dict) -> list:
        """Generate parsing paths for properties"""
        paths = []
        for path, structure in properties.items():
            if isinstance(structure, dict):
                for key, value_type in structure.items():
                    full_path = f"{path}.{key}" if path else key
                    paths.append({
                        'path': full_path,
                        'type': value_type,
                        'extraction_method': self.suggest_extraction_method(value_type)
                    })
        return paths[:50]  # Limit to top 50 paths
    
    def suggest_extraction_method(self, value_type: str) -> str:
        """Suggest extraction method based on type"""
        if value_type == 'object':
            return 'recursive_extract'
        elif value_type == 'array':
            return 'iterate_extract'
        elif value_type in ['str', 'string']:
            return 'string_extract'
        elif value_type in ['int', 'float', 'number']:
            return 'numeric_extract'
        elif value_type in ['bool', 'boolean']:
            return 'boolean_extract'
        else:
            return 'generic_extract'
    
    def get_structure_template(self, obj: Any) -> Any:
        """Get structure template for an object"""
        if isinstance(obj, dict):
            template = {}
            for key, value in list(obj.items())[:20]:  # Limit to 20 keys
                template[key] = self.get_structure_template(value)
            return template
        elif isinstance(obj, list):
            if obj:
                return [self.get_structure_template(obj[0])]
            return []
        else:
            return type(obj).__name__
    
    def print_discovery_summary(self):
        """Print discovery summary"""
        print("\n" + "="*80)
        print("PATTERN DISCOVERY SUMMARY")
        print("="*80)
        
        print(f"\n📊 Resource Types Discovered: {len(self.discoveries['resource_types'])}")
        for res_type, count in self.discoveries['resource_types'].most_common(10):
            print(f"  • {res_type}: {count}")
        
        if self.discoveries['activity_types']:
            print(f"\n⚡ Activity Types: {len(self.discoveries['activity_types'])}")
            for act_type, count in self.discoveries['activity_types'].most_common(10):
                print(f"  • {act_type}: {count}")
        
        if self.discoveries['dataflow_types']:
            print(f"\n🌊 DataFlow Types: {len(self.discoveries['dataflow_types'])}")
            for df_type, count in self.discoveries['dataflow_types'].items():
                print(f"  • {df_type}: {count}")
        
        if self.discoveries['transformation_types']:
            print(f"\n🔄 Transformation Types: {len(self.discoveries['transformation_types'])}")
            for trans_type, count in self.discoveries['transformation_types'].most_common(10):
                print(f"  • {trans_type}: {count}")
        
        print(f"\n🔗 References Found:")
        for ref_type, refs in self.reference_patterns.items():
            if refs:
                print(f"  • {ref_type}: {len(refs)}")
        
        print(f"\n📝 Expression Functions Used: {len(self.discoveries['expression_functions'])}")
        for func, count in self.discoveries['expression_functions'].most_common(10):
            print(f"  • {func}: {count}")
    
    def generate_enhanced_parser_code(self) -> str:
        """Generate enhanced parser code based on discoveries"""
        code = '''
# Auto-generated parser enhancements based on pattern discovery
# Add these methods to your UltimateADFParser class

def parse_discovered_resources(self):
    """Parse all discovered resource types"""
    resources = self.data.get('resources', [])
    
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        
        res_type = resource.get('type', '')
        
'''
        
        # Add parsing methods for each discovered type
        for res_type in self.discoveries['resource_types'].keys():
            safe_name = res_type.replace('-', '_').replace('.', '_')
            code += f'''        if '{res_type.lower()}' in res_type.lower():
            self.parse_{safe_name}(resource)
'''
        
        # Add specific parsing methods
        for res_type, template in self.parser_templates.items():
            safe_name = res_type.replace('-', '_').replace('.', '_')
            code += f'''
def parse_{safe_name}(self, resource: dict):
    """Parse {res_type} resource"""
    try:
        name = self.extract_name(resource.get('name', ''))
        props = resource.get('properties', {{}})
        
        rec = {{
            'Type': '{res_type}',
            'Name': self.sanitize_value(name),
'''
            
            # Add common properties
            for prop, info in list(template.get('common_properties', {}).items())[:10]:
                code += f'''            '{prop}': self.sanitize_value(props.get('{prop}', '')),
'''
            
            code += '''        }
        
        # Store in appropriate results list
        if '{res_type}' not in self.results:
            self.results['{res_type}'] = []
        self.results['{res_type}'].append(rec)
        
    except Exception as e:
        self.log_error(resource, f"{res_type}: {{e}}")
'''
        
        return code
    
    def export_discoveries(self, output_dir: str = None):
        """Export all discoveries to files"""
        if output_dir is None:
            output_dir = Path('arm_discoveries')
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Export complete discovery JSON
        discovery_file = output_dir / f'discoveries_{timestamp}.json'
        discovery_data = {
            'source_file': self.json_path,
            'analyzed_at': datetime.now().isoformat(),
            'discoveries': {
                'resource_types': dict(self.discoveries['resource_types']),
                'activity_types': dict(self.discoveries['activity_types']),
                'dataflow_types': dict(self.discoveries['dataflow_types']),
                'transformation_types': dict(self.discoveries['transformation_types']),
                'expression_functions': dict(self.discoveries['expression_functions']),
                'authentication_types': dict(self.discoveries['authentication_types'])
            },
            'parser_templates': self.parser_templates,
            'reference_counts': {
                key: len(refs) for key, refs in self.reference_patterns.items()
            }
        }
        
        with open(discovery_file, 'w', encoding='utf-8') as f:
            json.dump(discovery_data, f, indent=2, default=str)
        print(f"✅ Discoveries JSON: {discovery_file}")
        
        # 2. Export Excel report
        excel_file = output_dir / f'discoveries_{timestamp}.xlsx'
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Resource types
            if self.discoveries['resource_types']:
                df = pd.DataFrame([
                    {'Resource Type': k, 'Count': v}
                    for k, v in self.discoveries['resource_types'].items()
                ])
                df.to_excel(writer, sheet_name='Resource Types', index=False)
            
            # Activity types
            if self.discoveries['activity_types']:
                df = pd.DataFrame([
                    {'Activity Type': k, 'Count': v}
                    for k, v in self.discoveries['activity_types'].items()
                ])
                df.to_excel(writer, sheet_name='Activity Types', index=False)
            
            # DataFlow info
            if self.discoveries['dataflow_types'] or self.discoveries['transformation_types']:
                df = pd.DataFrame([
                    {'Type': 'DataFlow Types', 'Name': k, 'Count': v}
                    for k, v in self.discoveries['dataflow_types'].items()
                ] + [
                    {'Type': 'Transformations', 'Name': k, 'Count': v}
                    for k, v in self.discoveries['transformation_types'].most_common(50)
                ])
                df.to_excel(writer, sheet_name='DataFlow Patterns', index=False)
            
            # References
            ref_summary = []
            for ref_type, refs in self.reference_patterns.items():
                if refs and isinstance(refs[0], dict):
                    for ref in refs[:100]:  # First 100 references
                        ref_summary.append({
                            'Reference Type': ref_type,
                            'From': ref.get('from', ref.get('path', '')),
                            'To': ref.get('to', ref.get('reference', '')),
                            'Context': ref.get('type', '')
                        })
            if ref_summary:
                df = pd.DataFrame(ref_summary)
                df.to_excel(writer, sheet_name='References', index=False)
            
            # Parser templates
            template_rows = []
            for res_type, template in self.parser_templates.items():
                template_rows.append({
                    'Resource Type': res_type,
                    'Count': template['count'],
                    'Common Properties': ', '.join(list(template['common_properties'].keys())[:10]),
                    'Parsing Paths': len(template['parsing_paths'])
                })
            if template_rows:
                df = pd.DataFrame(template_rows)
                df.to_excel(writer, sheet_name='Parser Templates', index=False)
        
        print(f"✅ Discoveries Excel: {excel_file}")
        
        # 3. Export enhanced parser code
        parser_file = output_dir / f'enhanced_parser_{timestamp}.py'
        with open(parser_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_enhanced_parser_code())
        print(f"✅ Enhanced Parser Code: {parser_file}")
        
        # 4. Export dependency graph
        dep_file = output_dir / f'dependencies_{timestamp}.json'
        dep_data = {
            'dependencies': {
                key: {
                    'uses': {k: list(v) for k, v in value['uses'].items()},
                    'referenced_by': list(value['referenced_by'])
                }
                for key, value in self.dependencies.items()
                if value['uses'] or value['referenced_by']
            }
        }
        with open(dep_file, 'w', encoding='utf-8') as f:
            json.dump(dep_data, f, indent=2, default=str)
        print(f"✅ Dependency Graph: {dep_file}")
        
        return output_dir


def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python arm_pattern_discovery.py <arm_template.json>")
        print("\nThis will discover ALL patterns in your ARM template and generate:")
        print("  1. Complete pattern analysis")
        print("  2. Parser templates for each resource type")
        print("  3. Dependency graphs")
        print("  4. Enhanced parser code")
        sys.exit(1)
    
    json_path = sys.argv[1]
    
    # Run discovery
    discoverer = ARMTemplatePatternDiscovery(json_path)
    
    if not discoverer.load_json():
        sys.exit(1)
    
    discoverer.discover_patterns()
    
    # Export results
    print("\n📝 Exporting discoveries...")
    output_dir = discoverer.export_discoveries()
    
    print("\n" + "="*80)
    print("✅ PATTERN DISCOVERY COMPLETE!")
    print("="*80)
    print(f"\n📊 Results exported to: {output_dir}")
    print("\n💡 Next steps:")
    print("  1. Review discoveries.json for all patterns found")
    print("  2. Check discoveries.xlsx for detailed analysis")  
    print("  3. Use enhanced_parser.py code to update your parser")
    print("  4. Review dependencies.json for resource relationships")
    

if __name__ == "__main__":
    main()