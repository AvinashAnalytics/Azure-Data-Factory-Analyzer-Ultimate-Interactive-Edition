"""
Advanced ARM Template Pattern Discovery Engine v3.0 - Production Ready
Automatically discovers ALL patterns, references, and relationships in ARM templates
Generates comprehensive parsing templates for dynamic parser enhancement

Features:
- Complete resource pattern analysis
- Reference tracking and dependency mapping
- Expression pattern detection
- Auto-generated parser code
- Multiple export formats (JSON, Excel, Python)
- Performance optimized for large templates
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
from dataclasses import dataclass, field
from enum import Enum
import traceback

warnings.filterwarnings('ignore')


class ResourceType(Enum):
    """Common ADF Resource Types"""
    PIPELINE = "pipelines"
    DATAFLOW = "dataflows"
    DATASET = "datasets"
    LINKED_SERVICE = "linkedservices"
    TRIGGER = "triggers"
    INTEGRATION_RUNTIME = "integrationruntimes"
    CREDENTIAL = "credentials"
    MANAGED_VN = "managedvirtualnetworks"
    MANAGED_PE = "managedprivateendpoints"


@dataclass
class ResourcePattern:
    """Structure to hold resource pattern information"""
    count: int = 0
    paths: Set[str] = field(default_factory=set)
    properties: Dict = field(default_factory=dict)
    type_properties: Dict = field(default_factory=dict)
    references: List[Dict] = field(default_factory=list)
    samples: List[int] = field(default_factory=list)
    property_stats: Dict = field(default_factory=dict)


class ARMTemplatePatternDiscovery:
    """Advanced pattern discovery for ARM templates"""
    
    def __init__(self, json_path: str, verbose: bool = True):
        self.json_path = json_path
        self.verbose = verbose
        self.data = None
        
        # Pattern tracking with structured data
        self.resource_patterns: Dict[str, ResourcePattern] = defaultdict(ResourcePattern)
        
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
            'variable_refs': [],
            'credential_refs': []
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
            'nested_structures': defaultdict(dict),
            'integration_runtime_types': Counter(),
            'credential_types': Counter()
        }
        
        # Path mappings for parser generation
        self.parser_templates = {}
        
        # Error tracking
        self.errors = []
        
    def log(self, message: str, level: str = "INFO"):
        """Log message if verbose"""
        if self.verbose:
            icons = {
                "INFO": "ℹ️",
                "SUCCESS": "✅",
                "ERROR": "❌",
                "WARNING": "⚠️",
                "PROGRESS": "🔄"
            }
            icon = icons.get(level, "•")
            print(f"{icon} {message}")
    
    def log_error(self, context: str, error: Exception):
        """Log and store errors"""
        error_info = {
            'context': context,
            'error': str(error),
            'traceback': traceback.format_exc()
        }
        self.errors.append(error_info)
        if self.verbose:
            print(f"❌ Error in {context}: {error}")
    
    def load_json(self) -> bool:
        """Load and validate JSON file"""
        try:
            self.log(f"Loading ARM Template: {self.json_path}")
            
            file_path = Path(self.json_path)
            if not file_path.exists():
                self.log(f"File not found: {self.json_path}", "ERROR")
                return False
            
            file_size = file_path.stat().st_size
            
            with open(self.json_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            self.log(f"Loaded: {file_size/1024/1024:.2f} MB", "SUCCESS")
            
            # Validation
            if not isinstance(self.data, dict):
                self.log("Invalid ARM template: root should be an object", "ERROR")
                return False
            
            if 'resources' in self.data:
                resource_count = len(self.data['resources'])
                self.log(f"Found {resource_count} resources")
            else:
                self.log("No 'resources' array found", "WARNING")
            
            return True
            
        except json.JSONDecodeError as e:
            self.log(f"JSON parsing error: {e}", "ERROR")
            return False
        except Exception as e:
            self.log_error("load_json", e)
            return False
    
    def discover_patterns(self):
        """Main discovery process"""
        self.log("\n🔍 Starting Pattern Discovery...\n")
        
        try:
            # Phase 1: Resource patterns
            self.log("Phase 1: Discovering resource patterns...", "PROGRESS")
            self.discover_resource_patterns()
            
            # Phase 2: References
            self.log("Phase 2: Discovering references and links...", "PROGRESS")
            self.discover_references()
            
            # Phase 3: Expressions
            self.log("Phase 3: Discovering expressions and parameters...", "PROGRESS")
            self.discover_expressions()
            
            # Phase 4: Dependencies
            self.log("Phase 4: Building dependency graph...", "PROGRESS")
            self.build_dependency_graph()
            
            # Phase 5: Parser templates
            self.log("Phase 5: Generating parser templates...", "PROGRESS")
            self.generate_parser_templates()
            
            self.log("\n✅ Pattern discovery complete!", "SUCCESS")
            self.print_discovery_summary()
            
        except Exception as e:
            self.log_error("discover_patterns", e)
    
    def discover_resource_patterns(self):
        """Discover all resource type patterns"""
        resources = self.data.get('resources', [])
        total = len(resources)
        
        for idx, resource in enumerate(resources):
            if not isinstance(resource, dict):
                continue
            
            # Progress indicator
            if total > 100 and idx % 50 == 0:
                self.log(f"  Processing resource {idx}/{total}...", "PROGRESS")
            
            try:
                # Get resource type
                res_type = resource.get('type', '')
                res_category = self._extract_category(res_type)
                
                # Track resource
                self.discoveries['resource_types'][res_category] += 1
                
                # Analyze based on type
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
                elif 'credentials' in res_type.lower():
                    self.analyze_credential_pattern(resource, idx)
                else:
                    self.analyze_generic_pattern(resource, res_category, idx)
                
                # Store pattern
                pattern = self.resource_patterns[res_category]
                pattern.count += 1
                pattern.samples.append(idx)
                if len(pattern.samples) > 10:  # Keep only first 10 samples
                    pattern.samples = pattern.samples[:10]
                
                # Discover nested structures
                self.discover_nested_structures(resource, f"resources[{idx}]", res_category)
                
            except Exception as e:
                self.log_error(f"analyze_resource[{idx}]", e)
    
    def _extract_category(self, res_type: str) -> str:
        """Extract category from resource type"""
        if not res_type:
            return 'unknown'
        parts = res_type.split('/')
        return parts[-1] if parts else 'unknown'
    
    def analyze_pipeline_pattern(self, resource: dict, idx: int):
        """Analyze pipeline patterns"""
        try:
            props = resource.get('properties', {})
            activities = props.get('activities', [])
            
            # Track pipeline parameters
            parameters = props.get('parameters', {})
            for param_name in parameters.keys():
                self.discoveries['parameter_patterns'][f"pipeline.{param_name}"] += 1
            
            # Track variables
            variables = props.get('variables', {})
            for var_name in variables.keys():
                self.discoveries['parameter_patterns'][f"pipeline.variable.{var_name}"] += 1
            
            # Analyze activities
            for act_idx, activity in enumerate(activities):
                if not isinstance(activity, dict):
                    continue
                
                act_type = activity.get('type', 'Unknown')
                act_name = activity.get('name', f'activity_{act_idx}')
                self.discoveries['activity_types'][act_type] += 1
                
                # Track type properties
                type_props = activity.get('typeProperties', {})
                self.discover_property_patterns(type_props, f"activity.{act_type}")
                
                # Handle specific activity types
                if act_type == 'ExecuteDataFlow':
                    self._handle_execute_dataflow(type_props, idx, act_idx, act_name)
                elif act_type == 'ExecutePipeline':
                    self._handle_execute_pipeline(type_props, idx, act_idx, act_name)
                elif act_type in ['Copy', 'CopyActivity']:
                    self._handle_copy_activity(type_props, idx, act_idx, act_name)
                elif 'Lookup' in act_type:
                    self._handle_lookup_activity(type_props, idx, act_idx, act_name)
                
                # Track dependencies
                depends_on = activity.get('dependsOn', [])
                for dep in depends_on:
                    if isinstance(dep, dict):
                        dep_activity = dep.get('activity', '')
                        if dep_activity:
                            self.reference_patterns['activity_refs'].append({
                                'from': f"pipeline[{idx}].{act_name}",
                                'to': dep_activity,
                                'type': 'ActivityDependency'
                            })
        
        except Exception as e:
            self.log_error(f"analyze_pipeline[{idx}]", e)
    
    def _handle_execute_dataflow(self, type_props: dict, pipe_idx: int, act_idx: int, act_name: str):
        """Handle ExecuteDataFlow activity"""
        dataflow = type_props.get('dataflow', {})
        if isinstance(dataflow, dict) and 'referenceName' in dataflow:
            self.reference_patterns['dataflow_refs'].append({
                'from': f"pipeline[{pipe_idx}].{act_name}",
                'to': dataflow.get('referenceName'),
                'type': 'ExecuteDataFlow'
            })
    
    def _handle_execute_pipeline(self, type_props: dict, pipe_idx: int, act_idx: int, act_name: str):
        """Handle ExecutePipeline activity"""
        pipeline = type_props.get('pipeline', {})
        if isinstance(pipeline, dict) and 'referenceName' in pipeline:
            self.reference_patterns['pipeline_refs'].append({
                'from': f"pipeline[{pipe_idx}].{act_name}",
                'to': pipeline.get('referenceName'),
                'type': 'ExecutePipeline'
            })
    
    def _handle_copy_activity(self, type_props: dict, pipe_idx: int, act_idx: int, act_name: str):
        """Handle Copy activity"""
        # Source dataset
        source = type_props.get('source', {})
        if isinstance(source, dict):
            dataset = source.get('dataset', {})
            if isinstance(dataset, dict) and 'referenceName' in dataset:
                self.reference_patterns['dataset_refs'].append({
                    'from': f"pipeline[{pipe_idx}].{act_name}.source",
                    'to': dataset.get('referenceName'),
                    'type': 'CopySource'
                })
        
        # Sink dataset
        sink = type_props.get('sink', {})
        if isinstance(sink, dict):
            dataset = sink.get('dataset', {})
            if isinstance(dataset, dict) and 'referenceName' in dataset:
                self.reference_patterns['dataset_refs'].append({
                    'from': f"pipeline[{pipe_idx}].{act_name}.sink",
                    'to': dataset.get('referenceName'),
                    'type': 'CopySink'
                })
    
    def _handle_lookup_activity(self, type_props: dict, pipe_idx: int, act_idx: int, act_name: str):
        """Handle Lookup activity"""
        dataset = type_props.get('dataset', {})
        if isinstance(dataset, dict) and 'referenceName' in dataset:
            self.reference_patterns['dataset_refs'].append({
                'from': f"pipeline[{pipe_idx}].{act_name}",
                'to': dataset.get('referenceName'),
                'type': 'Lookup'
            })
    
    def analyze_dataflow_pattern(self, resource: dict, idx: int):
        """Analyze dataflow patterns"""
        try:
            props = resource.get('properties', {})
            flow_type = props.get('type', 'MappingDataFlow')
            self.discoveries['dataflow_types'][flow_type] += 1
            
            type_props = props.get('typeProperties', {})
            
            # Analyze sources
            sources = type_props.get('sources', [])
            for src_idx, source in enumerate(sources):
                if isinstance(source, dict):
                    src_name = source.get('name', f'source_{src_idx}')
                    
                    # Dataset reference
                    if 'dataset' in source:
                        dataset_ref = source['dataset']
                        if isinstance(dataset_ref, dict) and 'referenceName' in dataset_ref:
                            self.reference_patterns['dataset_refs'].append({
                                'from': f"dataflow[{idx}].source.{src_name}",
                                'to': dataset_ref.get('referenceName'),
                                'type': 'DataFlowSource'
                            })
                    
                    # Linked service reference
                    if 'linkedService' in source:
                        ls_ref = source['linkedService']
                        if isinstance(ls_ref, dict) and 'referenceName' in ls_ref:
                            self.reference_patterns['linkedservice_refs'].append({
                                'from': f"dataflow[{idx}].source.{src_name}",
                                'to': ls_ref.get('referenceName'),
                                'type': 'DataFlowSourceLS'
                            })
                    
                    # Track source type
                    self.discoveries['source_sink_types'][f"source.{src_name}"] += 1
            
            # Analyze sinks
            sinks = type_props.get('sinks', [])
            for sink_idx, sink in enumerate(sinks):
                if isinstance(sink, dict):
                    sink_name = sink.get('name', f'sink_{sink_idx}')
                    
                    # Dataset reference
                    if 'dataset' in sink:
                        dataset_ref = sink['dataset']
                        if isinstance(dataset_ref, dict) and 'referenceName' in dataset_ref:
                            self.reference_patterns['dataset_refs'].append({
                                'from': f"dataflow[{idx}].sink.{sink_name}",
                                'to': dataset_ref.get('referenceName'),
                                'type': 'DataFlowSink'
                            })
                    
                    # Linked service reference
                    if 'linkedService' in sink:
                        ls_ref = sink['linkedService']
                        if isinstance(ls_ref, dict) and 'referenceName' in ls_ref:
                            self.reference_patterns['linkedservice_refs'].append({
                                'from': f"dataflow[{idx}].sink.{sink_name}",
                                'to': ls_ref.get('referenceName'),
                                'type': 'DataFlowSinkLS'
                            })
                    
                    # Track sink type
                    self.discoveries['source_sink_types'][f"sink.{sink_name}"] += 1
            
            # Analyze transformations
            transformations = type_props.get('transformations', [])
            for trans in transformations:
                if isinstance(trans, dict):
                    trans_name = trans.get('name', '')
                    trans_desc = trans.get('description', '')
                    self.discoveries['transformation_types'][trans_name] += 1
            
            # Analyze script
            script_lines = type_props.get('scriptLines', [])
            if script_lines:
                self.analyze_dataflow_script(script_lines)
                
        except Exception as e:
            self.log_error(f"analyze_dataflow[{idx}]", e)
    
    def analyze_dataflow_script(self, script_lines: list):
        """Analyze dataflow script for transformation patterns"""
        if not isinstance(script_lines, list):
            return
        
        # Join lines for analysis
        script_text = '\n'.join(str(line) for line in script_lines[:2000])
        
        # Transformation patterns (fixed regex)
        trans_patterns = {
            r'~>\s*(\w+)': 'transformation_operator',
            r'\b(\w+)\s*KATEX_INLINE_OPEN': 'function_call',
            r'source\s*KATEX_INLINE_OPEN': 'source_definition',
            r'sink\s*KATEX_INLINE_OPEN': 'sink_definition',
            r'select\s*KATEX_INLINE_OPEN': 'select_transformation',
            r'derive\s*KATEX_INLINE_OPEN': 'derive_transformation',
            r'aggregate\s*KATEX_INLINE_OPEN': 'aggregate_transformation',
            r'join\s*KATEX_INLINE_OPEN': 'join_transformation',
            r'filter\s*KATEX_INLINE_OPEN': 'filter_transformation',
            r'sort\s*KATEX_INLINE_OPEN': 'sort_transformation',
            r'split\s*KATEX_INLINE_OPEN': 'split_transformation',
            r'union\s*KATEX_INLINE_OPEN': 'union_transformation',
            r'window\s*KATEX_INLINE_OPEN': 'window_transformation',
            r'pivot\s*KATEX_INLINE_OPEN': 'pivot_transformation',
            r'unpivot\s*KATEX_INLINE_OPEN': 'unpivot_transformation',
            r'flatten\s*KATEX_INLINE_OPEN': 'flatten_transformation',
            r'parse\s*KATEX_INLINE_OPEN': 'parse_transformation',
            r'alter\s*KATEX_INLINE_OPEN': 'alter_transformation',
            r'exists\s*KATEX_INLINE_OPEN': 'exists_transformation',
            r'lookup\s*KATEX_INLINE_OPEN': 'lookup_transformation',
            r'surrogateKey\s*KATEX_INLINE_OPEN': 'surrogatekey_transformation'
        }
        
        for pattern, trans_type in trans_patterns.items():
            try:
                matches = re.findall(pattern, script_text, re.IGNORECASE)
                if matches:
                    self.discoveries['transformation_types'][trans_type] += len(matches)
            except Exception as e:
                self.log_error(f"regex_pattern:{pattern}", e)
    
    def analyze_dataset_pattern(self, resource: dict, idx: int):
        """Analyze dataset patterns"""
        try:
            props = resource.get('properties', {})
            ds_type = props.get('type', 'Unknown')
            self.discoveries['dataset_types'][ds_type] += 1
            
            # Track linked service reference
            ls_ref = props.get('linkedServiceName', {})
            if isinstance(ls_ref, dict) and 'referenceName' in ls_ref:
                dataset_name = self._extract_name(resource.get('name', f'dataset_{idx}'))
                self.reference_patterns['linkedservice_refs'].append({
                    'from': f"dataset.{dataset_name}",
                    'to': ls_ref.get('referenceName'),
                    'type': 'DatasetLinkedService'
                })
            
            # Analyze type properties for dataset structure
            type_props = props.get('typeProperties', {})
            self.discover_property_patterns(type_props, f"dataset.{ds_type}")
            
        except Exception as e:
            self.log_error(f"analyze_dataset[{idx}]", e)
    
    def analyze_linkedservice_pattern(self, resource: dict, idx: int):
        """Analyze linked service patterns"""
        try:
            props = resource.get('properties', {})
            ls_type = props.get('type', 'Unknown')
            self.discoveries['linkedservice_types'][ls_type] += 1
            
            # Analyze authentication
            type_props = props.get('typeProperties', {})
            auth_type = self.detect_authentication_pattern(type_props)
            if auth_type:
                self.discoveries['authentication_types'][auth_type] += 1
            
            # Track integration runtime reference
            ir_ref = props.get('connectVia', {})
            if isinstance(ir_ref, dict) and 'referenceName' in ir_ref:
                ls_name = self._extract_name(resource.get('name', f'linkedservice_{idx}'))
                self.reference_patterns['ir_refs'].append({
                    'from': f"linkedservice.{ls_name}",
                    'to': ir_ref.get('referenceName'),
                    'type': 'LinkedServiceIR'
                })
                
        except Exception as e:
            self.log_error(f"analyze_linkedservice[{idx}]", e)
    
    def analyze_trigger_pattern(self, resource: dict, idx: int):
        """Analyze trigger patterns"""
        try:
            props = resource.get('properties', {})
            trigger_type = props.get('type', 'Unknown')
            self.discoveries['trigger_types'][trigger_type] += 1
            
            # Track pipeline references
            pipelines = props.get('pipelines', [])
            trigger_name = self._extract_name(resource.get('name', f'trigger_{idx}'))
            
            for pipe_idx, pipeline in enumerate(pipelines):
                if isinstance(pipeline, dict):
                    pipe_ref = pipeline.get('pipelineReference', {})
                    if isinstance(pipe_ref, dict) and 'referenceName' in pipe_ref:
                        self.reference_patterns['pipeline_refs'].append({
                            'from': f"trigger.{trigger_name}",
                            'to': pipe_ref.get('referenceName'),
                            'type': 'TriggerPipeline'
                        })
                        
        except Exception as e:
            self.log_error(f"analyze_trigger[{idx}]", e)
    
    def analyze_integration_runtime_pattern(self, resource: dict, idx: int):
        """Analyze integration runtime patterns"""
        try:
            props = resource.get('properties', {})
            ir_type = props.get('type', 'Unknown')
            self.discoveries['integration_runtime_types'][ir_type] += 1
            
            # Analyze type-specific properties
            type_props = props.get('typeProperties', {})
            self.discover_property_patterns(type_props, f"integrationruntime.{ir_type}")
            
        except Exception as e:
            self.log_error(f"analyze_integration_runtime[{idx}]", e)
    
    def analyze_credential_pattern(self, resource: dict, idx: int):
        """Analyze credential patterns"""
        try:
            props = resource.get('properties', {})
            cred_type = props.get('type', 'Unknown')
            self.discoveries['credential_types'][cred_type] += 1
            
        except Exception as e:
            self.log_error(f"analyze_credential[{idx}]", e)
    
    def analyze_generic_pattern(self, resource: dict, category: str, idx: int):
        """Generic pattern analysis for unknown resource types"""
        try:
            structure = self.get_structure_template(resource)
            self.discoveries['nested_structures'][category][f"resource_{idx}"] = structure
        except Exception as e:
            self.log_error(f"analyze_generic[{category}][{idx}]", e)
    
    def discover_property_patterns(self, obj: dict, prefix: str, depth: int = 0, max_depth: int = 5):
        """Discover patterns in properties with depth limit"""
        if not isinstance(obj, dict) or depth > max_depth:
            return
        
        try:
            for key, value in obj.items():
                path = f"{prefix}.{key}"
                
                # Track property types
                if isinstance(value, dict):
                    # Check for reference patterns
                    if 'referenceName' in value and 'type' in value:
                        ref_type = value.get('type', '')
                        ref_name = value.get('referenceName', '')
                        
                        if 'Dataset' in ref_type:
                            self.reference_patterns['dataset_refs'].append({
                                'path': path,
                                'reference': ref_name,
                                'context': prefix
                            })
                        elif 'Pipeline' in ref_type:
                            self.reference_patterns['pipeline_refs'].append({
                                'path': path,
                                'reference': ref_name,
                                'context': prefix
                            })
                        elif 'LinkedService' in ref_type:
                            self.reference_patterns['linkedservice_refs'].append({
                                'path': path,
                                'reference': ref_name,
                                'context': prefix
                            })
                    
                    # Recurse
                    self.discover_property_patterns(value, path, depth + 1, max_depth)
                    
                elif isinstance(value, list) and value:
                    # Sample first few items
                    for i, item in enumerate(value[:3]):
                        if isinstance(item, dict):
                            self.discover_property_patterns(item, f"{path}[{i}]", depth + 1, max_depth)
                            
        except Exception as e:
            self.log_error(f"discover_property_patterns:{prefix}", e)
    
    def discover_nested_structures(self, obj: Any, path: str, category: str, depth: int = 0, max_depth: int = 4):
        """Discover nested structure patterns with depth limit"""
        if depth > max_depth:
            return
        
        try:
            if isinstance(obj, dict):
                structure = {}
                for key, value in obj.items():
                    if isinstance(value, dict):
                        structure[key] = 'object'
                        self.discover_nested_structures(value, f"{path}.{key}", category, depth + 1, max_depth)
                    elif isinstance(value, list):
                        structure[key] = 'array'
                        if value and isinstance(value[0], dict):
                            self.discover_nested_structures(value[0], f"{path}.{key}[0]", category, depth + 1, max_depth)
                    else:
                        structure[key] = type(value).__name__
                
                pattern = self.resource_patterns[category]
                pattern.properties[path] = structure
                
        except Exception as e:
            self.log_error(f"discover_nested_structures:{path}", e)
    
    def discover_references(self):
        """Discover all reference patterns in expressions"""
        try:
            self._find_references_recursive(self.data.get('resources', []))
        except Exception as e:
            self.log_error("discover_references", e)
    
    def _find_references_recursive(self, obj: Any, path: str = '', depth: int = 0, max_depth: int = 10):
        """Recursively find references with depth limit"""
        if depth > max_depth:
            return
        
        try:
            if isinstance(obj, str):
                self._extract_expression_references(obj, path)
                        
            elif isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{path}.{key}" if path else key
                    self._find_references_recursive(value, new_path, depth + 1, max_depth)
                    
            elif isinstance(obj, list):
                for idx, item in enumerate(obj[:200]):  # Limit to 200 items
                    new_path = f"{path}[{idx}]" if path else f"[{idx}]"
                    self._find_references_recursive(item, new_path, depth + 1, max_depth)
                    
        except Exception as e:
            self.log_error(f"find_references_recursive:{path}", e)
    
    def _extract_expression_references(self, expression: str, path: str):
        """Extract references from expression strings"""
        # Fixed regex patterns for ADF expressions
        ref_patterns = [
            (r"@pipelineKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.parameters\.(\w+)", 'parameter'),
            (r"@variablesKATEX_INLINE_OPEN'([^']+)'KATEX_INLINE_CLOSE", 'variable'),
            (r"@activityKATEX_INLINE_OPEN'([^']+)'KATEX_INLINE_CLOSE", 'activity'),
            (r"@datasetKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", 'dataset_param'),
            (r"@linkedServiceKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", 'linkedservice_param'),
            (r"@triggerKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", 'trigger_param'),
            (r"@itemKATEX_INLINE_OPENKATEX_INLINE_CLOSE\.(\w+)", 'item_property'),
            (r"@parametersKATEX_INLINE_OPEN'([^']+)'KATEX_INLINE_CLOSE", 'parameter_alt'),
            (r"concatKATEX_INLINE_OPENparametersKATEX_INLINE_OPEN'factoryName'KATEX_INLINE_CLOSE,\s*'/([^']+)'KATEX_INLINE_CLOSE", 'resource_name')
        ]
        
        for pattern, ref_type in ref_patterns:
            try:
                matches = re.findall(pattern, expression)
                for match in matches:
                    self.expression_patterns[ref_type].append({
                        'path': path,
                        'expression': expression[:300] if len(expression) > 300 else expression,
                        'reference': match
                    })
                    
                    # Also track in reference patterns
                    if ref_type == 'parameter':
                        self.reference_patterns['parameter_refs'].append({
                            'path': path,
                            'parameter': match
                        })
                    elif ref_type in ['variable', 'variable_alt']:
                        self.reference_patterns['variable_refs'].append({
                            'path': path,
                            'variable': match
                        })
                    elif ref_type == 'activity':
                        self.reference_patterns['activity_refs'].append({
                            'path': path,
                            'activity': match
                        })
            except Exception as e:
                self.log_error(f"extract_expression:{pattern}", e)
    
    def discover_expressions(self):
        """Discover expression function patterns"""
        try:
            self._find_expressions_recursive(self.data)
        except Exception as e:
            self.log_error("discover_expressions", e)
    
    def _find_expressions_recursive(self, obj: Any, depth: int = 0, max_depth: int = 10):
        """Recursively find expression functions with depth limit"""
        if depth > max_depth:
            return
        
        try:
            if isinstance(obj, str):
                # Common ADF expression functions
                functions = [
                    'concat', 'substring', 'replace', 'split', 'join',
                    'toLower', 'toUpper', 'trim', 'length', 'indexOf',
                    'contains', 'startsWith', 'endsWith', 'equals',
                    'greater', 'less', 'greaterOrEquals', 'lessOrEquals',
                    'if', 'and', 'or', 'not', 'coalesce',
                    'utcnow', 'addDays', 'addHours', 'addMinutes', 'formatDateTime',
                    'int', 'string', 'bool', 'float', 'array', 'json',
                    'pipeline', 'variables', 'activity', 'dataset',
                    'linkedService', 'trigger', 'item', 'items',
                    'guid', 'base64', 'encodeUriComponent', 'decodeUriComponent',
                    'first', 'last', 'take', 'skip', 'union', 'intersection',
                    'createArray', 'range', 'reverse', 'sort', 'min', 'max'
                ]
                
                obj_lower = obj.lower()
                for func in functions:
                    # Look for function calls
                    if f"@{func}(" in obj_lower or f"{func}(" in obj_lower:
                        self.discoveries['expression_functions'][func] += 1
                        
            elif isinstance(obj, dict):
                for value in obj.values():
                    self._find_expressions_recursive(value, depth + 1, max_depth)
            elif isinstance(obj, list):
                for item in obj[:200]:  # Limit items
                    self._find_expressions_recursive(item, depth + 1, max_depth)
                    
        except Exception as e:
            pass  # Silent fail for expression discovery
    
    def detect_authentication_pattern(self, type_props: dict) -> str:
        """Detect authentication pattern in linked service"""
        try:
            auth_indicators = {
                'authenticationType': lambda v: str(v),
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
                'clientId': 'OAuth/ServicePrincipal',
                'tenantId': 'ServicePrincipal',
                'azureCloudType': 'Azure'
            }
            
            for key, auth_type in auth_indicators.items():
                if key in type_props:
                    if callable(auth_type):
                        return auth_type(type_props[key])
                    return auth_type
            
            return 'Unknown'
        except:
            return 'Unknown'
    
    def build_dependency_graph(self):
        """Build complete dependency graph"""
        try:
            # Process all reference types
            for ref_type, ref_list in self.reference_patterns.items():
                for ref in ref_list:
                    if not isinstance(ref, dict):
                        continue
                    
                    from_node = ref.get('from', ref.get('path', ''))
                    to_node = ref.get('to', ref.get('reference', ''))
                    context = ref.get('type', ref_type)
                    
                    if from_node and to_node:
                        self.dependencies[from_node]['uses'][context].add(to_node)
                        self.dependencies[to_node]['referenced_by'].add(from_node)
                        
        except Exception as e:
            self.log_error("build_dependency_graph", e)
    
    def generate_parser_templates(self):
        """Generate parser templates for discovered patterns"""
        try:
            for res_type, pattern in self.resource_patterns.items():
                if pattern.count > 0:
                    template = {
                        'resource_type': res_type,
                        'count': pattern.count,
                        'sample_indices': list(pattern.samples),
                        'common_properties': self.get_common_properties(pattern.properties),
                        'parsing_paths': self.generate_parsing_paths(pattern.properties)
                    }
                    self.parser_templates[res_type] = template
        except Exception as e:
            self.log_error("generate_parser_templates", e)
    
    def get_common_properties(self, properties: dict) -> dict:
        """Extract common properties from patterns"""
        common = {}
        
        try:
            for path, structure in properties.items():
                if isinstance(structure, dict):
                    for key, value_type in structure.items():
                        if key not in common:
                            common[key] = {'types': set(), 'frequency': 0}
                        common[key]['types'].add(str(value_type))
                        common[key]['frequency'] += 1
            
            # Convert sets to lists for JSON serialization
            for key in common:
                common[key]['types'] = sorted(list(common[key]['types']))
            
        except Exception as e:
            self.log_error("get_common_properties", e)
        
        return common
    
    def generate_parsing_paths(self, properties: dict) -> list:
        """Generate parsing paths for properties"""
        paths = []
        
        try:
            unique_paths = set()
            
            for path, structure in properties.items():
                if isinstance(structure, dict):
                    for key, value_type in structure.items():
                        full_path = f"{path}.{key}" if path else key
                        
                        if full_path not in unique_paths:
                            unique_paths.add(full_path)
                            paths.append({
                                'path': full_path,
                                'type': str(value_type),
                                'extraction_method': self.suggest_extraction_method(str(value_type))
                            })
            
            # Sort and limit
            paths.sort(key=lambda x: x['path'])
            return paths[:100]  # Top 100 paths
            
        except Exception as e:
            self.log_error("generate_parsing_paths", e)
            return []
    
    def suggest_extraction_method(self, value_type: str) -> str:
        """Suggest extraction method based on type"""
        type_lower = value_type.lower()
        
        if 'dict' in type_lower or 'object' in type_lower:
            return 'recursive_extract'
        elif 'list' in type_lower or 'array' in type_lower:
            return 'iterate_extract'
        elif 'str' in type_lower or 'string' in type_lower:
            return 'string_extract'
        elif 'int' in type_lower or 'float' in type_lower or 'number' in type_lower:
            return 'numeric_extract'
        elif 'bool' in type_lower:
            return 'boolean_extract'
        else:
            return 'generic_extract'
    
    def get_structure_template(self, obj: Any, depth: int = 0, max_depth: int = 3) -> Any:
        """Get structure template with depth limit"""
        if depth > max_depth:
            return "..."
        
        try:
            if isinstance(obj, dict):
                template = {}
                # Limit to 30 keys
                for key, value in list(obj.items())[:30]:
                    template[key] = self.get_structure_template(value, depth + 1, max_depth)
                return template
            elif isinstance(obj, list):
                if obj:
                    return [self.get_structure_template(obj[0], depth + 1, max_depth)]
                return []
            else:
                return type(obj).__name__
        except:
            return "unknown"
    
    def _extract_name(self, name_value: Any) -> str:
        """Extract clean name from name field (may contain expressions)"""
        if not isinstance(name_value, str):
            return str(name_value)
        
        # Try to extract from concat expression
        match = re.search(r"concatKATEX_INLINE_OPEN[^,]+,\s*'([^']+)'KATEX_INLINE_CLOSE", name_value)
        if match:
            return match.group(1)
        
        # Try other patterns
        match = re.search(r"'([^']+)'", name_value)
        if match:
            return match.group(1)
        
        return name_value
    
    def print_discovery_summary(self):
        """Print comprehensive discovery summary"""
        print("\n" + "="*80)
        print("PATTERN DISCOVERY SUMMARY")
        print("="*80)
        
        # Resource Types
        if self.discoveries['resource_types']:
            print(f"\n📊 Resource Types Discovered: {len(self.discoveries['resource_types'])}")
            for res_type, count in self.discoveries['resource_types'].most_common(15):
                print(f"  • {res_type:30} : {count:4d}")
        
        # Activity Types
        if self.discoveries['activity_types']:
            print(f"\n⚡ Activity Types: {len(self.discoveries['activity_types'])}")
            for act_type, count in self.discoveries['activity_types'].most_common(15):
                print(f"  • {act_type:30} : {count:4d}")
        
        # Dataset Types
        if self.discoveries['dataset_types']:
            print(f"\n📦 Dataset Types: {len(self.discoveries['dataset_types'])}")
            for ds_type, count in self.discoveries['dataset_types'].most_common(10):
                print(f"  • {ds_type:30} : {count:4d}")
        
        # Linked Service Types
        if self.discoveries['linkedservice_types']:
            print(f"\n🔗 Linked Service Types: {len(self.discoveries['linkedservice_types'])}")
            for ls_type, count in self.discoveries['linkedservice_types'].most_common(10):
                print(f"  • {ls_type:30} : {count:4d}")
        
        # Authentication Types
        if self.discoveries['authentication_types']:
            print(f"\n🔐 Authentication Types:")
            for auth_type, count in self.discoveries['authentication_types'].most_common():
                print(f"  • {auth_type:30} : {count:4d}")
        
        # DataFlow Types
        if self.discoveries['dataflow_types']:
            print(f"\n🌊 DataFlow Types: {len(self.discoveries['dataflow_types'])}")
            for df_type, count in self.discoveries['dataflow_types'].items():
                print(f"  • {df_type:30} : {count:4d}")
        
        # Transformations
        if self.discoveries['transformation_types']:
            print(f"\n🔄 Transformation Types: {len(self.discoveries['transformation_types'])}")
            for trans_type, count in self.discoveries['transformation_types'].most_common(15):
                print(f"  • {trans_type:30} : {count:4d}")
        
        # Trigger Types
        if self.discoveries['trigger_types']:
            print(f"\n⏰ Trigger Types: {len(self.discoveries['trigger_types'])}")
            for trig_type, count in self.discoveries['trigger_types'].items():
                print(f"  • {trig_type:30} : {count:4d}")
        
        # References
        print(f"\n🔗 References Found:")
        total_refs = 0
        for ref_type, refs in self.reference_patterns.items():
            if refs:
                count = len(refs)
                total_refs += count
                print(f"  • {ref_type:30} : {count:4d}")
        print(f"  {'TOTAL':30} : {total_refs:4d}")
        
        # Expression Functions
        if self.discoveries['expression_functions']:
            print(f"\n📝 Expression Functions Used: {len(self.discoveries['expression_functions'])}")
            for func, count in self.discoveries['expression_functions'].most_common(15):
                print(f"  • {func:30} : {count:4d}")
        
        # Parser Templates
        print(f"\n🔧 Parser Templates Generated: {len(self.parser_templates)}")
        
        # Errors
        if self.errors:
            print(f"\n⚠️  Errors Encountered: {len(self.errors)}")
            print("  (See error log in exported files)")
    
    def generate_enhanced_parser_code(self) -> str:
        """Generate enhanced parser code based on discoveries"""
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        code = f'''"""
Auto-generated Parser Enhancements
Generated: {timestamp}
Source: {self.json_path}

Add these methods to your UltimateADFParser class
"""

from typing import Dict, List, Any, Optional


class EnhancedResourceParser:
    """Enhanced parser with auto-discovered patterns"""
    
    def __init__(self):
        self.results = {{}}
        self.errors = []
    
    def parse_all_resources(self, resources: List[dict]):
        """Parse all discovered resource types"""
        for idx, resource in enumerate(resources):
            if not isinstance(resource, dict):
                continue
            
            res_type = resource.get('type', '')
            
'''
        
        # Add conditional parsing for each discovered type
        for res_type in sorted(self.discoveries['resource_types'].keys()):
            safe_name = res_type.replace('-', '_').replace('.', '_').lower()
            code += f'''            if '{res_type.lower()}' in res_type.lower():
                self.parse_{safe_name}(resource, idx)
'''
        
        code += "\n"
        
        # Generate parsing methods
        for res_type, template in sorted(self.parser_templates.items()):
            safe_name = res_type.replace('-', '_').replace('.', '_').lower()
            
            code += f'''
    def parse_{safe_name}(self, resource: dict, idx: int):
        """
        Parse {res_type} resource
        Found: {template['count']} instances
        """
        try:
            name = self._extract_name(resource.get('name', f'{safe_name}_{{idx}}'))
            props = resource.get('properties', {{}})
            
            record = {{
                'ResourceType': '{res_type}',
                'Name': name,
                'Index': idx,
'''
            
            # Add common properties
            common_props = template.get('common_properties', {})
            for prop_name in sorted(list(common_props.keys())[:15]):
                code += f'''                '{prop_name}': props.get('{prop_name}', ''),
'''
            
            code += '''            }
            
            # Store result
            if '{res_type}' not in self.results:
                self.results['{res_type}'] = []
            self.results['{res_type}'].append(record)
            
        except Exception as e:
            self._log_error('{res_type}', idx, e)
'''
        
        # Add helper methods
        code += '''
    def _extract_name(self, name_value: Any) -> str:
        """Extract clean name from expression or string"""
        import re
        if not isinstance(name_value, str):
            return str(name_value)
        
        # Extract from concat expression
        match = re.search(r"concat\KATEX_INLINE_OPEN[^,]+,\\s*'([^']+)'\KATEX_INLINE_CLOSE", name_value)
        if match:
            return match.group(1)
        
        # Extract from quotes
        match = re.search(r"'([^']+)'", name_value)
        if match:
            return match.group(1)
        
        return name_value
    
    def _log_error(self, res_type: str, idx: int, error: Exception):
        """Log parsing error"""
        self.errors.append({
            'resource_type': res_type,
            'index': idx,
            'error': str(error)
        })
        print(f"❌ Error parsing {res_type}[{idx}]: {error}")


# Usage Example:
# parser = EnhancedResourceParser()
# parser.parse_all_resources(arm_template['resources'])
# results = parser.results
'''
        
        return code
    
    def export_discoveries(self, output_dir: Optional[str] = None) -> Path:
        """Export all discoveries to files"""
        
        if output_dir is None:
            output_dir = Path('arm_discoveries')
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        self.log(f"\n📝 Exporting discoveries to: {output_dir}")
        
        # 1. Complete discovery JSON
        self._export_discovery_json(output_dir, timestamp)
        
        # 2. Excel report
        self._export_excel_report(output_dir, timestamp)
        
        # 3. Enhanced parser code
        self._export_parser_code(output_dir, timestamp)
        
        # 4. Dependency graph
        self._export_dependency_graph(output_dir, timestamp)
        
        # 5. Reference map
        self._export_reference_map(output_dir, timestamp)
        
        # 6. Error log
        if self.errors:
            self._export_error_log(output_dir, timestamp)
        
        return output_dir
    
    def _export_discovery_json(self, output_dir: Path, timestamp: str):
        """Export complete discovery as JSON"""
        try:
            discovery_file = output_dir / f'discoveries_{timestamp}.json'
            
            # Convert Counter objects and sets to serializable format
            def convert_for_json(obj):
                if isinstance(obj, Counter):
                    return dict(obj)
                elif isinstance(obj, set):
                    return sorted(list(obj))
                elif isinstance(obj, defaultdict):
                    return dict(obj)
                return obj
            
            discovery_data = {
                'metadata': {
                    'source_file': str(self.json_path),
                    'analyzed_at': datetime.now().isoformat(),
                    'total_resources': sum(self.discoveries['resource_types'].values()),
                    'total_errors': len(self.errors)
                },
                'discoveries': {
                    key: convert_for_json(value) 
                    for key, value in self.discoveries.items()
                },
                'parser_templates': self.parser_templates,
                'reference_summary': {
                    key: len(refs) 
                    for key, refs in self.reference_patterns.items()
                },
                'statistics': {
                    'unique_resource_types': len(self.discoveries['resource_types']),
                    'unique_activity_types': len(self.discoveries['activity_types']),
                    'unique_dataset_types': len(self.discoveries['dataset_types']),
                    'unique_linkedservice_types': len(self.discoveries['linkedservice_types']),
                    'total_references': sum(len(refs) for refs in self.reference_patterns.values()),
                    'expression_functions_used': len(self.discoveries['expression_functions'])
                }
            }
            
            with open(discovery_file, 'w', encoding='utf-8') as f:
                json.dump(discovery_data, f, indent=2, default=str)
            
            self.log(f"✅ Discovery JSON: {discovery_file}", "SUCCESS")
            
        except Exception as e:
            self.log_error("export_discovery_json", e)
    
    def _export_excel_report(self, output_dir: Path, timestamp: str):
        """Export comprehensive Excel report"""
        try:
            excel_file = output_dir / f'discoveries_{timestamp}.xlsx'
            
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                # Summary sheet
                summary_data = []
                summary_data.append({'Metric': 'Source File', 'Value': str(self.json_path)})
                summary_data.append({'Metric': 'Analysis Date', 'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                summary_data.append({'Metric': 'Total Resources', 'Value': sum(self.discoveries['resource_types'].values())})
                summary_data.append({'Metric': 'Resource Types', 'Value': len(self.discoveries['resource_types'])})
                summary_data.append({'Metric': 'Total References', 'Value': sum(len(refs) for refs in self.reference_patterns.values())})
                summary_data.append({'Metric': 'Errors', 'Value': len(self.errors)})
                
                df = pd.DataFrame(summary_data)
                df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Resource types
                if self.discoveries['resource_types']:
                    df = pd.DataFrame([
                        {'Resource Type': k, 'Count': v}
                        for k, v in self.discoveries['resource_types'].most_common()
                    ])
                    df.to_excel(writer, sheet_name='Resource Types', index=False)
                
                # Activity types
                if self.discoveries['activity_types']:
                    df = pd.DataFrame([
                        {'Activity Type': k, 'Count': v}
                        for k, v in self.discoveries['activity_types'].most_common()
                    ])
                    df.to_excel(writer, sheet_name='Activity Types', index=False)
                
                # Dataset types
                if self.discoveries['dataset_types']:
                    df = pd.DataFrame([
                        {'Dataset Type': k, 'Count': v}
                        for k, v in self.discoveries['dataset_types'].most_common()
                    ])
                    df.to_excel(writer, sheet_name='Dataset Types', index=False)
                
                # Linked Service types
                if self.discoveries['linkedservice_types']:
                    df = pd.DataFrame([
                        {'LinkedService Type': k, 'Count': v}
                        for k, v in self.discoveries['linkedservice_types'].most_common()
                    ])
                    df.to_excel(writer, sheet_name='LinkedService Types', index=False)
                
                # Authentication types
                if self.discoveries['authentication_types']:
                    df = pd.DataFrame([
                        {'Authentication Type': k, 'Count': v}
                        for k, v in self.discoveries['authentication_types'].most_common()
                    ])
                    df.to_excel(writer, sheet_name='Authentication', index=False)
                
                # DataFlow info
                df_rows = []
                for k, v in self.discoveries['dataflow_types'].items():
                    df_rows.append({'Category': 'DataFlow Type', 'Name': k, 'Count': v})
                for k, v in self.discoveries['transformation_types'].most_common(50):
                    df_rows.append({'Category': 'Transformation', 'Name': k, 'Count': v})
                
                if df_rows:
                    df = pd.DataFrame(df_rows)
                    df.to_excel(writer, sheet_name='DataFlow Patterns', index=False)
                
                # References summary
                ref_summary = []
                for ref_type, refs in self.reference_patterns.items():
                    ref_summary.append({
                        'Reference Type': ref_type,
                        'Count': len(refs)
                    })
                if ref_summary:
                    df = pd.DataFrame(ref_summary)
                    df.to_excel(writer, sheet_name='Reference Summary', index=False)
                
                # Detailed references (sample)
                ref_details = []
                for ref_type, refs in self.reference_patterns.items():
                    for ref in refs[:500]:  # First 500 of each type
                        if isinstance(ref, dict):
                            ref_details.append({
                                'Type': ref_type,
                                'From': str(ref.get('from', ref.get('path', ''))),
                                'To': str(ref.get('to', ref.get('reference', ''))),
                                'Context': str(ref.get('type', ref.get('context', '')))
                            })
                
                if ref_details:
                    df = pd.DataFrame(ref_details)
                    df.to_excel(writer, sheet_name='References Detail', index=False)
                
                # Expression functions
                if self.discoveries['expression_functions']:
                    df = pd.DataFrame([
                        {'Function': k, 'Usage Count': v}
                        for k, v in self.discoveries['expression_functions'].most_common()
                    ])
                    df.to_excel(writer, sheet_name='Expression Functions', index=False)
                
                # Parser templates summary
                template_rows = []
                for res_type, template in self.parser_templates.items():
                    common_props = template.get('common_properties', {})
                    template_rows.append({
                        'Resource Type': res_type,
                        'Instance Count': template['count'],
                        'Properties Found': len(common_props),
                        'Sample Properties': ', '.join(list(common_props.keys())[:10])
                    })
                
                if template_rows:
                    df = pd.DataFrame(template_rows)
                    df.to_excel(writer, sheet_name='Parser Templates', index=False)
            
            self.log(f"✅ Excel Report: {excel_file}", "SUCCESS")
            
        except Exception as e:
            self.log_error("export_excel_report", e)
    
    def _export_parser_code(self, output_dir: Path, timestamp: str):
        """Export generated parser code"""
        try:
            parser_file = output_dir / f'enhanced_parser_{timestamp}.py'
            
            code = self.generate_enhanced_parser_code()
            
            with open(parser_file, 'w', encoding='utf-8') as f:
                f.write(code)
            
            self.log(f"✅ Parser Code: {parser_file}", "SUCCESS")
            
        except Exception as e:
            self.log_error("export_parser_code", e)
    
    def _export_dependency_graph(self, output_dir: Path, timestamp: str):
        """Export dependency graph"""
        try:
            dep_file = output_dir / f'dependencies_{timestamp}.json'
            
            # Convert dependencies to serializable format
            dep_data = {
                'dependencies': {}
            }
            
            for key, value in self.dependencies.items():
                if value['uses'] or value['referenced_by']:
                    dep_data['dependencies'][str(key)] = {
                        'uses': {
                            str(k): sorted(list(v)) 
                            for k, v in value['uses'].items()
                        },
                        'referenced_by': sorted(list(value['referenced_by']))
                    }
            
            with open(dep_file, 'w', encoding='utf-8') as f:
                json.dump(dep_data, f, indent=2)
            
            self.log(f"✅ Dependency Graph: {dep_file}", "SUCCESS")
            
        except Exception as e:
            self.log_error("export_dependency_graph", e)
    
    def _export_reference_map(self, output_dir: Path, timestamp: str):
        """Export complete reference map"""
        try:
            ref_file = output_dir / f'references_{timestamp}.json'
            
            ref_data = {
                'reference_patterns': {},
                'statistics': {}
            }
            
            for ref_type, refs in self.reference_patterns.items():
                ref_data['reference_patterns'][ref_type] = refs[:1000]  # Limit to 1000 each
                ref_data['statistics'][ref_type] = len(refs)
            
            with open(ref_file, 'w', encoding='utf-8') as f:
                json.dump(ref_data, f, indent=2)
            
            self.log(f"✅ Reference Map: {ref_file}", "SUCCESS")
            
        except Exception as e:
            self.log_error("export_reference_map", e)
    
    def _export_error_log(self, output_dir: Path, timestamp: str):
        """Export error log"""
        try:
            error_file = output_dir / f'errors_{timestamp}.json'
            
            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump({'errors': self.errors}, f, indent=2)
            
            self.log(f"⚠️  Error Log: {error_file}", "WARNING")
            
        except Exception as e:
            self.log_error("export_error_log", e)


def main():
    """Main execution"""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║           ARM Template Pattern Discovery Engine v3.0                         ║
║           Production-Ready Azure Data Factory Analysis Tool                  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    if len(sys.argv) < 2:
        print("""
Usage: python arm_pattern_discovery.py <arm_template.json> [output_dir]

Arguments:
  arm_template.json  : Path to your ARM template file
  output_dir         : (Optional) Output directory for results

This tool will:
  ✅ Discover ALL patterns in your ARM template
  ✅ Generate parser templates for each resource type  
  ✅ Build dependency graphs
  ✅ Create reference maps
  ✅ Export comprehensive Excel reports
  ✅ Generate enhanced parser code

Example:
  python arm_pattern_discovery.py factory_arm_template.json
  python arm_pattern_discovery.py factory_arm_template.json ./analysis_results
        """)
        sys.exit(1)
    
    json_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Run discovery
    discoverer = ARMTemplatePatternDiscovery(json_path, verbose=True)
    
    if not discoverer.load_json():
        print("\n❌ Failed to load JSON file. Exiting.")
        sys.exit(1)
    
    # Discover patterns
    discoverer.discover_patterns()
    
    # Export results
    print("\n" + "="*80)
    result_dir = discoverer.export_discoveries(output_dir)
    
    print("\n" + "="*80)
    print("✅ PATTERN DISCOVERY COMPLETE!")
    print("="*80)
    print(f"\n📊 Results exported to: {result_dir.absolute()}")
    print("\n📋 Generated Files:")
    print("   • discoveries_TIMESTAMP.json  - Complete pattern analysis")
    print("   • discoveries_TIMESTAMP.xlsx  - Detailed Excel report")
    print("   • enhanced_parser_TIMESTAMP.py - Auto-generated parser code")
    print("   • dependencies_TIMESTAMP.json - Resource dependency graph")
    print("   • references_TIMESTAMP.json   - Complete reference map")
    
    if discoverer.errors:
        print(f"\n⚠️  {len(discoverer.errors)} errors encountered (see errors_TIMESTAMP.json)")
    
    print("\n💡 Next Steps:")
    print("   1. Review discoveries.xlsx for comprehensive analysis")
    print("   2. Check discoveries.json for detailed patterns")
    print("   3. Use enhanced_parser.py code in your project")
    print("   4. Explore dependencies.json for resource relationships")
    print("   5. Review references.json for all cross-references")
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()