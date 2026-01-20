"""
Auto-generated Parser Enhancements
Generated: 2025-10-17 16:27:38
Source: test.json

Add these methods to your UltimateADFParser class
"""

from typing import Dict, List, Any, Optional


class EnhancedResourceParser:
    """Enhanced parser with auto-discovered patterns"""
    
    def __init__(self):
        self.results = {}
        self.errors = []
    
    def parse_all_resources(self, resources: List[dict]):
        """Parse all discovered resource types"""
        for idx, resource in enumerate(resources):
            if not isinstance(resource, dict):
                continue
            
            res_type = resource.get('type', '')
            
            if 'dataflows' in res_type.lower():
                self.parse_dataflows(resource, idx)
            if 'datasets' in res_type.lower():
                self.parse_datasets(resource, idx)
            if 'integrationruntimes' in res_type.lower():
                self.parse_integrationruntimes(resource, idx)
            if 'linkedservices' in res_type.lower():
                self.parse_linkedservices(resource, idx)
            if 'managedvirtualnetworks' in res_type.lower():
                self.parse_managedvirtualnetworks(resource, idx)
            if 'pipelines' in res_type.lower():
                self.parse_pipelines(resource, idx)
            if 'triggers' in res_type.lower():
                self.parse_triggers(resource, idx)


    def parse_dataflows(self, resource: dict, idx: int):
        """
        Parse dataflows resource
        Found: 145 instances
        """
        try:
            name = self._extract_name(resource.get('name', f'dataflows_{idx}'))
            props = resource.get('properties', {})
            
            record = {
                'ResourceType': 'dataflows',
                'Name': name,
                'Index': idx,
                'apiVersion': props.get('apiVersion', ''),
                'dataset': props.get('dataset', ''),
                'dependsOn': props.get('dependsOn', ''),
                'description': props.get('description', ''),
                'folder': props.get('folder', ''),
                'linkedService': props.get('linkedService', ''),
                'name': props.get('name', ''),
                'properties': props.get('properties', ''),
                'referenceName': props.get('referenceName', ''),
                'scriptLines': props.get('scriptLines', ''),
                'sinks': props.get('sinks', ''),
                'sources': props.get('sources', ''),
                'transformations': props.get('transformations', ''),
                'type': props.get('type', ''),
                'typeProperties': props.get('typeProperties', ''),
            }
            
            # Store result
            if '{res_type}' not in self.results:
                self.results['{res_type}'] = []
            self.results['{res_type}'].append(record)
            
        except Exception as e:
            self._log_error('{res_type}', idx, e)

    def parse_datasets(self, resource: dict, idx: int):
        """
        Parse datasets resource
        Found: 65 instances
        """
        try:
            name = self._extract_name(resource.get('name', f'datasets_{idx}'))
            props = resource.get('properties', {})
            
            record = {
                'ResourceType': 'datasets',
                'Name': name,
                'Index': idx,
                'annotations': props.get('annotations', ''),
                'apiVersion': props.get('apiVersion', ''),
                'dependsOn': props.get('dependsOn', ''),
                'linkedServiceName': props.get('linkedServiceName', ''),
                'name': props.get('name', ''),
                'p_schema': props.get('p_schema', ''),
                'p_table': props.get('p_table', ''),
                'parameters': props.get('parameters', ''),
                'properties': props.get('properties', ''),
                'referenceName': props.get('referenceName', ''),
                'schema': props.get('schema', ''),
                'table': props.get('table', ''),
                'type': props.get('type', ''),
                'typeProperties': props.get('typeProperties', ''),
                'value': props.get('value', ''),
            }
            
            # Store result
            if '{res_type}' not in self.results:
                self.results['{res_type}'] = []
            self.results['{res_type}'].append(record)
            
        except Exception as e:
            self._log_error('{res_type}', idx, e)

    def parse_integrationruntimes(self, resource: dict, idx: int):
        """
        Parse integrationRuntimes resource
        Found: 4 instances
        """
        try:
            name = self._extract_name(resource.get('name', f'integrationruntimes_{idx}'))
            props = resource.get('properties', {})
            
            record = {
                'ResourceType': 'integrationRuntimes',
                'Name': name,
                'Index': idx,
                'apiVersion': props.get('apiVersion', ''),
                'authorizationType': props.get('authorizationType', ''),
                'cleanup': props.get('cleanup', ''),
                'computeType': props.get('computeType', ''),
                'coreCount': props.get('coreCount', ''),
                'customProperties': props.get('customProperties', ''),
                'dependsOn': props.get('dependsOn', ''),
                'description': props.get('description', ''),
                'linkedInfo': props.get('linkedInfo', ''),
                'name': props.get('name', ''),
                'properties': props.get('properties', ''),
                'resourceId': props.get('resourceId', ''),
                'timeToLive': props.get('timeToLive', ''),
                'type': props.get('type', ''),
                'typeProperties': props.get('typeProperties', ''),
            }
            
            # Store result
            if '{res_type}' not in self.results:
                self.results['{res_type}'] = []
            self.results['{res_type}'].append(record)
            
        except Exception as e:
            self._log_error('{res_type}', idx, e)

    def parse_linkedservices(self, resource: dict, idx: int):
        """
        Parse linkedServices resource
        Found: 44 instances
        """
        try:
            name = self._extract_name(resource.get('name', f'linkedservices_{idx}'))
            props = resource.get('properties', {})
            
            record = {
                'ResourceType': 'linkedServices',
                'Name': name,
                'Index': idx,
                'annotations': props.get('annotations', ''),
                'apiVersion': props.get('apiVersion', ''),
                'baseUrl': props.get('baseUrl', ''),
                'connectVia': props.get('connectVia', ''),
                'connectionString': props.get('connectionString', ''),
                'dependsOn': props.get('dependsOn', ''),
                'host': props.get('host', ''),
                'name': props.get('name', ''),
                'properties': props.get('properties', ''),
                'referenceName': props.get('referenceName', ''),
                'sasUri': props.get('sasUri', ''),
                'secretName': props.get('secretName', ''),
                'store': props.get('store', ''),
                'type': props.get('type', ''),
                'typeProperties': props.get('typeProperties', ''),
            }
            
            # Store result
            if '{res_type}' not in self.results:
                self.results['{res_type}'] = []
            self.results['{res_type}'].append(record)
            
        except Exception as e:
            self._log_error('{res_type}', idx, e)

    def parse_managedvirtualnetworks(self, resource: dict, idx: int):
        """
        Parse managedVirtualNetworks resource
        Found: 1 instances
        """
        try:
            name = self._extract_name(resource.get('name', f'managedvirtualnetworks_{idx}'))
            props = resource.get('properties', {})
            
            record = {
                'ResourceType': 'managedVirtualNetworks',
                'Name': name,
                'Index': idx,
                'apiVersion': props.get('apiVersion', ''),
                'dependsOn': props.get('dependsOn', ''),
                'name': props.get('name', ''),
                'properties': props.get('properties', ''),
                'type': props.get('type', ''),
            }
            
            # Store result
            if '{res_type}' not in self.results:
                self.results['{res_type}'] = []
            self.results['{res_type}'].append(record)
            
        except Exception as e:
            self._log_error('{res_type}', idx, e)

    def parse_pipelines(self, resource: dict, idx: int):
        """
        Parse pipelines resource
        Found: 348 instances
        """
        try:
            name = self._extract_name(resource.get('name', f'pipelines_{idx}'))
            props = resource.get('properties', {})
            
            record = {
                'ResourceType': 'pipelines',
                'Name': name,
                'Index': idx,
                'activity': props.get('activity', ''),
                'body': props.get('body', ''),
                'dependencyConditions': props.get('dependencyConditions', ''),
                'dependsOn': props.get('dependsOn', ''),
                'headers': props.get('headers', ''),
                'method': props.get('method', ''),
                'name': props.get('name', ''),
                'retry': props.get('retry', ''),
                'retryIntervalInSeconds': props.get('retryIntervalInSeconds', ''),
                'secureInput': props.get('secureInput', ''),
                'secureOutput': props.get('secureOutput', ''),
                'timeout': props.get('timeout', ''),
                'type': props.get('type', ''),
                'url': props.get('url', ''),
                'value': props.get('value', ''),
            }
            
            # Store result
            if '{res_type}' not in self.results:
                self.results['{res_type}'] = []
            self.results['{res_type}'].append(record)
            
        except Exception as e:
            self._log_error('{res_type}', idx, e)

    def parse_triggers(self, resource: dict, idx: int):
        """
        Parse triggers resource
        Found: 80 instances
        """
        try:
            name = self._extract_name(resource.get('name', f'triggers_{idx}'))
            props = resource.get('properties', {})
            
            record = {
                'ResourceType': 'triggers',
                'Name': name,
                'Index': idx,
                'annotations': props.get('annotations', ''),
                'frequency': props.get('frequency', ''),
                'hours': props.get('hours', ''),
                'interval': props.get('interval', ''),
                'minutes': props.get('minutes', ''),
                'parameters': props.get('parameters', ''),
                'pipelineReference': props.get('pipelineReference', ''),
                'pipelines': props.get('pipelines', ''),
                'recurrence': props.get('recurrence', ''),
                'referenceName': props.get('referenceName', ''),
                'runtimeState': props.get('runtimeState', ''),
                'schedule': props.get('schedule', ''),
                'startTime': props.get('startTime', ''),
                'timeZone': props.get('timeZone', ''),
                'type': props.get('type', ''),
            }
            
            # Store result
            if '{res_type}' not in self.results:
                self.results['{res_type}'] = []
            self.results['{res_type}'].append(record)
            
        except Exception as e:
            self._log_error('{res_type}', idx, e)

    def _extract_name(self, name_value: Any) -> str:
        """Extract clean name from expression or string"""
        import re
        if not isinstance(name_value, str):
            return str(name_value)
        
        # Extract from concat expression
        match = re.search(r"concat\KATEX_INLINE_OPEN[^,]+,\s*'([^']+)'\KATEX_INLINE_CLOSE", name_value)
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
