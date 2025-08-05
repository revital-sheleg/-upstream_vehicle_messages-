#!/usr/bin/env python3
"""
Updated Configuration Manager for Table-Based Data Lake
Handles the new table-centric configuration structure
"""

import json
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass


@dataclass
class TableSchema:
    """Schema definition for a table column"""
    name: str
    type: str
    nullable: bool = True


@dataclass
class LayerConfig:
    """Configuration for a data lake layer within a table"""
    name: str
    description: str
    format: str
    catalog: Optional[str]
    partitioning: Dict[str, Any]
    cleaning: Dict[str, Any]
    schema: Dict[str, Any]
    path: str


@dataclass
class TableConfig:
    """Configuration for a complete table"""
    table_name: str
    api_source: Optional[Dict[str, str]]
    schema: List[TableSchema]
    paths: Dict[str, str]
    bronze: LayerConfig
    silver: LayerConfig
    gold: LayerConfig
    reporting: LayerConfig


@dataclass
class CleaningRule:
    """Configuration for a data cleaning rule"""
    type: str
    columns: List[str]
    description: str
    method: Optional[str] = None
    regex: Optional[str] = None
    options: Optional[List[str]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None


@dataclass
class GoldTableConfig:
    """Configuration for a gold layer aggregated table"""
    name: str
    description: str
    source_layer: str
    output_format: str
    aggregation_type: str
    group_by: List[str]
    aggregations: Dict[str, str]


@dataclass
class ReportConfig:
    """Configuration for a report"""
    name: str
    description: str
    source_layer: str
    source_table: Optional[str]
    output_format: str
    report_type: str
    columns: List[str]
    group_by: Optional[List[str]] = None
    top_n: Optional[int] = None
    order_by: Optional[str] = None


class ConfigManager:
    """Manages table-based data lake configuration"""
    
    def __init__(self, config_path: str = "config/config_new.json"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            print(f"✅ Loaded configuration from {self.config_path}")
            return config
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def _validate_config(self):
        """Validate configuration structure and values"""
        required_sections = ['data_lake', 'tables']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate tables
        if not isinstance(self.config['tables'], list) or len(self.config['tables']) == 0:
            raise ValueError("Tables section must be a non-empty array")
        
        print("✅ Configuration validation passed")
    
    def get_data_lake_config(self) -> Dict[str, Any]:
        """Get data lake general configuration"""
        return self.config['data_lake']
    
    def get_table_configs(self) -> List[TableConfig]:
        """Get all table configurations"""
        tables = []
        
        for table_data in self.config['tables']:
            # Parse schema
            schema = []
            for col_data in table_data.get('schema', []):
                schema.append(TableSchema(
                    name=col_data['name'],
                    type=col_data['type'],
                    nullable=col_data.get('nullable', True)
                ))
            
            # Parse layer configurations
            bronze_config = self._parse_layer_config(table_data.get('bronze', {}), 'bronze')
            silver_config = self._parse_layer_config(table_data.get('silver', {}), 'silver')
            gold_config = self._parse_layer_config(table_data.get('gold', {}), 'gold')
            reporting_config = self._parse_layer_config(table_data.get('reporting', {}), 'reporting')
            
            table_config = TableConfig(
                table_name=table_data['table_name'],
                api_source=table_data.get('api_source'),
                schema=schema,
                paths=table_data.get('paths', {}),
                bronze=bronze_config,
                silver=silver_config,
                gold=gold_config,
                reporting=reporting_config
            )
            
            tables.append(table_config)
        
        return tables
    
    def _parse_layer_config(self, layer_data: Dict[str, Any], layer_name: str) -> LayerConfig:
        """Parse layer configuration from table data"""
        return LayerConfig(
            name=layer_name,
            description=layer_data.get('description', f'{layer_name.title()} layer'),
            format=layer_data.get('format', self.config['data_lake']['default_format']),
            catalog=layer_data.get('catalog'),
            partitioning=layer_data.get('partitioning', {}),
            cleaning=layer_data.get('cleaning', {}),
            schema=layer_data.get('schema', {}),
            path=layer_data.get('path', f'./{layer_name}')
        )
    
    def get_table_config(self, table_name: str) -> Optional[TableConfig]:
        """Get configuration for a specific table"""
        tables = self.get_table_configs()
        for table in tables:
            if table.table_name == table_name:
                return table
        return None
    
    def get_layer_config(self, table_name: str, layer_name: str) -> Optional[LayerConfig]:
        """Get configuration for a specific layer of a table"""
        table_config = self.get_table_config(table_name)
        if not table_config:
            return None
        
        if layer_name == 'bronze':
            return table_config.bronze
        elif layer_name == 'silver':
            return table_config.silver
        elif layer_name == 'gold':
            return table_config.gold
        elif layer_name == 'reporting':
            return table_config.reporting
        else:
            return None
    
    def get_cleaning_rules(self, table_name: str, layer_name: str) -> List[CleaningRule]:
        """Get cleaning rules for a specific table layer"""
        layer_config = self.get_layer_config(table_name, layer_name)
        
        if not layer_config or not layer_config.cleaning.get('enabled', False):
            return []
        
        rules = []
        for rule_data in layer_config.cleaning.get('rules', []):
            # Get method details from cleaning_methods section
            method_config = self.config.get('cleaning_methods', {}).get(rule_data['type'], {})
            
            rule = CleaningRule(
                type=rule_data['type'],
                columns=rule_data['columns'],
                description=rule_data['description'],
                method=rule_data.get('method') or method_config.get('method'),
                regex=method_config.get('regex'),
                options=method_config.get('options'),
                min_value=method_config.get('min_value'),
                max_value=method_config.get('max_value')
            )
            rules.append(rule)
        
        return rules
    
    def get_format_config(self, format_name: str) -> Dict[str, Any]:
        """Get configuration for a specific format"""
        if format_name not in self.config.get('formats', {}):
            raise ValueError(f"Format configuration not found: {format_name}")
        
        return self.config['formats'][format_name]
    
    def get_gold_table_configs(self) -> List[GoldTableConfig]:
        """Get all gold table configurations"""
        gold_tables = []
        
        for table_name, table_data in self.config.get('gold_tables', {}).items():
            gold_table = GoldTableConfig(
                name=table_name,
                description=table_data['description'],
                source_layer=table_data['source_layer'],
                output_format=table_data['output_format'],
                aggregation_type=table_data['aggregation_type'],
                group_by=table_data['group_by'],
                aggregations=table_data['aggregations']
            )
            gold_tables.append(gold_table)
        
        return gold_tables
    
    def get_report_configs(self) -> List[ReportConfig]:
        """Get all report configurations"""
        reports = []
        
        for report_name, report_data in self.config.get('reports', {}).items():
            report = ReportConfig(
                name=report_name,
                description=report_data['description'],
                source_layer=report_data['source_layer'],
                source_table=report_data.get('source_table'),
                output_format=report_data['output_format'],
                report_type=report_data['report_type'],
                columns=report_data['columns'],
                group_by=report_data.get('group_by'),
                top_n=report_data.get('top_n'),
                order_by=report_data.get('order_by')
            )
            reports.append(report)
        
        return reports
    
    def get_security_config(self) -> Dict[str, Any]:
        """Get security configuration"""
        return self.config.get('security', {})
    
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance configuration"""
        return self.config.get('performance', {})
    
    def get_api_url(self, table_name: str) -> Optional[str]:
        """Get API URL for a specific table"""
        table_config = self.get_table_config(table_name)
        if table_config and table_config.api_source:
            return table_config.api_source.get('url')
        return None
    
    def get_partitioning_columns(self, table_name: str, layer_name: str) -> List[str]:
        """Get partitioning columns for a table layer"""
        layer_config = self.get_layer_config(table_name, layer_name)
        
        if not layer_config or not layer_config.partitioning.get('enabled', False):
            return []
        
        return layer_config.partitioning.get('columns', [])
    
    def get_partitioning_strategy(self, table_name: str, layer_name: str) -> str:
        """Get partitioning strategy for a table layer"""
        layer_config = self.get_layer_config(table_name, layer_name)
        if not layer_config:
            return 'none'
        return layer_config.partitioning.get('strategy', 'none')
    
    def is_cleaning_enabled(self, table_name: str, layer_name: str) -> bool:
        """Check if cleaning is enabled for a table layer"""
        layer_config = self.get_layer_config(table_name, layer_name)
        if not layer_config:
            return False
        return layer_config.cleaning.get('enabled', False)
    
    def get_layer_path(self, table_name: str, layer_name: str) -> str:
        """Get the path for a specific table layer"""
        table_config = self.get_table_config(table_name)
        if not table_config:
            return f'./{layer_name}'
        
        # Try to get from paths first, then from layer config
        if layer_name in table_config.paths:
            return table_config.paths[layer_name]
        
        layer_config = self.get_layer_config(table_name, layer_name)
        if layer_config:
            return layer_config.path
        
        return f'./{layer_name}/{table_name}'
    
    def override_layer_format(self, table_name: str, layer_name: str, format_name: str):
        """Override format for a specific table layer"""
        # Find the table in config
        for table_data in self.config['tables']:
            if table_data['table_name'] == table_name:
                if layer_name in table_data:
                    table_data[layer_name]['format'] = format_name
                    print(f"✅ Override {table_name}.{layer_name} format to {format_name}")
                    return
        
        print(f"⚠️ Table {table_name} or layer {layer_name} not found")
    
    def print_config_summary(self):
        """Print a summary of the current configuration"""
        print("\n📋 TABLE-BASED DATA LAKE CONFIGURATION SUMMARY")
        print("=" * 60)
        
        # Data lake info
        dl_config = self.get_data_lake_config()
        print(f"Name: {dl_config['name']}")
        print(f"Base Path: {dl_config['base_path']}")
        print(f"Default Format: {dl_config['default_format']}")
        
        # Tables
        tables = self.get_table_configs()
        print(f"\n📊 Tables: {len(tables)}")
        
        for table in tables:
            print(f"\n  📋 {table.table_name.upper()}:")
            print(f"    Schema: {len(table.schema)} columns")
            print(f"    API: {table.api_source['url'] if table.api_source else 'None'}")
            
            for layer_name in ['bronze', 'silver', 'gold', 'reporting']:
                layer_config = getattr(table, layer_name)
                cleaning_enabled = layer_config.cleaning.get('enabled', False)
                partitioning_enabled = layer_config.partitioning.get('enabled', False)
                
                print(f"    {layer_name.upper()}: {layer_config.format} format, "
                      f"partitioning: {'enabled' if partitioning_enabled else 'disabled'}, "
                      f"cleaning: {'enabled' if cleaning_enabled else 'disabled'}")
        
        # Gold Tables
        gold_tables = self.get_gold_table_configs()
        print(f"\n🥇 Gold Tables: {len(gold_tables)}")
        for table in gold_tables:
            print(f"  - {table.name}: {table.description}")
        
        # Reports
        reports = self.get_report_configs()
        print(f"\n📈 Reports: {len(reports)}")
        for report in reports:
            print(f"  - {report.name}: {report.description}")
        
        # Security
        security = self.get_security_config()
        sql_detection = security.get('sql_injection_detection', {})
        print(f"\n🛡️ Security:")
        print(f"  SQL Injection Detection: {sql_detection.get('enabled', False)}")


if __name__ == "__main__":
    # Test configuration manager
    try:
        config_manager = ConfigManager()
        config_manager.print_config_summary()
        
        # Test table configurations
        print("\n🧪 Testing table configurations...")
        tables = config_manager.get_table_configs()
        for table in tables:
            print(f"\n{table.table_name}:")
            for layer_name in ['bronze', 'silver', 'gold']:
                cleaning_rules = config_manager.get_cleaning_rules(table.table_name, layer_name)
                print(f"  {layer_name}: {len(cleaning_rules)} cleaning rules")
        
        print("\n✅ Configuration manager test completed!")
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        import traceback
        traceback.print_exc()