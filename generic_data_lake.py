#!/usr/bin/env python3
"""
Updated Generic Data Lake for Table-Based Configuration
Supports the new table-centric configuration structure with multiple tables
"""

import re
import json
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from config_manager import ConfigManager
from generic_data_processor import GenericDataProcessor, DataCleaner, PartitionManager, ParquetHandler, IcebergHandler
from mock_data_generator import MockDataGenerator


class TableProcessor:
    """Processes a single table through all layers"""
    
    def __init__(self, table_config, config_manager: ConfigManager, use_mock: bool = False):
        self.table_config = table_config
        self.config_manager = config_manager
        self.use_mock = use_mock
        self.mock_generator = MockDataGenerator() if use_mock else None
        
        # Setup paths
        dl_config = config_manager.get_data_lake_config()
        self.base_path = Path(dl_config['base_path'])
        
        # Setup layer paths for this table
        self.layer_paths = {}
        for layer_name in ['bronze', 'silver', 'gold', 'reporting']:
            layer_path = config_manager.get_layer_path(table_config.table_name, layer_name)
            self.layer_paths[layer_name] = self.base_path / layer_path
            self.layer_paths[layer_name].mkdir(parents=True, exist_ok=True)
        
        # Setup processors
        self.data_cleaner = DataCleaner()
        self.partition_manager = PartitionManager(config_manager)
        
        # Setup format handlers
        self.format_handlers = {}
        self._setup_format_handlers()
    
    def _setup_format_handlers(self):
        """Setup format handlers based on configuration"""
        formats_config = self.config_manager.config.get('formats', {})
        
        for format_name, format_config in formats_config.items():
            if format_name == 'parquet':
                self.format_handlers[format_name] = ParquetHandler(format_config)
            elif format_name == 'iceberg':
                self.format_handlers[format_name] = IcebergHandler(format_config)
    
    def fetch_data(self, amount: int = None) -> List[Dict[str, Any]]:
        """Fetch data for this table"""
        if self.use_mock:
            amount = amount or 10000
            print(f"🎭 Generating {amount} mock messages for {self.table_config.table_name}...")
            return self.mock_generator.generate_messages(amount)
        
        # Try API if available
        if self.table_config.api_source:
            try:
                url = self.table_config.api_source['url']
                if amount and '?' in url:
                    # Replace amount parameter if it exists
                    import urllib.parse
                    parsed = urllib.parse.urlparse(url)
                    params = urllib.parse.parse_qs(parsed.query)
                    params['amount'] = [str(amount)]
                    new_query = urllib.parse.urlencode(params, doseq=True)
                    url = urllib.parse.urlunparse(parsed._replace(query=new_query))
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                messages = response.json()
                print(f"📡 Fetched {len(messages)} messages from API for {self.table_config.table_name}")
                return messages
            except requests.RequestException as e:
                print(f"⚠️ API failed for {self.table_config.table_name} ({e}), falling back to mock data...")
                self.use_mock = True
                if not self.mock_generator:
                    self.mock_generator = MockDataGenerator()
                return self.mock_generator.generate_messages(amount or 10000)
        else:
            print(f"⚠️ No API source configured for {self.table_config.table_name}, using mock data...")
            self.use_mock = True
            if not self.mock_generator:
                self.mock_generator = MockDataGenerator()
            return self.mock_generator.generate_messages(amount or 10000)
    
    def process_layer(self, df: pd.DataFrame, layer_name: str) -> bool:
        """Process data for a specific layer of this table"""
        print(f"\n📊 Processing {self.table_config.table_name}.{layer_name.upper()} layer...")
        
        layer_config = self.config_manager.get_layer_config(self.table_config.table_name, layer_name)
        if not layer_config:
            print(f"❌ No configuration found for {self.table_config.table_name}.{layer_name}")
            return False
        
        # Apply cleaning rules
        if self.config_manager.is_cleaning_enabled(self.table_config.table_name, layer_name):
            cleaning_rules = self.config_manager.get_cleaning_rules(self.table_config.table_name, layer_name)
            df = self.data_cleaner.apply_cleaning_rules(df, cleaning_rules)
        
        # Create partitions
        partitions = self._create_partitions(df, layer_name)
        
        # Get format handler
        format_handler = self.format_handlers.get(layer_config.format)
        if not format_handler:
            print(f"❌ No handler for format: {layer_config.format}")
            return False
        
        # Write partitions
        success_count = 0
        for partition_key, partition_df in partitions.items():
            if partition_key == 'default':
                file_path = self.layer_paths[layer_name] / "data.parquet"
            else:
                file_path = self.layer_paths[layer_name] / partition_key / "data.parquet"
            
            if format_handler.write_data(partition_df, file_path):
                print(f"✅ Saved {len(partition_df)} records to {file_path}")
                success_count += 1
            else:
                print(f"❌ Failed to save partition: {partition_key}")
        
        print(f"✅ {self.table_config.table_name}.{layer_name.upper()} processing completed: {success_count}/{len(partitions)} partitions saved")
        return success_count == len(partitions)
    
    def _create_partitions(self, df: pd.DataFrame, layer_name: str) -> Dict[str, pd.DataFrame]:
        """Create partitions for this table layer"""
        partition_columns = self.config_manager.get_partitioning_columns(self.table_config.table_name, layer_name)
        strategy = self.config_manager.get_partitioning_strategy(self.table_config.table_name, layer_name)
        
        if not partition_columns or strategy == 'none':
            return {'default': df}
        
        if strategy == 'directory':
            return self._create_directory_partitions(df, partition_columns)
        elif strategy == 'metadata':
            return self._create_metadata_partitions(df, partition_columns)
        else:
            print(f"⚠️ Unknown partitioning strategy: {strategy}")
            return {'default': df}
    
    def _create_directory_partitions(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, pd.DataFrame]:
        """Create directory-based partitions (Hive-style)"""
        partitions = {}
        
        # Group by partition columns
        if len(columns) == 1:
            groups = df.groupby(columns[0])
        else:
            groups = df.groupby(columns)
        
        for partition_key, group in groups:
            if isinstance(partition_key, tuple):
                # Multiple partition columns
                partition_path = '/'.join([f"{col}={val}" for col, val in zip(columns, partition_key)])
            else:
                # Single partition column
                partition_path = f"{columns[0]}={partition_key}"
            
            partitions[partition_path] = group
        
        return partitions
    
    def _create_metadata_partitions(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, pd.DataFrame]:
        """Create metadata-based partitions (Iceberg-style)"""
        # For now, return single partition - Iceberg handles partitioning internally
        return {'metadata_partitioned': df}
    
    def read_layer_data(self, layer_name: str) -> Optional[pd.DataFrame]:
        """Read all data from a layer of this table"""
        layer_config = self.config_manager.get_layer_config(self.table_config.table_name, layer_name)
        if not layer_config:
            return None
        
        format_handler = self.format_handlers.get(layer_config.format)
        if not format_handler:
            print(f"❌ No handler for format: {layer_config.format}")
            return None
        
        # Find all data files in the layer
        layer_path = self.layer_paths[layer_name]
        data_files = list(layer_path.rglob("*.parquet"))
        
        if not data_files:
            print(f"⚠️ No data files found in {self.table_config.table_name}.{layer_name} layer")
            return None
        
        # Read and combine all files
        dataframes = []
        for file_path in data_files:
            df = format_handler.read_data(file_path)
            if df is not None:
                dataframes.append(df)
        
        if not dataframes:
            return None
        
        combined_df = pd.concat(dataframes, ignore_index=True)
        print(f"✅ Read {len(combined_df)} records from {self.table_config.table_name}.{layer_name} layer ({len(data_files)} files)")
        return combined_df


class GenericDataLake:
    """Updated generic data lake for table-based configuration"""
    
    def __init__(self, config_path: str = "config/config_data_lake.json", use_mock: bool = False):
        self.config_manager = ConfigManager(config_path)
        self.use_mock = use_mock
        
        # Get performance configuration
        self.performance_config = self.config_manager.get_performance_config()
        self.batch_size = self.performance_config.get('batch_size', 10000)
        
        # Setup table processors
        self.table_processors = {}
        self._setup_table_processors()
        
        print(f"🏗️ Generic Data Lake initialized (Table-based)")
        print(f"📋 Configuration: {self.config_manager.config_path}")
        print(f"📊 Data source: {'Mock' if use_mock else 'API'}")
        print(f"📋 Tables: {len(self.table_processors)}")
    
    def _setup_table_processors(self):
        """Setup processors for each table"""
        table_configs = self.config_manager.get_table_configs()
        
        for table_config in table_configs:
            processor = TableProcessor(table_config, self.config_manager, self.use_mock)
            self.table_processors[table_config.table_name] = processor
    
    def process_table_bronze_layer(self, table_name: str, amount: int = None) -> bool:
        """Process bronze layer for a specific table"""
        if table_name not in self.table_processors:
            print(f"❌ Table not found: {table_name}")
            return False
        
        processor = self.table_processors[table_name]
        
        print(f"\n🥉 PROCESSING {table_name.upper()} BRONZE LAYER")
        print("=" * 60)
        
        # Fetch raw data
        messages = processor.fetch_data(amount)
        if not messages:
            print(f"❌ No messages to process for {table_name}")
            return False
        
        # Convert to DataFrame and add derived columns
        df = pd.DataFrame(messages)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['datetime'].dt.date
        df['hour'] = df['datetime'].dt.hour
        
        # Process using table processor
        return processor.process_layer(df, 'bronze')
    
    def process_table_silver_layer(self, table_name: str) -> bool:
        """Process silver layer for a specific table"""
        if table_name not in self.table_processors:
            print(f"❌ Table not found: {table_name}")
            return False
        
        processor = self.table_processors[table_name]
        
        print(f"\n🥈 PROCESSING {table_name.upper()} SILVER LAYER")
        print("=" * 60)
        
        # Read bronze data
        bronze_df = processor.read_layer_data('bronze')
        if bronze_df is None:
            print(f"❌ No bronze data available for {table_name}")
            return False
        
        # Process using table processor (includes cleaning)
        return processor.process_layer(bronze_df, 'silver')
    
    def process_table_gold_layer(self, table_name: str) -> bool:
        """Process gold layer for a specific table - creates aggregated tables"""
        if table_name not in self.table_processors:
            print(f"❌ Table not found: {table_name}")
            return False
        
        print(f"\n🥇 PROCESSING {table_name.upper()} GOLD LAYER")
        print("=" * 60)
        
        # Generate aggregated tables for this table
        gold_tables = self._generate_gold_tables(table_name)
        
        if gold_tables:
            print(f"✅ Generated {len(gold_tables)} gold tables for {table_name}")
            return True
        else:
            print(f"❌ No gold tables generated for {table_name}")
            return False
    
    def process_table_reporting_layer(self, table_name: str) -> bool:
        """Process reporting layer for a specific table - creates business reports"""
        if table_name not in self.table_processors:
            print(f"❌ Table not found: {table_name}")
            return False
        
        print(f"\n📊 PROCESSING {table_name.upper()} REPORTING LAYER")
        print("=" * 60)
        
        # Generate reports for this table
        reports = self._generate_table_reports(table_name)
        
        if reports:
            print(f"✅ Generated {len(reports)} reports for {table_name}")
            return True
        else:
            print(f"❌ No reports generated for {table_name}")
            return False
    
    def _generate_gold_tables(self, table_name: str) -> Dict[str, pd.DataFrame]:
        """Generate aggregated tables for the gold layer"""
        processor = self.table_processors[table_name]
        gold_tables = {}
        gold_table_configs = self.config_manager.get_gold_table_configs()
        
        for gold_config in gold_table_configs:
            print(f"  - {gold_config.name}: {gold_config.description}")
            
            # Read source data
            source_df = processor.read_layer_data(gold_config.source_layer)
            if source_df is None:
                print(f"    ❌ No source data available from {gold_config.source_layer} layer")
                continue
            
            # Generate aggregated table
            agg_df = self._generate_aggregated_table(source_df, gold_config)
            if agg_df is not None:
                gold_tables[gold_config.name] = agg_df
                
                # Save to gold layer
                gold_path = processor.layer_paths['gold']
                table_path = gold_path / f"{gold_config.name}.parquet"
                
                format_handler = processor.format_handlers.get(gold_config.output_format, 
                                                            processor.format_handlers.get('parquet'))
                
                if format_handler and format_handler.write_data(agg_df, table_path):
                    print(f"    ✅ Saved {len(agg_df)} aggregated records to parquet table")
        
        return gold_tables
    
    def _generate_table_reports(self, table_name: str) -> Dict[str, pd.DataFrame]:
        """Generate business reports for a specific table - saves only to reporting layer"""
        processor = self.table_processors[table_name]
        reports = {}
        report_configs = self.config_manager.get_report_configs()
        
        for report_config in report_configs:
            print(f"  - {report_config.name}: {report_config.description}")
            
            # Read source data (from gold layer parquet tables)
            if report_config.source_table:
                # Read specific gold table (parquet file)
                gold_path = processor.layer_paths['gold']
                source_file = gold_path / f"{report_config.source_table}.parquet"
                
                format_handler = processor.format_handlers.get('parquet')
                source_df = format_handler.read_data(source_file) if format_handler else None
                
                if source_df is None:
                    print(f"    ❌ No source table available: {report_config.source_table}")
                    continue
            else:
                # Fallback: Read from specified layer
                source_df = processor.read_layer_data(report_config.source_layer)
                if source_df is None:
                    print(f"    ❌ No source data available from {report_config.source_layer} layer")
                    continue
            
            # Generate report
            report_df = self._generate_single_report(source_df, report_config)
            if report_df is not None:
                reports[report_config.name] = report_df
                
                # Save report to reporting layer in configured format
                # Create reporting path at project root level
                reporting_path = Path("reporting") / processor.table_config.table_name
                reporting_path.mkdir(parents=True, exist_ok=True)
                
                if report_config.output_format == 'csv':
                    report_path = reporting_path / f"{report_config.name}.csv"
                    report_df.to_csv(report_path, index=False)
                    print(f"    ✅ Saved {len(report_df)} records to CSV report")
                elif report_config.output_format == 'json':
                    report_path = reporting_path / f"{report_config.name}.json"
                    report_df.to_json(report_path, orient='records', indent=2)
                    print(f"    ✅ Saved {len(report_df)} records to JSON report")
                else:
                    # Default to CSV if format not recognized
                    report_path = reporting_path / f"{report_config.name}.csv"
                    report_df.to_csv(report_path, index=False)
                    print(f"    ✅ Saved {len(report_df)} records to CSV report (default format)")
        
        return reports
    
    def _generate_aggregated_table(self, df: pd.DataFrame, gold_config) -> Optional[pd.DataFrame]:
        """Generate an aggregated table for the gold layer"""
        try:
            if gold_config.aggregation_type == 'group_by':
                # Group by specified columns and apply aggregations
                grouped = df.groupby(gold_config.group_by)
                
                agg_dict = {}
                for col_name, agg_func in gold_config.aggregations.items():
                    if agg_func == 'mean':
                        agg_dict[col_name] = df['velocity'].mean() if 'velocity' in df.columns else 0
                    elif agg_func == 'max':
                        if col_name == 'last_timestamp':
                            agg_dict[col_name] = df['timestamp'].max() if 'timestamp' in df.columns else 0
                        else:
                            agg_dict[col_name] = df['velocity'].max() if 'velocity' in df.columns else 0
                    elif agg_func == 'min':
                        agg_dict[col_name] = df['velocity'].min() if 'velocity' in df.columns else 0
                    elif agg_func == 'count':
                        agg_dict[col_name] = df.shape[0]
                    elif agg_func == 'nunique': #TODO: replace with configurable key (should be configured in table section in config file)
                        agg_dict[col_name] = df['vin'].nunique() if 'vin' in df.columns else 0
                
                # Apply aggregations properly
                result_data = []
                for group_key, group_df in grouped:
                    row = {}
                    
                    # Add group by columns
                    if isinstance(group_key, tuple):
                        for i, col in enumerate(gold_config.group_by):
                            row[col] = group_key[i]
                    else:
                        row[gold_config.group_by[0]] = group_key
                    
                    # Add aggregated columns
                    #TODO: move part of inline aggregation to config file
                    for col_name, agg_func in gold_config.aggregations.items():
                        if agg_func == 'mean' and 'velocity' in group_df.columns:
                            row[col_name] = group_df['velocity'].mean()
                        elif agg_func == 'max':
                            if col_name == 'last_timestamp' and 'timestamp' in group_df.columns:
                                row[col_name] = group_df['timestamp'].max()
                            elif 'velocity' in group_df.columns:
                                row[col_name] = group_df['velocity'].max()
                        elif agg_func == 'min' and 'velocity' in group_df.columns:
                            row[col_name] = group_df['velocity'].min()
                        elif agg_func == 'count':
                            row[col_name] = len(group_df)
                        elif agg_func == 'nunique' and 'vin' in group_df.columns:
                            row[col_name] = group_df['vin'].nunique()
                    
                    result_data.append(row)
                
                return pd.DataFrame(result_data)
            else:
                print(f"    ⚠️ Unknown aggregation type: {gold_config.aggregation_type}")
                return None
        except Exception as e:
            print(f"    ❌ Error generating aggregated table: {e}")
            return None
    
    def _generate_single_report(self, df: pd.DataFrame, report_config) -> Optional[pd.DataFrame]:
        """Generate a single report based on configuration"""
        try:
            if report_config.report_type == 'detail':
                # Simple detail report - select specified columns
                available_cols = [col for col in report_config.columns if col in df.columns]
                return df[available_cols].copy()
            
            elif report_config.report_type == 'top_n':
                # Top N report
                if report_config.group_by:
                    # Group by and get top N per group
                    result_data = []
                    grouped = df.groupby(report_config.group_by)
                    
                    for group_key, group_df in grouped:
                        top_records = group_df.nlargest(report_config.top_n or 10, report_config.order_by)
                        result_data.append(top_records)
                    
                    if result_data:
                        return pd.concat(result_data, ignore_index=True)
                else:
                    # Simple top N
                    return df.nlargest(report_config.top_n or 10, report_config.order_by)
            
            elif report_config.report_type == 'dashboard':
                # Dashboard report - return all data with specified columns
                available_cols = [col for col in report_config.columns if col in df.columns]
                return df[available_cols].copy()
            
            else:
                print(f"    ⚠️ Unknown report type: {report_config.report_type}")
                return None
                
        except Exception as e:
            print(f"    ❌ Error generating report: {e}")
            return None
    

    
    def run_full_pipeline(self, amount: int = None, tables: List[str] = None) -> bool:
        """Run the complete data lake pipeline for specified tables"""
        print(f"\n🚀 STARTING TABLE-BASED DATA LAKE PIPELINE")
        print("=" * 70)
        
        dl_config = self.config_manager.get_data_lake_config()
        print(f"📋 Data Lake: {dl_config['name']}")
        print(f"📁 Base Path: {dl_config['base_path']}")
        
        # Determine which tables to process
        if tables is None:
            tables = list(self.table_processors.keys())
        
        print(f"📊 Processing Tables: {', '.join(tables)}")
        
        # Show table configurations
        print(f"\n📊 Table Configurations:")
        for table_name in tables:
            if table_name in self.table_processors:
                table_config = self.config_manager.get_table_config(table_name)
                print(f"  {table_name.upper()}:")
                for layer_name in ['bronze', 'silver', 'gold', 'reporting']:
                    layer_config = self.config_manager.get_layer_config(table_name, layer_name)
                    if layer_config:
                        partition_cols = self.config_manager.get_partitioning_columns(table_name, layer_name)
                        cleaning_enabled = self.config_manager.is_cleaning_enabled(table_name, layer_name)
                        
                        print(f"    {layer_name}: {layer_config.format} format, "
                              f"partitions: {partition_cols or 'none'}, "
                              f"cleaning: {'enabled' if cleaning_enabled else 'disabled'}")
        
        # Process each table through all layers
        success = True
        
        for table_name in tables:
            if table_name not in self.table_processors:
                print(f"❌ Table not found: {table_name}")
                success = False
                continue
            
            # Process Bronze layer
            if not self.process_table_bronze_layer(table_name, amount):
                print(f"❌ Bronze layer processing failed for {table_name}")
                success = False
                continue
            
            # Process Silver layer
            if not self.process_table_silver_layer(table_name):
                print(f"❌ Silver layer processing failed for {table_name}")
                success = False
                continue
            
            # Process Gold layer
            if not self.process_table_gold_layer(table_name):
                print(f"❌ Gold layer processing failed for {table_name}")
                success = False
                continue
            
            # Process Reporting layer
            if not self.process_table_reporting_layer(table_name):
                print(f"❌ Reporting layer processing failed for {table_name}")
                success = False
        
        if success:
            print(f"\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        else:
            print(f"\n❌ PIPELINE COMPLETED WITH ERRORS")
        
        return success
    
    def sqlInjectionReport(self, columns: List[str], patterns: List[str]) -> pd.DataFrame:
        """
        Run batch SQL injection detection on Bronze dataset and produce violation report.
        Uses multi-stage filtering for optimal performance: keyword filtering → character filtering → regex matching.
        
        Args:
            columns: List of column names to run the detection on
            patterns: List of SQL injection detection regex patterns
            
        Returns:
            DataFrame with original violating message and violating column name
            
        Example:
            sqlInjectionReport(['vin'], ['"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"'])
        """
        print(f"\n🛡️ MULTI-STAGE SQL INJECTION DETECTION")
        print("=" * 50)
        print(f"📊 Scanning columns: {columns}")
        print(f"🔍 Using {len(patterns)} detection patterns")
        
        violations = []
        
        # Pre-compile regex patterns for better performance
        compiled_patterns = []
        for pattern in patterns:
            try:
                compiled_patterns.append(re.compile(pattern, re.IGNORECASE))
            except re.error as e:
                print(f"⚠️ Invalid regex pattern '{pattern}': {e}")
                continue
        
        if not compiled_patterns:
            print("❌ No valid regex patterns provided")
            return pd.DataFrame()
        
        # Define multi-stage filtering criteria
        # Stage 1: High-risk SQL keywords (most common injection attempts)
        high_risk_keywords = { #TODO: can be added to config
            'select', 'delete', 'drop', 'insert', 'update', 'union', 'alter', 'create',
            'exec', 'execute', 'merge', 'truncate', 'grant', 'revoke', 'declare'
        }
        
        # Stage 2: Medium-risk keywords and patterns
        medium_risk_keywords = {  #TODO: can be added to config
            'script', 'javascript', 'vbscript', 'onload', 'onerror', 'eval',
            'xp_', 'sp_', 'sys', 'information_schema', 'master', 'msdb'
        }
        
        # Stage 3: Suspicious characters and patterns
        suspicious_chars = {';', '--', '/*', '*/', '<', '>', '()', '\'\'', '""'}
        
        # Process each table's Bronze layer
        total_records_scanned = 0
        total_violations_found = 0
        stage1_filtered = 0
        stage2_filtered = 0
        stage3_filtered = 0
        
        for table_name, processor in self.table_processors.items():
            print(f"🔍 Scanning {table_name} Bronze layer...")
            
            # Read Bronze layer data
            bronze_df = processor.read_layer_data('bronze')
            if bronze_df is None:
                print(f"⚠️ No Bronze data found for {table_name}")
                continue
            
            table_records = len(bronze_df)
            table_violations = 0
            table_stage1 = 0
            table_stage2 = 0
            table_stage3 = 0
            total_records_scanned += table_records
            
            # Process each column separately for better performance
            for column in columns:
                if column not in bronze_df.columns:
                    continue
                
                # Convert column to string and handle nulls
                column_values = bronze_df[column].astype(str).fillna('')
                column_lower = column_values.str.lower()
                
                # STAGE 1: Fast keyword filtering (highest priority threats)
                stage1_mask = pd.Series([False] * len(bronze_df))
                for keyword in high_risk_keywords:
                    keyword_mask = column_lower.str.contains(keyword, na=False)
                    stage1_mask = stage1_mask | keyword_mask
                
                stage1_indices = bronze_df[stage1_mask].index
                table_stage1 += len(stage1_indices)
                
                # STAGE 2: Medium-risk keyword filtering (on remaining data)
                remaining_mask = ~stage1_mask
                stage2_mask = pd.Series([False] * len(bronze_df))
                
                if remaining_mask.any():
                    remaining_values = column_lower[remaining_mask]
                    for keyword in medium_risk_keywords:
                        keyword_mask = remaining_values.str.contains(keyword, na=False)
                        # Map back to original indices with proper boolean indexing
                        stage2_mask.loc[remaining_mask] = stage2_mask.loc[remaining_mask] | keyword_mask
                
                stage2_indices = bronze_df[stage2_mask].index
                table_stage2 += len(stage2_indices)
                
                # STAGE 3: Character pattern filtering (on remaining data)
                combined_mask = stage1_mask | stage2_mask
                remaining_mask = ~combined_mask
                stage3_mask = pd.Series([False] * len(bronze_df))
                
                if remaining_mask.any():
                    remaining_values = column_values[remaining_mask]
                    for char_pattern in suspicious_chars:
                        char_mask = remaining_values.str.contains(re.escape(char_pattern), na=False)
                        # Map back to original indices with proper boolean indexing
                        stage3_mask.loc[remaining_mask] = stage3_mask.loc[remaining_mask] | char_mask
                
                stage3_indices = bronze_df[stage3_mask].index
                table_stage3 += len(stage3_indices)
                
                # Combine all suspicious indices for regex testing
                all_suspicious_indices = set(stage1_indices) | set(stage2_indices) | set(stage3_indices)
                
                print(f"    📊 {column}: {len(all_suspicious_indices):,} suspicious from {table_records:,} total")
                print(f"      🔴 Stage 1 (high-risk): {len(stage1_indices):,}")
                print(f"      🟡 Stage 2 (medium-risk): {len(stage2_indices):,}")
                print(f"      🟠 Stage 3 (char patterns): {len(stage3_indices):,}")
                
                # STAGE 4: Apply expensive regex only on filtered data
                for row_idx in all_suspicious_indices:
                    row = bronze_df.loc[row_idx]
                    original_message = row.to_dict()
                    value = str(row[column])
                    
                    # Determine threat level based on which stage caught it
                    threat_level = "HIGH" if row_idx in stage1_indices else \
                                  "MEDIUM" if row_idx in stage2_indices else "LOW"
                    
                    # Test against all regex patterns
                    for pattern_obj in compiled_patterns:
                        if pattern_obj.search(value):
                            violations.append({
                                'table': table_name,
                                'layer': 'bronze',
                                'row_index': row_idx,
                                'original_message': original_message,
                                'violating_column': column,
                                'violating_value': value,
                                'pattern_matched': pattern_obj.pattern,
                                'threat_level': threat_level,
                                'detection_stage': 1 if row_idx in stage1_indices else 
                                                 2 if row_idx in stage2_indices else 3
                            })
                            table_violations += 1
                            total_violations_found += 1
                            break  # Stop at first match per column to avoid duplicates
            
            stage1_filtered += table_stage1
            stage2_filtered += table_stage2
            stage3_filtered += table_stage3
            
            total_suspicious = table_stage1 + table_stage2 + table_stage3
            efficiency = (1 - total_suspicious / table_records) * 100 if table_records > 0 else 0
            
            print(f"  ✅ Scanned {table_records:,} records, found {table_violations} violations")
            print(f"  ⚡ Efficiency: {efficiency:.1f}% records skipped expensive regex")
        
        # Create results DataFrame
        violations_df = pd.DataFrame(violations)
        
        # Save report if violations found
        if not violations_df.empty and self.table_processors:
            # Save to reporting folder (security reports belong with business reports)
            first_processor = next(iter(self.table_processors.values()))
            table_name = first_processor.table_config.table_name
            
            # Create reporting path
            reporting_path = Path("reporting") / table_name
            reporting_path.mkdir(parents=True, exist_ok=True)
            
            # Save in multiple formats for different use cases
            # Parquet for efficient querying and analysis (goes to gold layer)
            gold_path = first_processor.layer_paths['gold']
            parquet_path = gold_path / "sql_injection_violations.parquet"
            violations_df.to_parquet(parquet_path, index=False)
            
            # JSON for security teams and APIs (goes to reporting folder)
            json_path = reporting_path / "sql_injection_violations.json"
            violations_df.to_json(json_path, orient='records', indent=2)
            
            # CSV for easy analysis in Excel/BI tools (goes to reporting folder)
            csv_path = reporting_path / "sql_injection_violations.csv"
            violations_df.to_csv(csv_path, index=False)
            
            print(f"💾 Saved security reports:")
            print(f"   🗃️  Parquet (analytics): {parquet_path}")
            print(f"   📄 JSON (APIs): {json_path}")
            print(f"   📊 CSV (business): {csv_path}")
        
        # Performance summary with multi-stage filtering metrics
        total_filtered = stage1_filtered + stage2_filtered + stage3_filtered
        overall_efficiency = (1 - total_filtered / total_records_scanned) * 100 if total_records_scanned > 0 else 0
        
        print(f"\n📊 MULTI-STAGE FILTERING PERFORMANCE:")
        print(f"  📈 Total records scanned: {total_records_scanned:,}")
        print(f"  🔴 Stage 1 (high-risk keywords): {stage1_filtered:,} records")
        print(f"  🟡 Stage 2 (medium-risk keywords): {stage2_filtered:,} records")
        print(f"  🟠 Stage 3 (suspicious characters): {stage3_filtered:,} records")
        print(f"  🎯 Total suspicious records: {total_filtered:,} ({total_filtered/total_records_scanned*100:.1f}%)")
        print(f"  ⚡ Regex operations avoided: {total_records_scanned - total_filtered:,} ({overall_efficiency:.1f}%)")
        print(f"  🛡️ Violations confirmed: {total_violations_found}")
        print(f"  📋 Tables processed: {len(self.table_processors)}")
        
        # Threat level breakdown if violations found
        if not violations_df.empty:
            threat_counts = violations_df['threat_level'].value_counts()
            print(f"\n🚨 THREAT LEVEL BREAKDOWN:")
            for level in ['HIGH', 'MEDIUM', 'LOW']:
                count = threat_counts.get(level, 0)
                if count > 0:
                    print(f"  {level}: {count} violations")
        
        return violations_df
    
    def sql_injection_report(self, columns: List[str] = None, patterns: List[str] = None) -> pd.DataFrame:
        """Run SQL injection detection across all tables"""
        print(f"\n🛡️ RUNNING SQL INJECTION DETECTION")
        print("=" * 50)
        
        security_config = self.config_manager.get_security_config()
        sql_config = security_config.get('sql_injection_detection', {})
        
        if not sql_config.get('enabled', False):
            print("⚠️ SQL injection detection is disabled in configuration")
            return pd.DataFrame()
        
        # Use configuration or provided parameters
        target_layers = sql_config.get('target_layers', ['bronze'])
        columns = columns or sql_config.get('columns', ['vin'])
        patterns = patterns or sql_config.get('patterns', [])
        
        if not patterns:
            print("⚠️ No SQL injection patterns configured")
            return pd.DataFrame()
        
        violations = []
        compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
        
        # Check each table and layer
        for table_name, processor in self.table_processors.items():
            for layer_name in target_layers:
                print(f"🔍 Scanning {table_name}.{layer_name} layer...")
                
                layer_df = processor.read_layer_data(layer_name)
                if layer_df is None:
                    continue
                
                # Check each row and column
                for _, row in layer_df.iterrows():
                    for column in columns:
                        if column in row and pd.notna(row[column]):
                            value = str(row[column])
                            
                            for pattern in compiled_patterns:
                                if pattern.search(value):
                                    violations.append({
                                        'table': table_name,
                                        'layer': layer_name,
                                        'original_message': row.to_dict(),
                                        'violating_column': column,
                                        'violating_value': value,
                                        'pattern_matched': pattern.pattern
                                    })
                                    break  # Stop at first match per column
        
        violations_df = pd.DataFrame(violations)
        
        if not violations_df.empty:
            # Save violations report to reporting folder
            if self.table_processors:
                first_processor = next(iter(self.table_processors.values()))
                table_name = first_processor.table_config.table_name
                
                # Create reporting path
                reporting_path = Path("reporting") / table_name
                reporting_path.mkdir(parents=True, exist_ok=True)
                
                # Save in multiple formats for different use cases
                # Parquet for efficient querying and analysis (goes to gold layer)
                gold_path = first_processor.layer_paths['gold']
                parquet_path = gold_path / "sql_injection_violations.parquet"
                violations_df.to_parquet(parquet_path, index=False)
                
                # JSON for security teams and APIs (goes to reporting folder)
                json_path = reporting_path / "sql_injection_violations.json"
                violations_df.to_json(json_path, orient='records', indent=2)
                
                # CSV for easy analysis in Excel/BI tools (goes to reporting folder)
                csv_path = reporting_path / "sql_injection_violations.csv"
                violations_df.to_csv(csv_path, index=False)
                
                print(f"✅ Found {len(violations_df)} violations, saved across layers")
        else:
            print("✅ No SQL injection violations found")
        
        return violations_df
    
    def print_summary(self):
        """Print a comprehensive summary of the data lake"""
        print(f"\n📋 TABLE-BASED DATA LAKE SUMMARY")
        print("=" * 70)
        
        # Configuration summary
        self.config_manager.print_config_summary()
        
        # Table statistics
        print(f"\n📊 TABLE STATISTICS")
        print("=" * 50)
        
        for table_name, processor in self.table_processors.items():
            print(f"\n📋 {table_name.upper()}:")
            
            for layer_name in ['bronze', 'silver', 'gold', 'reporting']:
                if layer_name == 'gold':
                    # For gold layer, count only parquet tables (aggregated data)
                    layer_path = processor.layer_paths[layer_name]
                    parquet_files = list(layer_path.glob("*.parquet"))
                    
                    if parquet_files:
                        print(f"  {layer_name}: {len(parquet_files)} aggregated tables (parquet only)")
                    else:
                        print(f"  {layer_name}: No aggregated tables available")
                        
                elif layer_name == 'reporting':
                    # For reporting layer, count business reports by format
                    layer_path = processor.layer_paths[layer_name]
                    csv_files = list(layer_path.glob("*.csv"))
                    json_files = list(layer_path.glob("*.json"))
                    
                    total_reports = len(csv_files) + len(json_files)
                    if total_reports > 0:
                        formats = []
                        if csv_files:
                            formats.append(f"{len(csv_files)} CSV")
                        if json_files:
                            formats.append(f"{len(json_files)} JSON")
                        print(f"  {layer_name}: {', '.join(formats)} reports")
                    else:
                        print(f"  {layer_name}: No reports available")
                        
                else:
                    layer_df = processor.read_layer_data(layer_name)
                    
                    if layer_df is not None:
                        print(f"  {layer_name}: {len(layer_df)} records, {len(layer_df.columns)} columns")
                        
                        # Layer-specific statistics
                        if layer_name in ['bronze', 'silver'] and 'vin' in layer_df.columns:
                            unique_vins = layer_df['vin'].nunique()
                            print(f"    Unique VINs: {unique_vins}")
                    else:
                        print(f"  {layer_name}: No data available")


def main():
    """Main function with operation selection"""
    print("🏗️ TABLE-BASED GENERIC DATA LAKE")
    print("=" * 70)
    
    # First, ask what operation to perform
    print("Select operation:")
    print("1. Run data pipeline")
    print("2. Run SQL injection violation report")
    
    operation_choice = input("Choose operation (1-2): ").strip()
    
    if operation_choice == "1":
        run_data_pipeline()
    elif operation_choice == "2":
        run_sql_injection_report()
    else:
        print("❌ Invalid operation selection")
        return


def run_data_pipeline():
    """Run the data pipeline operations"""
    print("\n📊 DATA PIPELINE MODE")
    print("=" * 50)
    
    print("Data source:")
    print("1. API (requires Docker container)")
    print("2. Mock data")
    
    data_choice = input("Choose data source (1-2): ").strip()
    use_mock = data_choice == "2"
    
    try:
        # Initialize generic data lake
        lake = GenericDataLake(use_mock=use_mock)
        
        # Show available tables
        table_names = list(lake.table_processors.keys())
        print(f"\nAvailable tables: {', '.join(table_names)}")
        
        # Ask which tables to process
        print("\nTable selection:")
        print("1. Process all tables")
        print("2. Select specific tables")
        
        table_choice = input("Choose option (1-2): ").strip()
        
        tables_to_process = None
        if table_choice == "2":
            selected = input(f"Enter table names (comma-separated) from {table_names}: ").strip()
            if selected:
                tables_to_process = [t.strip() for t in selected.split(',')]
        
        # Ask about layer processing
        print("\nProcessing mode:")
        print("1. Full pipeline (all layers: bronze → silver → gold → reporting)")
        print("2. Process specific layer only")
        
        processing_choice = input("Choose processing mode (1-2): ").strip()
        
        if processing_choice == "2":
            # Single layer processing
            print("\nSelect layer to process:")
            print("1. Bronze layer only")
            print("2. Silver layer only") 
            print("3. Gold layer only")
            print("4. Reporting layer only")
            
            layer_choice = input("Choose layer (1-4): ").strip()
            
            layer_map = {"1": "bronze", "2": "silver", "3": "gold", "4": "reporting"}
            selected_layer = layer_map.get(layer_choice)
            
            if not selected_layer:
                print("❌ Invalid layer selection")
                return
            
            # Process single layer for selected tables
            success = process_single_layer(lake, selected_layer, tables_to_process)
            
        else:
            # Full pipeline processing
            success = lake.run_full_pipeline(tables=tables_to_process)
        
        # Print summary
        lake.print_summary()
        
        print(f"\n🎉 DATA PIPELINE COMPLETED!")
        print(f"✅ Pipeline success: {success}")
        
    except Exception as e:
        print(f"❌ Data pipeline failed: {e}")
        import traceback
        traceback.print_exc()



def run_sql_injection_report():
    """Run SQL injection violation detection independently"""
    print("\n🛡️ SQL INJECTION VIOLATION REPORT MODE")
    print("=" * 50)
    
    print("Data source:")
    print("1. API (requires Docker container)")
    print("2. Mock data")
    
    data_choice = input("Choose data source (1-2): ").strip()
    use_mock = data_choice == "2"
    
    try:
        # Initialize generic data lake
        lake = GenericDataLake(use_mock=use_mock)
        
        # Show available tables
        table_names = list(lake.table_processors.keys())
        print(f"\nAvailable tables: {', '.join(table_names)}")
        
        # Ask which tables to scan
        print("\nTable selection for security scan:")
        print("1. Scan all tables")
        print("2. Select specific tables")
        
        table_choice = input("Choose option (1-2): ").strip()
        
        tables_to_scan = None
        if table_choice == "2":
            selected = input(f"Enter table names (comma-separated) from {table_names}: ").strip()
            if selected:
                tables_to_scan = [t.strip() for t in selected.split(',')]
        
        # Ask for columns to scan
        print(f"\nColumn selection:")
        print("1. Use default columns (vin)")
        print("2. Specify custom columns")
        
        column_choice = input("Choose option (1-2): ").strip()
        
        if column_choice == "2":
            columns_input = input("Enter column names (comma-separated): ").strip()
            columns = [col.strip() for col in columns_input.split(',') if col.strip()]
        else:
            columns = ['vin']  # Default
        
        # Ask for regex patterns
        print(f"\nRegex patterns:")
        print("1. Use default SQL injection patterns")
        print("2. Specify custom patterns")
        
        pattern_choice = input("Choose option (1-2): ").strip()
        
        if pattern_choice == "2":
            print("Enter regex patterns (one per line, empty line to finish):")
            patterns = []
            while True:
                pattern = input("Pattern: ").strip()
                if not pattern:
                    break
                patterns.append(pattern)
        else:
            # Default SQL injection pattern
            patterns = ["('(''|[^'])*')|(;)|(\\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\\b)"]
        
        if not patterns:
            print("❌ No patterns specified")
            return
        
        # Run SQL injection detection using the new API
        print(f"\n🔍 Starting batch SQL injection scan...")
        violations = lake.sqlInjectionReport(columns, patterns)
        
        # Print detailed results
        print(f"\n🛡️ SQL INJECTION SCAN RESULTS")
        print("=" * 50)
        
        if len(violations) > 0:
            print(f"⚠️ Found {len(violations)} potential SQL injection violations:")
            
            # Group violations by table and pattern
            violation_summary = {}
            for _, violation in violations.iterrows():
                table = violation['table']
                pattern = violation['pattern_matched']
                
                if table not in violation_summary:
                    violation_summary[table] = {}
                if pattern not in violation_summary[table]:
                    violation_summary[table][pattern] = 0
                violation_summary[table][pattern] += 1
            
            for table, patterns in violation_summary.items():
                print(f"\n📋 {table.upper()}:")
                for pattern, count in patterns.items():
                    print(f"  - Pattern '{pattern}': {count} violations")
            
            # Show sample violations
            print(f"\n📝 Sample violations (first 5):")
            for i, (_, violation) in enumerate(violations.head().iterrows()):
                print(f"\n{i+1}. Table: {violation['table']}")
                print(f"   Column: {violation['violating_column']}")
                print(f"   Value: {violation['violating_value']}")
                print(f"   Pattern: {violation['pattern_matched']}")
        else:
            print("✅ No SQL injection violations found!")
        
        print(f"\n🎉 SQL INJECTION SCAN COMPLETED!")
        print(f"🛡️ Total violations found: {len(violations)}")
        
    except Exception as e:
        print(f"❌ SQL injection scan failed: {e}")
        import traceback
        traceback.print_exc()


def process_single_layer(lake: GenericDataLake, layer_name: str, tables: List[str] = None) -> bool:
    """Process only a specific layer for selected tables"""
    print(f"\n🎯 PROCESSING SINGLE LAYER: {layer_name.upper()}")
    print("=" * 60)
    
    # Determine which tables to process
    if tables is None:
        tables = list(lake.table_processors.keys())
    
    print(f"📊 Processing {layer_name} layer for tables: {', '.join(tables)}")
    
    success = True
    
    for table_name in tables:
        if table_name not in lake.table_processors:
            print(f"❌ Table not found: {table_name}")
            success = False
            continue
        
        print(f"\n📋 Processing {table_name}.{layer_name}...")
        
        if layer_name == "bronze":
            # For bronze layer, ask for amount of data
            try:
                amount_input = input(f"Enter amount of data to fetch for {table_name} (press Enter for default(10K)): ").strip()
                amount = int(amount_input) if amount_input else None
            except ValueError:
                amount = None
            
            if not lake.process_table_bronze_layer(table_name, amount):
                print(f"❌ Bronze layer processing failed for {table_name}")
                success = False
                
        elif layer_name == "silver":
            if not lake.process_table_silver_layer(table_name):
                print(f"❌ Silver layer processing failed for {table_name}")
                success = False
                
        elif layer_name == "gold":
            if not lake.process_table_gold_layer(table_name):
                print(f"❌ Gold layer processing failed for {table_name}")
                success = False
                
        elif layer_name == "reporting":
            if not lake.process_table_reporting_layer(table_name):
                print(f"❌ Reporting layer processing failed for {table_name}")
                success = False
    
    if success:
        print(f"\n✅ {layer_name.upper()} LAYER PROCESSING COMPLETED SUCCESSFULLY!")
    else:
        print(f"\n❌ {layer_name.upper()} LAYER PROCESSING COMPLETED WITH ERRORS")
    
    return success



if __name__ == "__main__":
    main()