#!/usr/bin/env python3
"""
Generic Data Processor
Configurable data processing engine that supports multiple formats and cleaning strategies
"""

import re
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from abc import ABC, abstractmethod

from config_manager import ConfigManager, CleaningRule


class DataCleaner:
    """Generic data cleaning engine based on configuration"""
    
    def __init__(self):
        self.cleaning_functions = {
            'trim_strings': self._trim_strings,
            'filter_nulls': self._filter_nulls,
            'standardize_gear': self._standardize_gear,
            'normalize_case': self._normalize_case,
            'validate_range': self._validate_range
        }
    
    def apply_cleaning_rules(self, df: pd.DataFrame, rules: List[CleaningRule]) -> pd.DataFrame:
        """Apply all cleaning rules to a DataFrame"""
        if not rules:
            return df
        
        print(f"🧹 Applying {len(rules)} cleaning rules...")
        original_count = len(df)
        
        for rule in rules:
            print(f"  - {rule.description}")
            df = self._apply_single_rule(df, rule)
        
        final_count = len(df)
        print(f"✅ Cleaning completed: {original_count} → {final_count} records")
        
        return df
    
    def _apply_single_rule(self, df: pd.DataFrame, rule: CleaningRule) -> pd.DataFrame:
        """Apply a single cleaning rule"""
        if rule.type not in self.cleaning_functions:
            print(f"⚠️ Unknown cleaning rule type: {rule.type}")
            return df
        
        try:
            return self.cleaning_functions[rule.type](df, rule)
        except Exception as e:
            print(f"❌ Error applying rule {rule.type}: {e}")
            return df
    
    def _trim_strings(self, df: pd.DataFrame, rule: CleaningRule) -> pd.DataFrame:
        """Remove leading and trailing whitespace"""
        for column in rule.columns:
            if column in df.columns:
                df[column] = df[column].astype(str).str.strip()
        return df
    
    def _filter_nulls(self, df: pd.DataFrame, rule: CleaningRule) -> pd.DataFrame:
        """Remove rows with null values in specified columns"""
        return df.dropna(subset=rule.columns)
    
    def _standardize_gear(self, df: pd.DataFrame, rule: CleaningRule) -> pd.DataFrame:
        """Standardize gear position values"""
        def extract_numeric_gear(gear):
            if pd.isna(gear):
                return None
            gear_str = str(gear)
            if rule.regex:
                match = re.search(rule.regex, gear_str)
                if match:
                    return int(match.group())
            return None
        
        for column in rule.columns:
            if column in df.columns:
                df[column] = df[column].apply(extract_numeric_gear)
        
        return df
    
    def _normalize_case(self, df: pd.DataFrame, rule: CleaningRule) -> pd.DataFrame:
        """Normalize string case"""
        case_method = rule.method or 'lower'
        
        for column in rule.columns:
            if column in df.columns:
                if case_method == 'upper':
                    df[column] = df[column].astype(str).str.upper()
                elif case_method == 'lower':
                    df[column] = df[column].astype(str).str.lower()
                elif case_method == 'title':
                    df[column] = df[column].astype(str).str.title()
        
        return df
    
    def _validate_range(self, df: pd.DataFrame, rule: CleaningRule) -> pd.DataFrame:
        """Validate numeric values within range"""
        for column in rule.columns:
            if column in df.columns:
                if rule.min_value is not None:
                    df = df[df[column] >= rule.min_value]
                if rule.max_value is not None:
                    df = df[df[column] <= rule.max_value]
        
        return df


class PartitionManager:
    """Manages data partitioning strategies"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
    
    def create_partitions(self, df: pd.DataFrame, layer_name: str) -> Dict[str, pd.DataFrame]:
        """Create partitions based on layer configuration"""
        partition_columns = self.config_manager.get_partitioning_columns(layer_name)
        strategy = self.config_manager.get_partitioning_strategy(layer_name)
        
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


class FormatHandler(ABC):
    """Abstract base class for format handlers"""
    
    @abstractmethod
    def write_data(self, df: pd.DataFrame, path: Path, **kwargs) -> bool:
        """Write DataFrame to storage"""
        pass
    
    @abstractmethod
    def read_data(self, path: Path, **kwargs) -> Optional[pd.DataFrame]:
        """Read DataFrame from storage"""
        pass


class ParquetHandler(FormatHandler):
    """Parquet format handler"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def write_data(self, df: pd.DataFrame, path: Path, **kwargs) -> bool:
        """Write DataFrame to Parquet file"""
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            
            write_options = {
                'compression': self.config.get('compression', 'snappy'),
                'index': False
            }
            write_options.update(kwargs)
            
            df.to_parquet(path, **write_options)
            return True
        except Exception as e:
            print(f"❌ Error writing Parquet file {path}: {e}")
            return False
    
    def read_data(self, path: Path, **kwargs) -> Optional[pd.DataFrame]:
        """Read DataFrame from Parquet file"""
        try:
            if not path.exists():
                return None
            return pd.read_parquet(path, **kwargs)
        except Exception as e:
            print(f"❌ Error reading Parquet file {path}: {e}")
            return None


class IcebergHandler(FormatHandler):
    """Iceberg format handler"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.catalog = None
        self._setup_catalog()
    
    def _setup_catalog(self):
        """Setup Iceberg catalog"""
        try:
            from pyiceberg.catalog import load_catalog
            
            catalog_type = self.config.get('catalog_type', 'filesystem')
            catalog_config = self.config.get('catalog_config', {})
            
            self.catalog = load_catalog("local", type=catalog_type, **catalog_config)
            print(f"✅ Iceberg catalog setup: {catalog_type}")
        except ImportError:
            print("⚠️ PyIceberg not available, falling back to Parquet")
            self.catalog = None
        except Exception as e:
            print(f"⚠️ Iceberg catalog setup failed: {e}")
            self.catalog = None
    
    def write_data(self, df: pd.DataFrame, path: Path, **kwargs) -> bool:
        """Write DataFrame to Iceberg table"""
        if not self.catalog:
            # Fallback to Parquet
            parquet_handler = ParquetHandler({'compression': 'snappy'})
            return parquet_handler.write_data(df, path, **kwargs)
        
        try:
            # Implementation would depend on specific Iceberg table operations
            # For now, fallback to Parquet
            print("⚠️ Iceberg write not fully implemented, using Parquet fallback")
            parquet_handler = ParquetHandler({'compression': 'snappy'})
            return parquet_handler.write_data(df, path, **kwargs)
        except Exception as e:
            print(f"❌ Error writing Iceberg table: {e}")
            return False
    
    def read_data(self, path: Path, **kwargs) -> Optional[pd.DataFrame]:
        """Read DataFrame from Iceberg table"""
        if not self.catalog:
            # Fallback to Parquet
            parquet_handler = ParquetHandler({'compression': 'snappy'})
            return parquet_handler.read_data(path, **kwargs)
        
        try:
            # Implementation would depend on specific Iceberg table operations
            # For now, fallback to Parquet
            parquet_handler = ParquetHandler({'compression': 'snappy'})
            return parquet_handler.read_data(path, **kwargs)
        except Exception as e:
            print(f"❌ Error reading Iceberg table: {e}")
            return None


class GenericDataProcessor:
    """Generic data processor using configuration-driven approach"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.data_cleaner = DataCleaner()
        self.partition_manager = PartitionManager(config_manager)
        
        # Initialize format handlers
        self.format_handlers = {}
        self._setup_format_handlers()
        
        # Setup base paths
        dl_config = config_manager.get_data_lake_config()
        self.base_path = Path(dl_config['base_path'])
        self.layer_paths = {
            'bronze': self.base_path / 'bronze',
            'silver': self.base_path / 'silver', 
            'gold': self.base_path / 'gold'
        }
        
        # Create directories
        for path in self.layer_paths.values():
            path.mkdir(parents=True, exist_ok=True)
    
    def _setup_format_handlers(self):
        """Setup format handlers based on configuration"""
        formats_config = self.config_manager.config.get('formats', {})
        
        for format_name, format_config in formats_config.items():
            if format_name == 'parquet':
                self.format_handlers[format_name] = ParquetHandler(format_config)
            elif format_name == 'iceberg':
                self.format_handlers[format_name] = IcebergHandler(format_config)
    
    def process_layer(self, df: pd.DataFrame, layer_name: str) -> bool:
        """Process data for a specific layer"""
        print(f"\n📊 Processing {layer_name.upper()} layer...")
        
        layer_config = self.config_manager.get_layer_config(layer_name)
        
        # Apply cleaning rules
        if self.config_manager.is_cleaning_enabled(layer_name):
            cleaning_rules = self.config_manager.get_cleaning_rules(layer_name)
            df = self.data_cleaner.apply_cleaning_rules(df, cleaning_rules)
        
        # Create partitions
        partitions = self.partition_manager.create_partitions(df, layer_name)
        
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
        
        print(f"✅ {layer_name.upper()} layer processing completed: {success_count}/{len(partitions)} partitions saved")
        return success_count == len(partitions)
    
    def read_layer_data(self, layer_name: str) -> Optional[pd.DataFrame]:
        """Read all data from a layer"""
        layer_config = self.config_manager.get_layer_config(layer_name)
        format_handler = self.format_handlers.get(layer_config.format)
        
        if not format_handler:
            print(f"❌ No handler for format: {layer_config.format}")
            return None
        
        # Find all data files in the layer
        layer_path = self.layer_paths[layer_name]
        data_files = list(layer_path.rglob("*.parquet"))
        
        if not data_files:
            print(f"⚠️ No data files found in {layer_name} layer")
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
        print(f"✅ Read {len(combined_df)} records from {layer_name} layer ({len(data_files)} files)")
        return combined_df
    
    def generate_reports(self) -> Dict[str, pd.DataFrame]:
        """Generate all configured reports"""
        print(f"\n📈 Generating reports...")
        
        reports = {}
        report_configs = self.config_manager.get_report_configs()
        
        for report_config in report_configs:
            print(f"  - {report_config.name}: {report_config.description}")
            
            # Read source data
            source_df = self.read_layer_data(report_config.source_layer)
            if source_df is None:
                print(f"    ❌ No source data available")
                continue
            
            # Generate report
            report_df = self._generate_single_report(source_df, report_config)
            if report_df is not None:
                reports[report_config.name] = report_df
                
                # Save report
                report_path = self.layer_paths['gold'] / f"{report_config.name}.parquet"
                format_handler = self.format_handlers.get(report_config.output_format, 
                                                        self.format_handlers['parquet'])
                
                if format_handler.write_data(report_df, report_path):
                    print(f"    ✅ Saved {len(report_df)} records")
                    
                    # Also save as CSV for easy viewing
                    csv_path = self.layer_paths['gold'] / f"{report_config.name}.csv"
                    report_df.to_csv(csv_path, index=False)
        
        return reports
    
    def _generate_single_report(self, df: pd.DataFrame, report_config) -> Optional[pd.DataFrame]:
        """Generate a single report based on configuration"""
        try:
            if report_config.name == 'vin_last_state':
                return self._generate_vin_last_state_report(df, report_config)
            elif report_config.name == 'fastest_vehicles_per_hour':
                return self._generate_fastest_vehicles_report(df, report_config)
            else:
                print(f"    ⚠️ Unknown report type: {report_config.name}")
                return None
        except Exception as e:
            print(f"    ❌ Error generating report: {e}")
            return None
    
    def _generate_vin_last_state_report(self, df: pd.DataFrame, report_config) -> pd.DataFrame:
        """Generate VIN last state report"""
        df_sorted = df.sort_values('timestamp')
        
        vin_last_state = []
        for vin in df['vin'].unique():
            vin_data = df_sorted[df_sorted['vin'] == vin]
            
            last_timestamp = vin_data['timestamp'].max()
            
            # Get last non-null values
            front_door = vin_data.dropna(subset=['frontLeftDoorState'])['frontLeftDoorState'].iloc[-1] \
                if not vin_data.dropna(subset=['frontLeftDoorState']).empty else None
            wipers = vin_data.dropna(subset=['wipersState'])['wipersState'].iloc[-1] \
                if not vin_data.dropna(subset=['wipersState']).empty else None
            
            vin_last_state.append({
                'vin': vin,
                'last_reported_timestamp': last_timestamp,
                'front_left_door_state': front_door,
                'wipers_state': wipers
            })
        
        return pd.DataFrame(vin_last_state)
    
    def _generate_fastest_vehicles_report(self, df: pd.DataFrame, report_config) -> pd.DataFrame:
        """Generate fastest vehicles per hour report"""
        df['date_hour'] = df['datetime'].dt.strftime('%Y-%m-%d %H:00')
        
        # Get max velocity per VIN per hour
        fastest_per_hour = df.groupby(['date_hour', 'vin'])['velocity'].max().reset_index()
        
        # Get top N fastest per hour
        top_n = report_config.top_n or 10
        top_fastest = []
        
        for date_hour in fastest_per_hour['date_hour'].unique():
            hour_data = fastest_per_hour[fastest_per_hour['date_hour'] == date_hour]
            top_vehicles = hour_data.nlargest(top_n, 'velocity')
            top_fastest.append(top_vehicles)
        
        if top_fastest:
            result_df = pd.concat(top_fastest, ignore_index=True)
            return result_df.sort_values(['date_hour', 'velocity'], ascending=[True, False])
        
        return pd.DataFrame()


if __name__ == "__main__":
    # Test the generic data processor
    try:
        config_manager = ConfigManager()
        processor = GenericDataProcessor(config_manager)
        
        print("✅ Generic data processor initialized successfully")
        print(f"📁 Base path: {processor.base_path}")
        print(f"🔧 Format handlers: {list(processor.format_handlers.keys())}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()