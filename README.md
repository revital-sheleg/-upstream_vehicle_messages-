# upstream_vehicle_messages

## Getting Started

**Prerequisites**: Install dependencies before running
```bash
pip install -r requirements.txt
```

**Entry Point**: Run the main application
```bash
python generic_data_lake_new.py
```
## Project Structure

```
upstream_vehicle_messages/
├── 📁 config/
│   └── config_data_lake.json          # Data lake configuration, tables, partitions etc'
├── 📁 data_lake/                      # Data lake storage layers
│   ├── 📁 bronze/                     # Raw ingested data
│   │   └── [table_name]/
│   │       └── data.parquet
│   ├── 📁 silver/                     # Cleaned & transformed data
│   │   └── [table_name]/
│   │       └── data.parquet
│   └── 📁 gold/                       # Aggregated analytics tables
│       └── [table_name]/
│           └── [aggregation_name\report].parquet
├── 📁 reporting/                      # Business reports output
│   └── [table_name]/
│       ├── report1.csv
│       └── report2.json
├── 📄 generic_data_lake.py        # Main application entry point
├── 📄 generic_data_processor.py       # Core data processing engine
├── 📄 config_manager.py               # Configuration management
├── 📄 mock_data_generator.py          # Mock data generation
├── 📄 requirements.txt                # Python dependencies
└── 📄 README.md                       # Project documentation
```

### Key Components

- **Main Application**: `generic_data_lake.py` - Interactive menu-driven interface
- **Processing Engine**: `generic_data_processor.py` - Configurable data processing logic
- **Configuration**: `config_data_lake.json` - Table definitions, layer configs, and processing rules
- **Data Storage**: Organized in bronze/silver/gold layers with Parquet format
- **Reports**: Generated in `reporting/` directory in CSV/JSON formats



## Menu Options

The data lake application provides the following main operations:

### 1. Run Data Pipeline
Complete data processing pipeline with multiple sub-options:

**Data Source Options:**
- API (requires Docker container)
- Mock data

**Table Selection:**
- Process all tables (currently only one table available, but configuration suppports additional tables)
- Select specific tables

**Processing Mode:**
- Full pipeline (bronze → silver → gold → reporting)
- Single layer processing:
  - Bronze layer only
  - Silver layer only (assume data in earlier stages)
  - Gold layer only (assume data in earlier stages)
  - Reporting layer only (assume data in earlier stages)

### 2. Run SQL Injection Violation Report
**data is needed in bronze layer
Security scanning functionality with options for:

**Table Selection:**
- 1.Scan all tables (currently only one table available, but configuration suppports additional tables)
- 2. Select specific tables

**Column Selection:**
- Use default columns (vin)
- Specify custom columns

**Pattern Selection:**
- Use default SQL injection patterns
- Specify custom regex patterns

## Data Lake Architecture

The table-based data lake follows a layered architecture:

- **Bronze Layer**: Raw ingested data in original format
- **Silver Layer**: Cleaned and transformed data with applied business rules
- **Gold Layer**: Aggregated tables optimized for analytics. Can hold longer period\ scope than report **Consider Retention here
- **Reporting**: Business reports in CSV/JSON format (including security report)

## Considerations and Future Improvements 

**Spark Integration**: This project currently uses pandas for simplicity. For production scale, consider migrating to Apache Spark for distributed processing.

**Data Lake Formats**: Open table formats like Apache Iceberg are supported (partial implementation). Consider full migration for:


