# TransferRoom Data Migration Example

This repository contains a proof-of-concept demonstration for TransferRoom's migration from SQL Server on Azure to Databricks. It showcases a modern data engineering approach using DuckDB (as a stand-in for the actual databases), DLT for data loading, and SQLMesh for transformation management.

## Architecture Overview

This repository demonstrates a migration path from the current architecture where analytics are performed directly on the production SQL Server database to a more scalable architecture using Databricks for analytics workloads.

- **Current State**: Analytics queries run directly against the production SQL Server database on Azure
- **Target State**: Analytics workloads moved to Databricks, with data synchronized from the production database

In this demo:
- `database/legacy/transferroom.duckdb` represents the SQL Server on Azure (production database)
- `database/new/transferroom.duckdb` represents Databricks (analytics platform)

## Migration Approach

The migration follows these key steps:

1. **Data Replication**: Copy raw data from SQL Server to Databricks (represented by copying data to the `raw` schema)
2. **Transformation**: Rebuild analytics models in Databricks using SQLMesh
3. **Validation**: Use the migration testing framework to verify data consistency between source and target systems
4. **Cutover**: Once validation passes, redirect analytics queries to the new platform

## Data Pipeline Example

This sample project demonstrates:

1. **Data Loading**: Using DLT to load and validate football transfer data
2. **Legacy Processing**: Running the existing legacy transformation code to create derived tables (also pushing them into the 'new' database that represents Databricks)
3. **Modern Transformation**: Using SQLMesh to create analytics models in a version-controlled, testable way
4. **Validation**: Using the migration test framework to compare results between systems

## Complete Workflow

The complete workflow for this demonstration is:

1. **Load Source Data**: Load sample football transfer data into the source database
2. **Run Legacy Transformations**: Execute the legacy transformation code to create derived tables in the legacy database (this also pushes the data into the 'new' database that represents Databricks)
3. **Copy to Target**: Copy the derived tables from the legacy database to the target database (raw schema)
4. **Run Modern Transformations**: Execute SQLMesh transformations to create the same tables in the target database (prod schema)
5. **Validate Results**: Run migration tests to compare the legacy and modern transformation results 

## Setup

1. Install UV package manager (if not already installed)
2. Run `make init` to set up the Python environment
3. Run `make load-source-data` to load sample data into both the legacy DuckDB database and the new DuckDB database
4. Run `make run-legacy` to execute legacy transformations and copy results to the legacy DuckDB database and the new DuckDB database
5. Run `make sqlmesh-plan` to execute SQLMesh transformations on the new DuckDB database
6. Run `make migration-test` to validate the results between the legacy and new DuckDB databases

**NOTE:** If you encounter issues with the `make init` command, you may need to adjust it for your operating system. The Makefile includes Windows and Mac-specific paths.

## Project Structure

- `database/`: Contains the DuckDB database files
  - `legacy/`: Represents SQL Server on Azure (production database)
  - `new/`: Represents Databricks (analytics platform)
- `src/`
  - `loader/`: DLT scripts for loading data into DuckDB
  - `legacy/`: Legacy transformation code that runs on the source database
    - `derived_a.py`, `derived_b.py`, `derived_c.py`: Legacy transformation implementations
    - `run_all.py`: Script to run all legacy transformations and copy results to the target database
  - `sqlmesh/`: SQLMesh configuration and models
    - `audits/`: SQLMesh audits for data quality
    - `models/`: SQLMesh transformation models
    - `config.py`: SQLMesh configuration
  - `migration_test/`: Framework for validating data consistency between systems
  - `utils/`: Shared utility functions
- `tests/`: Unit tests for loader and utility functions
  - `migration_test/`: Tests for the migration testing framework itself
    - `utils/`: Utility functions for the migration testing framework
      - `test_compare_duckdb_tables.py`: Tests for the migration testing framework itself

## Available Commands

### Development Setup
- `make init`: Set up Python environment and install dependencies
- `make clean`: Remove Python cache files and build artifacts
- `make upgrade-python-deps`: Upgrade all Python dependencies

### Testing & Validation
- `make test`: Run all unit tests with coverage report and verbose logging
- `make mypy`: Run static type checking
- `make migration-test`: Run data validation between source and target systems

### Data Pipeline
- `make load-source-data`: Load sample football transfer data into DuckDB
- `make run-legacy`: Run legacy transformations and copy results to the target database
- `make sqlmesh-plan`: Execute SQLMesh transformations (only applies changes if models or data have changed)
- `make sqlmesh-run`: Run models based on their configured schedules
- `make sqlmesh-restate`: Force rerun of specific model transformations regardless of changes
- `make sqlmesh-test`: Run SQLMesh model unit tests
- `make sqlmesh-audit`: Run data quality audits / checks

## Legacy Code

The `src/legacy/` directory contains the existing transformation code that currently runs on the production SQL Server database. This code is included to demonstrate the migration process from legacy implementations to modern, declarative transformations using SQLMesh.

Key components:
- `derived_a.py`, `derived_b.py`, `derived_c.py`: Individual transformation implementations
- `run_all.py`: Orchestration script that:
  1. Runs all legacy transformations to create derived tables in the legacy database
  2. Copies the derived tables from the legacy database to the target database (raw schema)

This approach allows us to:
1. Maintain the existing transformation logic during migration
2. Create a baseline for comparison with the new implementations
3. Gradually migrate transformations while ensuring consistency

## Migration Testing Framework

The migration testing framework provides tools to validate data consistency between the source and target systems:

1. **Table Comparison**: Compares tables between systems using primary keys
2. **Tolerance Settings**: Configurable tolerance for numeric differences and row count discrepancies
3. **Detailed Reporting**: Generates reports showing which columns and rows have differences
4. **Sampling**: Provides sample rows with differences to aid in debugging

### Migration Testing Approach

The repository offers a script-based approach to migration testing:

#### Script-Based Testing (`make migration-test`)
- Simple script that runs comparisons and outputs results
- Generates CSV reports in the `migration_test_results` directory
- Provides basic pass/fail information
- Detailed error output for all failed tables
- Useful for validating the entire migration process

## SQLMesh for Transformation Management

SQLMesh provides several key benefits for managing transformations during and after migration:

1. **Version Control**: All transformations are version-controlled SQL files
2. **Testing**: Built-in testing framework for validating transformations
3. **Dependency Management**: Automatically manages dependencies between models
4. **Incremental Processing**: Supports incremental model updates (though this demo uses full refreshes for simplicity)
5. **Scheduling**: Cron-based scheduling for model refreshes

### SQLMesh Execution Model

SQLMesh uses an intelligent execution model to determine when to run transformations:

1. **Plan vs Run**:
   - `sqlmesh plan` is used for deploying changes to models and synchronizing environments
   - `sqlmesh run` is used for scheduled execution of models based on their cron parameters

2. **Plan Behavior**:
   - `sqlmesh plan` only applies changes when:
     - Model definitions have changed
     - New data is available
     - Models haven't been run for their scheduled interval

3. **Run Behavior**:
   - `sqlmesh run` checks each model's cron schedule
   - Only executes models whose scheduled interval has elapsed since last run

4. **Cron Scheduling**:
   - Models use `cron '*/5 * * * *'` configuration (runs every 5 minutes)
   - SQLMesh tracks the last successful run time

5. **Restate Command**:
   - Use `make sqlmesh-restate` to force model re-execution
   - Useful for testing changes or fixing data quality issues

## Data Quality & Validation

### Loader Validation
The DLT loader includes comprehensive validation:
- Strict schema enforcement using Patito models
- Type validation for all fields
- Business rule validation (e.g., valid age ranges, market values)
- Automated data quality checks during ingestion

### SQLMesh Validation
SQLMesh models include:
- Data quality audits (e.g., value ranges, date validity)
- Schema validation
- Automated testing using SQLMesh's testing framework
- Data quality assertions in transformations

## Implementation Notes

For simplicity, the SQLMesh models are implemented using full refreshes rather than incremental processing. In a production environment with larger datasets, incremental processing would be implemented by adding timestamp/version columns to source data.

## Testing Approach

The project demonstrates comprehensive testing at multiple levels:
- Unit tests for Python code using pytest
- Data validation during ingestion using Patito
- Model tests using SQLMesh's testing framework
- Data quality audits for transformed data
- Migration tests comparing source and target systems
- Framework tests ensuring the migration testing framework itself is reliable

This multi-layered testing approach ensures data quality and transformation accuracy throughout the migration process.
