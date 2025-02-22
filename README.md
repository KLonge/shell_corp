# Shell Corp Data Pipeline

A sample project demonstrating the integration of DuckDB, DLT, and SQLMesh. 
DuckDB is used as the data warehouse, DLT loads and validates dummy football transfer data, and SQLMesh handles data transformations with built-in testing and validation.

## Setup

1. Install UV package manager (if not already installed)
2. Run `make init` to set up the Python environment
3. Run `make dlt` to load sample data into DuckDB
4. Run `make sqlmesh-plan` to execute SQLMesh transformations

## Project Structure

- `database/`: Contains the DuckDB database file
- `src/`
  - `loader/`: DLT scripts for loading data into DuckDB
  - `sqlmesh/`: SQLMesh configuration and models
    - `audits/`: SQLMesh audits
    - `models/`: SQLMesh transformation models
    - `tests/`: SQLMesh tests
    - `config.py`: SQLMesh configuration
  - `utils/`: Shared utility functions
- `tests/`: Unit tests for loader and utility functions

## Available Commands

### Development Setup
- `make init`: Set up Python environment and install dependencies
- `make clean`: Remove Python cache files and build artifacts
- `make upgrade-python-deps`: Upgrade all Python dependencies

### Testing & Validation
- `make test`: Run all unit tests with coverage report
- `make test-debug`: Run tests in debug mode with detailed logging
- `make mypy`: Run static type checking

### Data Pipeline
- `make dlt`: Load sample football transfer data into DuckDB
- `make sqlmesh-plan`: Execute SQLMesh transformations
- `make sqlmesh-restate`: Rerun specific model transformations
- `make sqlmesh-test`: Run SQLMesh model tests
- `make sqlmesh-audit`: Run data quality audits

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

For simplicity, the SQLMesh models are implemented using full refreshes rather than incremental processing. Converting to incremental processing would involve:
1. Adding timestamp/version columns to source data
2. Modifying models to use incremental processing
3. Implementing merge strategies for each model

This would definitely be done if this was a larger dataset in a real project with more frequent updates.

## Testing Approach

The project demonstrates comprehensive testing at multiple levels:
- Unit tests for Python code using pytest
- Data validation during ingestion using Patito
- Model tests using SQLMesh's testing framework
- Data quality audits for transformed data

This ensures data quality and transformation accuracy throughout the pipeline.
