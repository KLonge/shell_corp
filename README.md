# Shell Corp Data Pipeline

A sample project demonstrating the integration of DuckDB, DLT, and SQLMesh. 
DuckDB is of course being used as the data warehouse, DLT is used to load dummy data into DuckDB, and SQLMesh is used to transform the data.

## Setup

1. Install UV package manager (if not already installed)
2. Run `make init` to set up the Python environment
3. Run `make run-loader` to load sample data into DuckDB
4. Run `make sqlmesh-plan` to execute SQLMesh transformations

## Project Structure

- `database/`: Contains the DuckDB database file
- `src/`
  - `loader/`: DLT scripts for loading data into DuckDB
  - `sqlmesh/`: SQLMesh configuration and models
    - `models/`: SQLMesh transformation models
    - `config.py`: SQLMesh configuration
