# Shell Corp Take Home

This repository contains the PDF and small code sample (rough draft) for my submission to the Shell Corp Take Home assignment.

## Architecture

The architecture is described in the PDF `proposal.pdf`. This contains images of the current and proposed architecture diagrams. There is also a description of the current data pipeline and proposed changes. Images can be found separately in the `diagrams` folder (`.png` and `.svg` versions).

## Data Pipeline (Basic Example)

A sample project demonstrating the integration of DuckDB, DLT, and SQLMesh. 
DuckDB is used as the data warehouse, DLT loads and validates dummy (semi-real) football transfer data, and SQLMesh handles data transformations with built-in testing and validation.

I have skipped the steps regarding converting the CDC logs to tables representing their active state as it would probably take too long to do in the time frame. I will happily explain it in the interview though.

**NOTE:** Don't be surprised if `make init` doesn't work. The original version of the Makefile was for Mac but I had to make it work for Windows which caused some hiccups.

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
- `make test`: Run all unit tests with coverage report and verbose logging
- `make mypy`: Run static type checking

### Data Pipeline
- `make dlt`: Load sample football transfer data into DuckDB
- `make sqlmesh-plan`: Execute SQLMesh transformations (only applies changes if models or data have changed)
- `make sqlmesh-run`: Run models based on their configured schedules
- `make sqlmesh-restate`: Force rerun of specific model transformations regardless of changes
- `make sqlmesh-test`: Run SQLMesh model unit tests
- `make sqlmesh-audit`: Run data quality audits / checks

### SQLMesh Execution Model

SQLMesh uses an intelligent execution model to determine when to run transformations:

1. **Plan vs Run**:
   - `sqlmesh plan` is used for deploying changes to models and synchronizing environments
   - `sqlmesh run` is used for scheduled execution of models based on their cron parameters
   - Use `plan` during development and deployment
   - Use `run` for production scheduled execution

2. **Plan Behavior**:
   - `sqlmesh plan` only applies changes when:
     - Model definitions have changed
     - New data is available
     - Models haven't been run for their scheduled interval
   - Running `plan` multiple times without changes will not trigger re-execution

3. **Run Behavior**:
   - `sqlmesh run` checks each model's cron schedule
   - Only executes models whose scheduled interval has elapsed since last run
   - Does not re-execute models that have run within their interval
   - Typically executed on a schedule (e.g., via crontab) at least as frequently as your shortest model interval
   - Example: If models run every 5 minutes, schedule `sqlmesh run` every 5 minutes

4. **Cron Scheduling**:
   - Models use `cron '*/5 * * * *'` configuration (runs every 5 minutes)
   - SQLMesh tracks the last successful run time
   - A model will only run if:
     - It hasn't been run in the last 5 minutes, OR
     - You explicitly force a rerun with `sqlmesh-restate`
   - The schedule is based on exact intervals, not calendar days
   - Forward-only runs don't mark the interval as complete

5. **Restate Command**:
   - Use `make sqlmesh-restate` to force model re-execution
   - Useful for:
     - Testing changes during development
     - Fixing data quality issues
     - Backfilling historical data

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

For simplicity, the SQLMesh models are implemented using full refreshes rather than incremental processing. Converting to incremental processing would involve adding timestamp/version columns to source data, which doesnt make sense in this example.

This would definitely be done if this was a larger dataset in a real project with more frequent updates.

## Testing Approach

The project demonstrates comprehensive testing at multiple levels:
- Unit tests for Python code using pytest
- Data validation during ingestion using Patito
- Model tests using SQLMesh's testing framework
- Data quality audits for transformed data

This ensures data quality and transformation accuracy throughout the pipeline.
