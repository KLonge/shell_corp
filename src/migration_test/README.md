# Migration Test

This module provides tools for comparing tables between different schemas in DuckDB to validate data migrations.

## Overview

The migration test compares tables between `prod` and `legacy_prod` schemas to ensure data consistency during migrations. It checks for:

- Row count differences
- Value differences with configurable tolerance
- Column-level differences

## Usage

To run the migration test:

```python
from src.migration_test.main import main

# Run the test with default settings
main(folder_path="migration_test_results")
```

## Configuration

The tables to test are defined in `main.py` with their primary keys:

```python
tables_to_test = {
    "derived_a": ["id", "timestamp"],
    "derived_b": ["user_id", "event_date"],
    "derived_c": ["session_id"]
}
```

You can modify this dictionary to add or remove tables as needed.

## Tolerances

The test uses two types of tolerances:

- `value_tolerance`: Maximum allowed difference for numeric values (default: 0.20 or 20%)
- `row_tolerance`: Maximum percentage of rows that can fail (default: 0.01 or 1%)

## Results

Results are saved to CSV files in the `comparison_results` directory:
- A complete results file
- A failures-only file (if there are any failures)

The test also prints a summary of the results to the console.

## Customization

You can customize the test by modifying:

- The tables to test and their primary keys
- The tolerance values
- The columns to exclude from comparison
- The timestamp range to exclude from comparison 