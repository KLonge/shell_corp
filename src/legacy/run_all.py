"""Script to run all legacy derived tables in the correct order."""

import time
from collections.abc import Callable

# Import the functions directly
from src.legacy.derived_a import create_derived_table_a
from src.legacy.derived_b import create_derived_table_b
from src.legacy.derived_c import create_derived_table_c


def run_function(name: str, func: Callable[[], None]) -> None:
    """Run a function and time it.

    Args:
        name: Name of the function for display
        func: Function to run
    """
    print(f"\n{'=' * 80}")
    print(f"Running {name}...")
    print(f"{'=' * 80}\n")

    try:
        start_time = time.time()
        func()
        end_time = time.time()

        print(f"\nCompleted {name} in {end_time - start_time:.2f} seconds")

    except Exception as e:
        print(f"Error running {name}: {e}")
        raise


def main() -> None:
    """Run all derived tables in the correct order."""
    # Run the derived tables in order
    run_function("derived_table_a", create_derived_table_a)
    run_function("derived_table_b", create_derived_table_b)
    run_function("derived_table_c", create_derived_table_c)

    print("\nAll derived tables have been created successfully!")


if __name__ == "__main__":
    main()
