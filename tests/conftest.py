import pytest
import glob
import os

# list of duckdb files to not remove
DO_NOT_REMOVE_LIST = []


@pytest.fixture(scope="session", autouse=True)
def cleanup_duckdb_files():
    """Fixture to clean up *.duckdb files after all tests in a session run."""
    # Teardown: This code runs after all tests in the session have finished
    print("\n--- Pytest session finished, cleaning up generated *.duckdb files ---")
    files = glob.glob("*.duckdb")
    for file in [f for f in files if f not in DO_NOT_REMOVE_LIST]:
        try:
            os.remove(file)
            print(f"Deleted generated file: {file}")
        except OSError as e:
            print(f"Error deleting file {file}: {e}")
