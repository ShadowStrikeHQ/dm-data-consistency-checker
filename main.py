import argparse
import logging
import sqlite3
import sys
from typing import List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def setup_argparse() -> argparse.ArgumentParser:
    """
    Sets up the argument parser for the command-line interface.

    Returns:
        argparse.ArgumentParser: The argument parser object.
    """
    parser = argparse.ArgumentParser(
        description="Verifies referential integrity across masked datasets."
    )
    parser.add_argument(
        "--db_path1",
        required=True,
        help="Path to the first SQLite database file.",
    )
    parser.add_argument(
        "--db_path2",
        required=True,
        help="Path to the second SQLite database file (masked version).",
    )
    parser.add_argument(
        "--table_name",
        required=True,
        help="Name of the table to check for referential integrity.",
    )
    parser.add_argument(
        "--foreign_key_column",
        required=True,
        help="Name of the foreign key column in the table.",
    )
    parser.add_argument(
        "--parent_table",
        required=True,
        help="Name of the parent table referenced by the foreign key.",
    )
    parser.add_argument(
        "--parent_key_column",
        required=True,
        help="Name of the primary key column in the parent table.",
    )

    return parser


def execute_query(db_path: str, query: str) -> List[Tuple]:
    """
    Executes an SQL query against the specified database and returns the results.

    Args:
        db_path (str): Path to the SQLite database file.
        query (str): The SQL query to execute.

    Returns:
        List[Tuple]: A list of tuples representing the query results.  Empty list if query fails.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        return results
    except sqlite3.Error as e:
        logging.error(f"Error executing query on {db_path}: {e}")
        return []


def check_referential_integrity(
    db_path1: str,
    db_path2: str,
    table_name: str,
    foreign_key_column: str,
    parent_table: str,
    parent_key_column: str,
) -> bool:
    """
    Checks referential integrity between two databases after masking.

    Args:
        db_path1 (str): Path to the original database.
        db_path2 (str): Path to the masked database.
        table_name (str): Name of the table with the foreign key.
        foreign_key_column (str): Name of the foreign key column.
        parent_table (str): Name of the parent table.
        parent_key_column (str): Name of the parent table's primary key column.

    Returns:
        bool: True if referential integrity is maintained, False otherwise.
    """

    # Build the query to find orphaned foreign keys in the masked database
    query = f"""
    SELECT COUNT(*)
    FROM {table_name} t
    LEFT JOIN {parent_table} p ON t.{foreign_key_column} = p.{parent_key_column}
    WHERE t.{foreign_key_column} IS NOT NULL AND p.{parent_key_column} IS NULL;
    """

    results = execute_query(db_path2, query)

    if not results:
        logging.error("Failed to execute the query to check referential integrity.")
        return False

    orphaned_count = results[0][0]

    if orphaned_count > 0:
        logging.warning(f"Referential integrity check failed. Found {orphaned_count} orphaned foreign keys.")

        # Optionally, log some examples of orphaned keys for further investigation.
        example_query = f"""
            SELECT t.{foreign_key_column}
            FROM {table_name} t
            LEFT JOIN {parent_table} p ON t.{foreign_key_column} = p.{parent_key_column}
            WHERE t.{foreign_key_column} IS NOT NULL AND p.{parent_key_column} IS NULL
            LIMIT 5;
        """
        example_results = execute_query(db_path2, example_query)
        if example_results:
            logging.warning(f"Example orphaned keys: {example_results}")
        return False

    logging.info("Referential integrity check passed.")
    return True


def validate_database_path(db_path: str) -> bool:
    """
    Validates if the provided database path exists.

    Args:
        db_path (str): Path to the database file.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    import os

    if not os.path.exists(db_path):
        logging.error(f"Database file not found: {db_path}")
        return False
    return True


def main():
    """
    Main function to execute the data consistency checker.
    """
    parser = setup_argparse()
    args = parser.parse_args()

    # Validate input arguments (database paths)
    if not validate_database_path(args.db_path1):
        sys.exit(1)
    if not validate_database_path(args.db_path2):
        sys.exit(1)

    # Validate input arguments (table and column names). Ideally, these would
    # be validated against the schema of the database files, but that requires
    # significantly more complex code, so we're skipping it here for simplicity,
    # in favor of letting sqlite3 raise an error if a table or column does not exist.

    try:
        if check_referential_integrity(
            args.db_path1,
            args.db_path2,
            args.table_name,
            args.foreign_key_column,
            args.parent_table,
            args.parent_key_column,
        ):
            print("Data consistency check passed.")
        else:
            print("Data consistency check failed.")
            sys.exit(1)  # Exit with an error code
    except Exception as e:
        logging.exception("An unexpected error occurred:")
        print(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Example usage:
    #   python main.py --db_path1 original.db --db_path2 masked.db --table_name orders --foreign_key_column customer_id --parent_table customers --parent_key_column id

    # Create dummy SQLite databases (for testing)
    # These are just examples and should NOT be included in production code.
    # This demonstrates how to use the script.  Remove these lines in a production context.
    import os

    def create_dummy_db(db_path: str):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create customers table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
            """
        )
        cursor.executemany(
            "INSERT INTO customers (id, name) VALUES (?, ?)",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        )

        # Create orders table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                amount REAL,
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            )
            """
        )
        cursor.executemany(
            "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
            [(101, 1, 100.00), (102, 2, 200.00), (103, 1, 150.00)],
        )

        conn.commit()
        conn.close()

    if not os.path.exists("original.db"):
        create_dummy_db("original.db")

    if not os.path.exists("masked.db"):
        create_dummy_db("masked.db")
        # Simulate masking by removing a customer and their order
        conn = sqlite3.connect("masked.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM orders WHERE customer_id = 3")
        cursor.execute("DELETE FROM customers WHERE id = 3")  # Create an orphaned order.
        conn.commit()
        conn.close()

    main()