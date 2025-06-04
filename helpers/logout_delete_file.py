# logout_delete_file_helper_function

from db.config import Database
import logging
from io import StringIO

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def delete_data_from_table(table_name, sheet_names):
    """Delete tables that match the given table_name pattern."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            logger.error("Database connection failed")
            return False
            
        cursor = conn.cursor()

        # Query to find tables matching the pattern
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE %s
            AND table_schema NOT IN ('pg_catalog', 'information_schema')
        """
        cursor.execute(query, (f"{table_name}%",))  # Use a wildcard for matching tables starting with `table_name`
        tables_to_drop = cursor.fetchall()
        logger.info(f"Found tables to drop: {tables_to_drop}")

        if tables_to_drop:
            # Use StringIO to construct the DROP TABLE query efficiently
            query_builder = StringIO()
            query_builder.write("DROP TABLE IF EXISTS ")

            # Collect all table names in the query
            for i, row in enumerate(tables_to_drop):
                table_name_with_sheet = row[0]
                query_builder.write(f'"{table_name_with_sheet}"')  # Quote table names for safety
                if i < len(tables_to_drop) - 1:
                    query_builder.write(", ")  # Add a comma between table names if it's not the last one
            
            # Convert the StringIO content to a string
            drop_query = query_builder.getvalue()

            logger.info(f"Executing drop query: {drop_query}")
            cursor.execute(drop_query)
            conn.commit()

            logger.info(f"All tables starting with {table_name} have been dropped.")
            return True
        else:
            logger.warning(f"No tables found starting with {table_name}")
            return False

    except Exception as e:
        logger.error(f"Error deleting tables for {table_name}: {str(e)}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        db.close_cursor_and_connection(cursor, conn)
