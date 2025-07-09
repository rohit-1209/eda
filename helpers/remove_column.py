#remove_column_helper_function

from db.config import Database
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def get_remaining_columns(table_name, cursor, schema='postgres'):
    """Fetch remaining columns from the specified table and schema."""
    try:
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND table_schema = %s
            AND column_name != 'id'
            ORDER BY ordinal_position;
        """, (table_name, schema))
        
        columns = [row[0] for row in cursor.fetchall()]
        return {"columns": columns}
    except Exception as e:
        logger.error(f"Error fetching columns: {e}")
        return {"columns": []}

# def verify_table_exists(cursor, table_name):
#     """Verify if the table exists and return all available tables if it doesn't."""
#     cursor.execute("""
#         SELECT table_name 
#         FROM information_schema.tables 
#         WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
#     """)
#     available_tables = [row[0] for row in cursor.fetchall()]
#     return table_name in available_tables, available_tables

def verify_table_exists(cursor, table_name):
    """Verify if the table exists and return all available tables if it doesn't."""
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'postgres';
    """)
    available_tables = [row[0] for row in cursor.fetchall()]
    return table_name in available_tables, available_tables

def remove_columns(original_table_name, sheet_name, columns_to_remove):
    """Remove specified columns from a table."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        # Create the table name with sheet name
        table_name = f"{original_table_name}_{sheet_name}_copy".lower()
        
        logger.info(f"Removing columns from table: {table_name}")
        logger.info(f"Columns to remove: {columns_to_remove}")
        
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        
        # Verify table exists
        table_exists, available_tables = verify_table_exists(cursor, table_name)
        
        if not table_exists:
            return None, f"Table '{table_name}' does not exist. Available tables: {available_tables}"

        # Construct the SQL command to remove columns
        for column in columns_to_remove:
            logger.info(f"Attempting to remove column: {column}")
            cursor.execute(f'ALTER TABLE "{table_name}" DROP COLUMN IF EXISTS "{column}";')

        # Get remaining columns
        remaining_columns = get_remaining_columns(table_name, cursor)

        conn.commit()
        return {
            "message": "Columns removed successfully",
            "remaining_columns": remaining_columns["columns"]
        }, None

    except Exception as e:
        logger.error(f"Error removing columns: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)