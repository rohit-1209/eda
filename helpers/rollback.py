# rollback_helper_function

from db.config import Database
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def sync_table_structure(cursor, original_table, copy_table):
    """Synchronize the structure of the copy table with the original table."""
    try:
        # Get original table structure
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (original_table,))
        original_columns = cursor.fetchall()
        
        # Create dictionary of column types
        original_column_types = {col[0]: col[1] for col in original_columns}
        
        # Add or modify columns in copy table
        for column_name, data_type in original_columns:
            column_name_quoted = '"{}"'.format(column_name.replace('"', '""'))

            
            # Add column if it doesn't exist
            cursor.execute(f"""
                ALTER TABLE {copy_table}
                ADD COLUMN IF NOT EXISTS {column_name_quoted} {data_type};
            """)
            
            # Check and update column type if necessary
            cursor.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = %s AND column_name = %s
            """, (copy_table, column_name))
            copy_column_type = cursor.fetchone()
            
            if copy_column_type and copy_column_type[0] != data_type:
                cursor.execute(f"""
                    ALTER TABLE {copy_table}
                    ALTER COLUMN {column_name_quoted} TYPE {data_type}
                    USING {column_name_quoted}::{data_type};
                """)
        
        # Remove extra columns from copy table
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (copy_table,))
        copy_columns = [col[0] for col in cursor.fetchall()]
        
        for column_name in copy_columns:
            if column_name not in original_column_types:
                column_name_quoted = '"{}"'.format(column_name.replace('"', '""'))
                cursor.execute(f"ALTER TABLE {copy_table} DROP COLUMN IF EXISTS {column_name_quoted};")
                
        return True
    except Exception as e:
        logger.error(f"Error in sync_table_structure: {str(e)}")
        raise

def sync_data_from_original_to_copy(cursor, original_table, copy_table):
    """Copy data from the original table to the copy table."""
    try:
        # Clear existing data
        cursor.execute(f"TRUNCATE TABLE {copy_table}")
        
        # Get column names
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (original_table,))
        
        columns = [row[0] for row in cursor.fetchall()]
        logger.debug(f"Columns to sync: {columns}")
        
        column_str = ', '.join(f'"{col}"' for col in columns)
        
        if columns:
            # Copy data with specified columns
            cursor.execute(f"""
                INSERT INTO {copy_table} ({column_str})
                SELECT {column_str} FROM {original_table}
            """)
        
        return True
        
    except Exception as e:
        logger.error(f"Error in sync_data_from_original_to_copy: {str(e)}")
        raise

def sync_tables(table_name, sheet_name):
    """Synchronize the original table with its copy."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        # Create table names
        # original_table = f"{table_name}_{sheet_name}".lower()
        original_table = f"{table_name}".lower()
        # copy_table = f"{table_name}_{sheet_name}_copy".lower()
        copy_table = f"{table_name}_copy".lower()
        # Establish database connection
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        
        # Sync table structure and data
        sync_table_structure(cursor, original_table, copy_table)
        sync_data_from_original_to_copy(cursor, original_table, copy_table)
        
        # Commit changes
        conn.commit()
        
        return {
            "status": "success",
            "message": f"Successfully synced data from {original_table} to {copy_table}"
        }, None
        
    except Exception as e:
          logger.error(f"Error syncing tables: {str(e)}")
          return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)

