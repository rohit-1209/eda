from db.config import Database
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def rename_columns_in_copy(table_name, sheet_name, column_mappings):
    """Rename columns in the copy table based on provided mappings."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return {"error": "Database connection failed"}
            
        cursor = conn.cursor()

        copy_table = f"{table_name}_{sheet_name}_copy"  # Copy table name

        # Check existing columns in the copy table
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = %s
        """, (copy_table,))
        columns = [row[0] for row in cursor.fetchall()]

        # Validate columns
        for old_column in column_mappings.keys():
            if old_column not in columns:
                return {"error": f"Column '{old_column}' not found in table '{copy_table}'"}

        # Rename each column
        for old_column, new_column in column_mappings.items():
            cursor.execute(f'ALTER TABLE "{copy_table}" RENAME COLUMN "{old_column}" TO "{new_column}"')

        conn.commit()
        return {"message": f"Columns renamed successfully in copy table '{copy_table}'"}

    except Exception as e:
        logger.error(f"Error renaming columns: {str(e)}")
        if conn:
            conn.rollback()
        return {"error": str(e)}

    finally:
        db.close_cursor_and_connection(cursor, conn)
