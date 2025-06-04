# datatype_helper_function

from db.config import Database
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def get_table_data_types(table_name, sheet_name):
    """
    Get the data types of columns in a table.
    
    Args:
        table_name (str): The name of the table
        sheet_name (str): The name of the sheet
        
    Returns:
        tuple: (result, error) where result contains column data types or None if error
    """
    db = Database()
    conn = None
    cursor = None
    
    try:
        # Generate possible table names to try
        possible_table_names = [
            f"{table_name.strip().lower()}_{sheet_name.lower()}",
            f"{table_name.strip().lower()}_{sheet_name}",
            table_name.strip().lower(),
        ]
        
        logger.info(f"Trying table names: {possible_table_names}")
        
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        
        # Find the correct table name
        table_exists = False
        actual_table_name = None
        
        for try_table_name in possible_table_names:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                    AND table_schema NOT IN ('pg_catalog', 'information_schema')
                );
            """, (try_table_name,))
            
            if cursor.fetchone()[0]:
                actual_table_name = try_table_name
                table_exists = True
                logger.info(f"Found existing table: {actual_table_name}")
                break
                
        if not table_exists:
            return None, f"Table not found. Tried: {', '.join(possible_table_names)}"
            
        # Get column data types
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position;
        """, (actual_table_name,))
        
        columns_info = cursor.fetchall()
        logger.debug(f"Found columns: {columns_info}")
        
        if not columns_info:
            return None, "No columns found in the table"
            
        # Create a dictionary of column names and data types
        data_types = {
            col_name: data_type 
            for col_name, data_type in columns_info
            if col_name != 'id'
        }
        
        return data_types, None
        
    except Exception as e:
        logger.error(f"Error getting table data types: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)
