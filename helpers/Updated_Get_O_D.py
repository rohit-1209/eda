# Get.py
import psycopg2
from db.config import Database
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def Gets_Data(table_name):
    db = Database()
    conn = None
    cursor = None
    try:
        # Remove extra spaces and ensure lowercase
        table_name = table_name.strip().lower()
        
        # Ensure we're using the correct table name (with _copy suffix)
        if not table_name.endswith('_copy'):
            table_name = f"{table_name}_copy"
        
        conn = db.get_db_connection()
        if not conn:
            return {"error": "Database connection failed"}, 500

        cursor = conn.cursor()
        
        # Debug: Print the table name being queried
        logger.debug(f"Attempting to query table: {table_name}")
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table_name,))
        
        exists = cursor.fetchone()[0]
        if not exists:
            logger.warning(f"Table {table_name} does not exist")
            return {"error": f"Table {table_name} does not exist"}, 404

        # Get the data from the table, excluding the 'id' column
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND column_name != 'id'
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = [col[0] for col in cursor.fetchall()]
        
        # Construct the query with quoted column names
        columns_str = ', '.join(f'"{col}"' for col in columns)
        query = f'SELECT {columns_str} FROM "{table_name}";'
        logger.debug(f"Executing query: {query}")
        cursor.execute(query)
        
        # Fetch data
        data = cursor.fetchall()
        
        # Convert data to list of dictionaries
        result = []
        for row in data:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            result.append(row_dict)

        logger.info(f"Successfully retrieved {len(result)} rows")
        return result, columns

    except Exception as e:
        logger.error(f"Error retrieving data: {str(e)}")
        return {"error": f"Database error: {str(e)}"}, 500
        
    finally:
        db.close_cursor_and_connection(cursor, conn)
