# datatype_manage_helper_function

from db.config import Database
import logging
import json
from datetime import date, datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle date and numeric types."""
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.strftime('%Y-%m-%d')
        if isinstance(obj, (int, float)):
            return obj
        return super().default(obj)

def check_table_existence(table_name):
    """
    Check if a table exists and get its column information.
    
    Args:
        table_name (str): The name of the table to check
        
    Returns:
        tuple: (result, error) where result contains table info or None if error
    """
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table_name,))
        
        exists = cursor.fetchone()[0]
        
        # If table exists, get column information
        if exists:
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s;
            """, (table_name,))
            
            columns = cursor.fetchall()
            
            return {
                "exists": True,
                "table_name": table_name,
                "columns": [{"name": col[0], "type": col[1]} for col in columns]
            }, None
        
        return {
            "exists": False,
            "table_name": table_name
        }, None
        
    except Exception as e:
        logger.error(f"Error checking table existence: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)

def get_column_data_types(table_name, column):
    """
    Get the data type of a specific column.
    
    Args:
        table_name (str): The name of the table
        column (str): The name of the column
        
    Returns:
        tuple: (mapped_type, original_type, error) where types are None if error
    """
    db = Database()
    conn = None
    cursor = None
    
    try:
        data_type_map = {
            "text": "string",
            "character varying": "string",
            "varchar": "string",
            "integer": "int",
            "bigint": "int",
            "smallint": "int",
            "timestamp": "datetime",
            "timestamp without time zone": "datetime",
            "timestamp with time zone": "datetime",
            "date": "datetime",
            "double precision": "float",
            "numeric": "float",
            "real": "float"
        }
        
        conn = db.get_db_connection()
        if not conn:
            return None, None, "Database connection failed"
            
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = %s;
        """, (table_name, column))
        
        result = cursor.fetchone()
        if not result:
            return None, None, f"Column '{column}' not found in table '{table_name}'"
            
        original_type = result[0].lower()
        mapped_type = data_type_map.get(original_type, "string")  # Default to string if unknown
        
        return mapped_type, original_type, None
        
    except Exception as e:
        logger.error(f"Error getting column data type: {str(e)}")
        error = str(e)
        return None, None, error
        
    finally:
        db.close_cursor_and_connection(cursor, conn)

# def change_column_data_types(table_name, columns_to_change):
#     """
#     Change the data types of columns in a table.
    
#     Args:
#         table_name (str): The name of the table
#         columns_to_change (dict): Dictionary mapping column names to new data types
        
#     Returns:
#         tuple: (result, error) where result contains success info or None if error
#     """
#     db = Database()
#     conn = None
#     cursor = None
    
#     try:
#         conn = db.get_db_connection()
#         if not conn:
#             return None, "Database connection failed"
            
#         cursor = conn.cursor()
        
#         # Check if table exists
#         cursor.execute("""
#             SELECT EXISTS (
#                 SELECT FROM information_schema.tables 
#                 WHERE table_name = %s
#             );
#         """, (table_name,))
        
#         if not cursor.fetchone()[0]:
#             return None, f"Table '{table_name}' does not exist"
            
#         # Process each column
#         for column, new_type in columns_to_change.items():
#             try:
#                 logger.info(f"Changing column {column} to type {new_type}")
                
#                 # Get current data type
#                 mapped_type, current_type, error = get_column_data_types(table_name, column)
#                 if error:
#                     return None, error
                    
#                 logger.debug(f"Current type: {current_type}, mapped to: {mapped_type}")
                
#                 # Skip if already the correct type
#                 if new_type == mapped_type:
#                     logger.info(f"Column {column} is already type {new_type}")
#                     continue
                    
#                 # Convert to string
#                 if new_type == 'string':
#                     cursor.execute(f"""
#                         ALTER TABLE "{table_name}"
#                         ALTER COLUMN "{column}" TYPE TEXT USING "{column}"::TEXT;
#                     """)
                    
#                 # Convert to numeric types
#                 elif new_type in ['int', 'float']:
#                     # Check if direct conversion is possible
#                     if current_type in ['integer', 'bigint', 'smallint', 'double precision', 'numeric', 'real']:
#                         if new_type == 'int':
#                             cursor.execute(f"""
#                                 ALTER TABLE "{table_name}"
#                                 ALTER COLUMN "{column}" TYPE INTEGER
#                                 USING ROUND("{column}"::numeric);
#                             """)
#                         else:  # to float
#                             cursor.execute(f"""
#                                 ALTER TABLE "{table_name}"
#                                 ALTER COLUMN "{column}" TYPE NUMERIC
#                                 USING "{column}"::numeric;
#                             """)
#                     else:
#                         # Create a temporary column
#                         cursor.execute(f"""
#                             ALTER TABLE "{table_name}"
#                             ADD COLUMN temp_col TEXT;
#                         """)
                        
#                         # Copy and clean data to temporary column
#                         cursor.execute(f"""
#                             UPDATE "{table_name}"
#                             SET temp_col = CASE 
#                                 WHEN "{column}" IS NULL OR "{column}" = '' OR "{column}" = '-' THEN '0'
#                                 ELSE regexp_replace(
#                                     regexp_replace("{column}", '[^0-9.-]', '', 'g'),
#                                     '(-.*-.*)|(-.*)|[^0-9.-]', '0',
#                                     'g'
#                                 )
#                             END;
#                         """)
                        
#                         # Convert the cleaned data to numeric
#                         if new_type == 'int':
#                             cursor.execute(f"""
#                                 ALTER TABLE "{table_name}"
#                                 DROP COLUMN "{column}";
                                
#                                 ALTER TABLE "{table_name}"
#                                 ADD COLUMN "{column}" INTEGER;
                                
#                                 UPDATE "{table_name}"
#                                 SET "{column}" = CASE 
#                                     WHEN temp_col ~ '^[-]?[0-9]*\\.?[0-9]+$' 
#                                     THEN ROUND(temp_col::NUMERIC)::INTEGER
#                                     ELSE 0 
#                                 END;
#                             """)
#                         else:  # float
#                             cursor.execute(f"""
#                                 ALTER TABLE "{table_name}"
#                                 DROP COLUMN "{column}";
                                
#                                 ALTER TABLE "{table_name}"
#                                 ADD COLUMN "{column}" NUMERIC;
                                
#                                 UPDATE "{table_name}"
#                                 SET "{column}" = CASE 
#                                     WHEN temp_col ~ '^[-]?[0-9]*\\.?[0-9]+$' 
#                                     THEN temp_col::NUMERIC
#                                     ELSE 0 
#                                 END;
#                             """)
                            
#                         # Drop temporary column
#                         cursor.execute(f"""
#                             ALTER TABLE "{table_name}"
#                             DROP COLUMN temp_col;
#                         """)
                        
#                 # Convert to datetime
#                 elif new_type == 'datetime':
#                     cursor.execute(f"""
#                         ALTER TABLE "{table_name}"
#                         ALTER COLUMN "{column}" TYPE TIMESTAMP
#                         USING CASE
#                             WHEN "{column}" IS NULL THEN '1900-01-01'::TIMESTAMP
#                             ELSE "{column}"::TIMESTAMP
#                         END;
#                     """)
                    
#                 conn.commit()
                
#             except Exception as e:
#                 conn.rollback()
#                 logger.error(f"Error processing column {column}: {str(e)}")
#                 return None, f"Error converting {column}: {str(e)}"
                
#         return {
#             "message": "Data types updated successfully",
#             "updated_columns": columns_to_change
#         }, None
        
#     except Exception as e:
#         logger.error(f"Error changing column data types: {str(e)}")
#         return db.handle_error(conn, e)
        
#     finally:
#         db.close_cursor_and_connection(cursor, conn)

def change_column_data_types(table_name, columns_to_change):
    """
    Change the data types of columns in a table.

    Args:
        table_name (str): The name of the table
        columns_to_change (dict): Dictionary mapping column names to new data types

    Returns:
        tuple: (result, error) where result contains success info or None if error
    """
    db = Database()
    conn = None
    cursor = None

    try:
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"

        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table_name,))

        if not cursor.fetchone()[0]:
            return None, f"Table '{table_name}' does not exist"

        # Process each column
        for column, new_type in columns_to_change.items():
            try:
                logger.info(f"Changing column {column} to type {new_type}")

                # Get current data type
                mapped_type, current_type, error = get_column_data_types(table_name, column)
                if error:
                    return None, error

                logger.debug(f"Current type: {current_type}, mapped to: {mapped_type}")

                # Skip if already the correct type
                if new_type == mapped_type:
                    logger.info(f"Column {column} is already type {new_type}")
                    continue

                if new_type == 'string':
                    cursor.execute(f"""
                        ALTER TABLE "{table_name}"
                        ALTER COLUMN "{column}" TYPE TEXT USING "{column}"::TEXT;
                    """)

                elif new_type in ['int', 'float']:
                    if current_type in ['integer', 'bigint', 'smallint', 'double precision', 'numeric', 'real']:
                        if new_type == 'int':
                            cursor.execute(f"""
                                ALTER TABLE "{table_name}"
                                ALTER COLUMN "{column}" TYPE INTEGER
                                USING ROUND("{column}"::numeric);
                            """)
                        else:
                            cursor.execute(f"""
                                ALTER TABLE "{table_name}"
                                ALTER COLUMN "{column}" TYPE NUMERIC
                                USING "{column}"::numeric;
                            """)
                    else:
                        cursor.execute(f"""
                            ALTER TABLE "{table_name}"
                            ADD COLUMN temp_col TEXT;
                        """)

                        cursor.execute(f"""
                            UPDATE "{table_name}"
                            SET temp_col = CASE 
                                WHEN "{column}" IS NULL OR "{column}" = '' OR "{column}" = '-' THEN '0'
                                ELSE regexp_replace(
                                    regexp_replace("{column}", '[^0-9.-]', '', 'g'),
                                    '(-.*-.*)|(-.*)|[^0-9.-]', '0',
                                    'g'
                                )
                            END;
                        """)

                        if new_type == 'int':
                            cursor.execute(f"""
                                ALTER TABLE "{table_name}" DROP COLUMN "{column}";
                                ALTER TABLE "{table_name}" ADD COLUMN "{column}" INTEGER;
                                UPDATE "{table_name}" SET "{column}" = CASE 
                                    WHEN temp_col ~ '^[-]?[0-9]*\\.?[0-9]+$' 
                                    THEN ROUND(temp_col::NUMERIC)::INTEGER
                                    ELSE 0 
                                END;
                            """)
                        else:
                            cursor.execute(f"""
                                ALTER TABLE "{table_name}" DROP COLUMN "{column}";
                                ALTER TABLE "{table_name}" ADD COLUMN "{column}" NUMERIC;
                                UPDATE "{table_name}" SET "{column}" = CASE 
                                    WHEN temp_col ~ '^[-]?[0-9]*\\.?[0-9]+$' 
                                    THEN temp_col::NUMERIC
                                    ELSE 0 
                                END;
                            """)

                        cursor.execute(f"""
                            ALTER TABLE "{table_name}" DROP COLUMN temp_col;
                        """)

                elif new_type == 'datetime':
                    cursor.execute(f"""
                        ALTER TABLE "{table_name}"
                        ALTER COLUMN "{column}" TYPE TIMESTAMP
                        USING CASE
                            WHEN "{column}" IS NULL THEN NULL
                            WHEN "{column}" ~ '^[0-9]+(\\.[0-9]+)?$' THEN 
                                '1899-12-30'::TIMESTAMP + ("{column}"::FLOAT * INTERVAL '1 day')
                            ELSE
                                "{column}"::TIMESTAMP
                        END;
                    """)

                conn.commit()

            except Exception as e:
                conn.rollback()
                logger.error(f"Error processing column {column}: {str(e)}")
                return None, f"Error converting {column}: {str(e)}"

        return {
            "message": "Data types updated successfully",
            "updated_columns": columns_to_change
        }, None

    except Exception as e:
        logger.error(f"Error changing column data types: {str(e)}")
        return db.handle_error(conn, e)

    finally:
        db.close_cursor_and_connection(cursor, conn)
