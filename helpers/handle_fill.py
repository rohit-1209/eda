import pandas as pd
from db.config import Database
from io import StringIO
import logging
import csv
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def get_table_data(table_name):
    """Get data from a table as a pandas DataFrame."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            raise Exception("Database connection failed")
            
        cursor = conn.cursor()
        
        logger.info(f"Attempting to find table: {table_name}")
        
        # Check if table exists (case-insensitive)
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name ILIKE %s
            AND table_schema NOT IN ('pg_catalog', 'information_schema');
        """, (table_name,))
        
        result = cursor.fetchone()
        if not result:
            raise Exception(f"Table '{table_name}' does not exist")
            
        # Use the actual table name from the database
        actual_table_name = result[0]
        
        logger.info(f"Requested table name: {table_name}")
        logger.info(f"Actual table name in database: {actual_table_name}")
        
        # Get column types
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s;
        """, (actual_table_name,))
        column_types = {col: dtype for col, dtype in cursor.fetchall()}
        logger.debug(f"Column types: {column_types}")
        
        # Get data
        cursor.execute(f'SELECT * FROM "{actual_table_name}"')
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        df = pd.DataFrame(data, columns=columns)
        logger.info(f"Retrieved {len(df)} rows from table")
        
        return df, actual_table_name, column_types
        
    except Exception as e:
        logger.error(f"Error in get_table_data: {str(e)}")
        raise e
    finally:
        db.close_cursor_and_connection(cursor, conn)

def update_table_data(df, table_name, column_types):
    """Update a table with new data from a DataFrame."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            raise Exception("Database connection failed")
            
        cursor = conn.cursor()
        
        temp_table = f"{table_name}_temp"
        cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        
        # Create columns
        columns = [f'"{col}" {column_types.get(col, "TEXT")}' for col in df.columns]
        create_query = f'CREATE TABLE "{temp_table}" ({", ".join(columns)})'
        cursor.execute(create_query)
        
        # Convert DataFrame to native Python types
        # This is crucial for handling numpy types like int64, float64, etc.
        df_copy = df.copy()
        
        # Convert all numpy int and float types to Python native types
        for col in df_copy.select_dtypes(include=['int', 'float']).columns:
            df_copy[col] = df_copy[col].astype(object)
        
        # Replace NaN/None values with SQL NULL
        df_copy = df_copy.replace({pd.NA: None, float('nan'): None})
        
        # Insert data row by row to avoid type issues
        columns_str = ', '.join([f'"{col}"' for col in df_copy.columns])
        placeholders = ', '.join(['%s'] * len(df_copy.columns))
        insert_query = f'INSERT INTO "{temp_table}" ({columns_str}) VALUES ({placeholders})'
        
        # Convert DataFrame to list of tuples for insertion
        data_tuples = [tuple(row) for row in df_copy.values]
        
        # Execute batch insert
        cursor.executemany(insert_query, data_tuples)
        
        # Swap tables
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cursor.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')
        
        conn.commit()   
        return True
        
    except Exception as e:
        logger.error(f"Error in update_table_data: {str(e)}")
        if conn:
            conn.rollback()
        raise e
    finally:
        db.close_cursor_and_connection(cursor, conn)

def get_column_types(df):
    """Determine the data type of each column in a DataFrame."""
    column_types = {}
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            column_types[column] = 'numeric'
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            column_types[column] = 'datetime'
        else:
            column_types[column] = 'string'
    return column_types


def handle_missing_data(table_name, columns, action, method=None):
    try:
        logger.info(f"Handling missing data for table: {table_name}")

        db = Database()
        conn = db.get_db_connection()
        cursor = conn.cursor()

        
        copy_table_name = f"{table_name}_copy"

       
        cursor.execute(f'DROP TABLE IF EXISTS "{copy_table_name}"')

        
        cursor.execute(f'CREATE TABLE "{copy_table_name}" AS TABLE "{table_name}"')

        conn.commit() 

       
        df, actual_table_name, column_types = get_table_data(copy_table_name)

        if action == 'remove':
            original_rows = len(df)
            df = df.dropna(subset=columns)
            rows_removed = original_rows - len(df)

            update_table_data(df, copy_table_name, column_types)
            return {
                "message": f"Successfully removed {rows_removed} rows with missing values in columns: {', '.join(columns)}",
                "rows_remaining": len(df),
                "table_name": copy_table_name  
            }, None

        elif action == 'fill':
            if not method:
                return None, "Method parameter is required for fill action"

            if method not in ['mean', 'median', 'mode', 'bfill', 'ffill', 'zero']:
                return None, f"Invalid fill method: {method}"

            pandas_column_types = get_column_types(df)

            numeric_cols = [col for col in columns if pandas_column_types.get(col) == 'numeric']
            non_numeric_cols = [col for col in columns if pandas_column_types.get(col) != 'numeric']

            if non_numeric_cols:
                return None, f"Cannot apply numeric fill methods to non-numeric columns: {', '.join(non_numeric_cols)}"

            if not numeric_cols:
                return None, "No numeric columns selected for filling"

            missing_columns = df[numeric_cols].isna().sum()
            columns_to_fill = missing_columns[missing_columns > 0].index.tolist()

            if not columns_to_fill:
                return {
                    "message": f"No missing values to fill in selected numeric columns: {', '.join(numeric_cols)}",
                    "columns_processed": numeric_cols,
                    "table_name": copy_table_name  
                }, None

            for column in columns_to_fill:
                if method == 'mean':
                    df[column].fillna(df[column].mean(), inplace=True)
                elif method == 'median':
                    df[column].fillna(df[column].median(), inplace=True)
                elif method == 'mode':
                    df[column].fillna(df[column].mode().iloc[0], inplace=True)
                elif method == 'bfill':
                    df[column].fillna(method='bfill', inplace=True)
                elif method == 'ffill':
                    df[column].fillna(method='ffill', inplace=True)
                elif method == 'zero':
                    df[column].fillna(0, inplace=True)

            update_table_data(df, copy_table_name, column_types)

            return {
                "message": f"Successfully filled missing values using '{method}' in columns: {', '.join(columns_to_fill)}",
                "columns_processed": columns_to_fill,
                "table_name": copy_table_name  
            }, None

        else:
            return None, f"Invalid action: {action}"

    except Exception as e:
        logger.error(f"Error handling missing data: {str(e)}")
        return None, str(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    
