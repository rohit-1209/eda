# handling_filling_helper_function

import pandas as pd
from db.config import Database
from io import StringIO
import logging

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
        
        # Convert DataFrame to CSV in memory
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        
        # Use COPY command for bulk insert
        cursor.copy_from(output, temp_table, null='\\N', columns=df.columns)
        
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
    """Handle missing data in a table by filling or removing."""
    try:
        logger.info(f"Handling missing data for table: {table_name}")
        logger.info(f"Action: {action}, Method: {method}, Columns: {columns}")
        
        # Get table data
        df, actual_table_name, column_types = get_table_data(table_name)
        
        if action == 'remove':
            logger.info("Performing row removal for missing values")
            original_rows = len(df)
            df = df.dropna(subset=columns)
            rows_removed = original_rows - len(df)
            
            update_table_data(df, actual_table_name, column_types)
            return {
                "message": f"Successfully removed {rows_removed} rows with missing values",
                "rows_remaining": len(df)
            }, None
            
        elif action == 'fill':
            logger.info("Performing fill operation for missing values")
            if not method:
                return None, "Method parameter is required for fill action"
                
            if method not in ['mean', 'median', 'mode', 'bfill', 'ffill', 'zero']:
                return None, "Invalid fill method"
            
            pandas_column_types = get_column_types(df)
            
            # Filter numeric columns
            numeric_cols = [col for col in columns if pandas_column_types.get(col) == 'numeric']
            non_numeric_cols = [col for col in columns if col not in numeric_cols]
            
            if non_numeric_cols:
                return None, f"Cannot apply numeric operations to non-numeric columns: {', '.join(non_numeric_cols)}"
            
            if not numeric_cols:
                return None, "No numeric columns selected"
            
            # Check if there are any missing values
            missing_data = df[numeric_cols].isna().sum()
            missing_columns = missing_data[missing_data > 0]
            
            if missing_columns.empty:
                return {
                    "message": f"No missing values in columns {', '.join(numeric_cols)} to fill",
                    "columns_processed": numeric_cols
                }, None
            
            logger.info(f"Applying {method} method to columns: {numeric_cols}")
            for column in numeric_cols:
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
            
            update_table_data(df, actual_table_name, column_types)
            
            return {
                "message": f"Successfully filled missing values using {method}",
                "columns_processed": numeric_cols
            }, None
            
        else:
            return None, f"Invalid action: {action}"
            
    except Exception as e:
        logger.error(f"Error handling missing data: {str(e)}")
        return None, str(e)
