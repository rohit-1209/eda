# feature_engineering_helper_function

from full_test_autoeda.autoeda_back_flask.db.config import Database
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def verify_table_exists(table_name):
    """Verify if a table exists in the database."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table_name,))
        
        return cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error verifying table existence: {str(e)}")
        return False
        
    finally:
        db.close_cursor_and_connection(cursor, conn)

def get_categorical_columns(table_name):
    """Get all categorical columns from a table."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND data_type IN ('character varying', 'text', 'varchar', 'character');
        """, (table_name,))
        
        return [row[0] for row in cursor.fetchall()], None
        
    except Exception as e:
        logger.error(f"Error getting categorical columns: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)

def split_categorical_columns(table_name, columns_config):
    """Split categorical columns based on delimiter."""
    
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        processed_columns = []
        
        for split_config in columns_config:
            column = split_config['column']
            delimiter = split_config['delimiter']
            processed_columns.append(column)
            
            # Get maximum number of parts
            cursor.execute(f"""
                SELECT MAX(ARRAY_LENGTH(STRING_TO_ARRAY("{column}", %s), 1))
                FROM "{table_name}";
            """, (delimiter,))
            
            max_parts = cursor.fetchone()[0] or 0
            
            # Generate new column names
            new_columns = [f"{column}_{i}" for i in range(1, max_parts + 1)]
            
            for idx, new_col in enumerate(new_columns, 1):
                cursor.execute(f"""
                    ALTER TABLE "{table_name}"
                    ADD COLUMN IF NOT EXISTS "{new_col}" TEXT;
                    
                    UPDATE "{table_name}"
                    SET "{new_col}" = TRIM(SPLIT_PART("{column}", %s, {idx}));
                """, (delimiter,))
        
        conn.commit()
        return processed_columns, None
        
    except Exception as e:
        logger.error(f"Error splitting categorical columns: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)

def one_hot_encode_columns(table_name, columns):
    """One-hot encode categorical columns."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        
        for column in columns:
            cursor.execute(f"""
                SELECT DISTINCT "{column}"
                FROM "{table_name}"
                WHERE "{column}" IS NOT NULL
                ORDER BY "{column}";
            """)
            
            unique_values = [row[0] for row in cursor.fetchall()]
            
            for value in unique_values:
                safe_col_name = f"{column}_{value}".replace(' ', '_').lower()
                cursor.execute(f"""
                    ALTER TABLE "{table_name}"
                    ADD COLUMN IF NOT EXISTS "{safe_col_name}" INTEGER;
                    
                    UPDATE "{table_name}"
                    SET "{safe_col_name}" = CASE 
                        WHEN "{column}" = %s THEN 1 
                        ELSE 0 
                    END;
                """, (value,))
        
        conn.commit()
        return columns, None
        
    except Exception as e:
        logger.error(f"Error one-hot encoding columns: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)

def concatenate_columns(table_name, concat_configs):
    """Concatenate multiple columns into a new column."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        new_columns = []
        
        for concat_config in concat_configs:
            source_cols = concat_config['columns']
            new_column = concat_config['new_column']
            delimiter = concat_config.get('delimiter', ' ')
            new_columns.append(new_column)
            
            # Build concatenation expression
            concat_parts = [f'COALESCE("{col}"::TEXT, \'\')' for col in source_cols]
            concat_expr = f" || '{delimiter}' || ".join(concat_parts)
            
            # Execute concatenation
            cursor.execute(f"""
                ALTER TABLE "{table_name}"
                ADD COLUMN IF NOT EXISTS "{new_column}" TEXT;
                
                UPDATE "{table_name}"
                SET "{new_column}" = {concat_expr};
            """)
        
        conn.commit()
        return new_columns, None
        
    except Exception as e:
        logger.error(f"Error concatenating columns: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)

def process_categorical_features(table_name, sheet_name, operation, columns):
    """Process categorical features based on the specified operation."""
    try:
        full_table_name = f"{table_name}_{sheet_name}_copy".lower()
        
        # Verify table exists
        if not verify_table_exists(full_table_name):
            return None, f"Table {full_table_name} not found"
        
        # Get categorical columns
        categorical_cols, error = get_categorical_columns(full_table_name)
        if error:
            return None, error
        
        if operation == "split":
            column_names = [item['column'] for item in columns]
            invalid_cols = [col for col in column_names if col not in categorical_cols]
            
            if invalid_cols:
                return None, {
                    "error": f"Columns {invalid_cols} are not categorical",
                    "available_categorical_columns": categorical_cols
                }
            
            processed_columns, error = split_categorical_columns(full_table_name, columns)
            if error:
                return None, error
                
            return {
                "message": "Categorical split completed successfully",
                "table_name": full_table_name,
                "processed_columns": processed_columns
            }, None
            
        elif operation == "one_hot":
            invalid_cols = [col for col in columns if col not in categorical_cols]
            
            if invalid_cols:
                return None, {
                    "error": f"Columns {invalid_cols} are not categorical",
                    "available_categorical_columns": categorical_cols
                }
            
            processed_columns, error = one_hot_encode_columns(full_table_name, columns)
            if error:
                return None, error
                
            return {
                "message": "One-hot encoding completed successfully",
                "table_name": full_table_name,
                "processed_columns": processed_columns
            }, None
            
        elif operation == "concatenate":
            for concat_config in columns:
                source_cols = concat_config['columns']
                invalid_cols = [col for col in source_cols if col not in categorical_cols]
                
                if invalid_cols:
                    return None, {
                        "error": f"Columns {invalid_cols} are not categorical",
                        "available_categorical_columns": categorical_cols
                    }
            
            new_columns, error = concatenate_columns(full_table_name, columns)
            if error:
                return None, error
                
            return {
                "message": "Concatenation completed successfully",
                "table_name": full_table_name,
                "processed_columns": new_columns
            }, None
            
        else:
            return None, f"Unsupported operation: {operation}"
            
    except Exception as e:
        logger.error(f"Error processing categorical features: {str(e)}")
        return None, str(e)
