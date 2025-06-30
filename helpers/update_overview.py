# updated_overview_helper_function

from full_test_autoeda.autoeda_back_flask.db.config import Database

def get_column_types_from_db(table_name):
    """Get the column types directly from PostgreSQL schema."""
    db = Database()
    conn = None
    cursor = None
    try:
        # Ensure we're using the correct table name (with _copy suffix)
        if not table_name.endswith('_copy'):
            table_name = f"{table_name}_copy"
            
        conn = db.get_db_connection()
        if not conn:
            raise Exception("Database connection failed")
            
        cursor = conn.cursor()
        
        # Query to get column data types from PostgreSQL schema
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema NOT IN ('pg_catalog', 'information_schema')
        """, (table_name,))
        
        columns_info = cursor.fetchall()
        
        column_types = {
            'numerical': [],
            'categorical': [],
            'dates': []
        }
        
        # Mapping PostgreSQL data types to categories
        for column_name, data_type in columns_info:
            if data_type in ['integer', 'double precision', 'numeric', 'bigint', 'smallint']:
                column_types['numerical'].append(column_name)
            elif data_type in ['character varying', 'text', 'char', 'varchar']:
                column_types['categorical'].append(column_name)
            elif data_type in ['timestamp without time zone', 'date']:
                column_types['dates'].append(column_name)
            else:
                column_types['categorical'].append(column_name)  # Treat other types as categorical
        
        # Remove the 'id' column from numerical columns if it exists
        if 'id' in column_types['numerical']:
            column_types['numerical'].remove('id')
            
        return column_types, None
        
    except Exception as e:
        return None, str(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            db.close_connection(conn)
