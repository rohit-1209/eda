# remove_duplicate_row_helper function

from full_test_autoeda.autoeda_back_flask.db.config import Database
import pandas as pd
from io import StringIO

def remove_duplicates_from_table(table_name, duplicate_columns):
    db = Database()
    conn = None
    try:
        conn = db.get_db_connection()
        
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        
        # Fetch data
        cursor.execute(f'SELECT * FROM "{table_name}";')
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=colnames)

        # Remove duplicates based on specified columns
        df_deduped = df.drop_duplicates(subset=duplicate_columns, keep='first')

        # Clear table and insert deduplicated data
        cursor.execute(f'TRUNCATE TABLE "{table_name}";')
        output = StringIO()
        df_deduped.to_csv(output, index=False, header=False)
        output.seek(0)
        cursor.copy_expert(f'COPY "{table_name}" FROM STDIN WITH (FORMAT csv)', output)
        conn.commit()
        cursor.close()
        
        return {
            "original_count": len(df),
            "deduped_count": len(df_deduped)
        }, None
        
    except Exception as e:
        return db.handle_error(conn, e)
        
    finally:
        db.close_connection(conn)
