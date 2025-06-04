# upload_insert_data_db_helper_function
import pandas as pd
from io import BytesIO, StringIO
from db.config import Database

def get_sql_type(dtype):
    """Convert pandas dtype to SQL type."""
    if pd.api.types.is_integer_dtype(dtype):
        return 'BIGINT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def insert_data_from_excel(file, original_table_name, copy_table_name, sheetname):
    print("Starting data import...")
    db = Database()
    conn = None
    cursor = None
    
    try:
        filename = file.filename.lower()
        
        if filename.endswith('.csv'):
            df = pd.read_csv(file)  # Read CSV directly
            sheetname = "csv_import"
        else:
            # Read Excel file
            df = pd.read_excel(BytesIO(file.read()), sheet_name=sheetname)

        # Rename id column if it exists
        if 'id' in df.columns:
            df = df.rename(columns={'id': 'ID'})
            
        print(f"Read {len(df)} rows from Excel")

        # Clean data
        df = df.replace({pd.NaT: None})
        df = df.where(pd.notnull(df), None)

        # Connect to database
        conn = db.get_db_connection()
        if not conn:
            raise Exception("Database connection failed")
        cursor = conn.cursor()

        # Create original table
        cursor.execute(f"DROP TABLE IF EXISTS {original_table_name} CASCADE;")
        
        columns = ', '.join([f'"{col}" {get_sql_type(df[col].dtype)}' for col in df.columns])
        create_table_sql = f"""
            CREATE TABLE {original_table_name} (
                id SERIAL PRIMARY KEY,
                {columns}
            );
        """
        cursor.execute(create_table_sql)
        print("Table created successfully")

        # Prepare data for COPY
        output = StringIO()
        df_copy = df.copy()
        
        # Convert data to appropriate format
        for col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(lambda x: None if pd.isna(x) 
                                            else x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, pd.Timestamp) 
                                            else str(x))
        
        df_copy.to_csv(output, index=False, header=False, sep=',', na_rep='\\N')
        output.seek(0)

        # Use COPY command
        copy_sql = f"""
            COPY {original_table_name} ({','.join([f'"{col}"' for col in df.columns])})
            FROM STDIN WITH (FORMAT CSV, NULL '\\N')
        """
        cursor.copy_expert(sql=copy_sql, file=output)
        conn.commit()
        print(f"Inserted {len(df)} rows using COPY")

        # Create copy table
        cursor.execute(f"DROP TABLE IF EXISTS {copy_table_name} CASCADE;")
        cursor.execute(f"""
            CREATE TABLE {copy_table_name} AS 
            SELECT * FROM {original_table_name};
        """)
        conn.commit()
        print("Data import completed successfully")
        
        return True, None
        
    except Exception as e:
        return db.handle_error(conn, e)
        
    finally:
        db.close_connection(conn)
