#filtering_helper_function

from db.config import Database
import logging
import pandas as pd
from io import StringIO

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def filter_dataframe_multiple(df, filters):
    """Apply multiple filters to a DataFrame."""

    filtered_df = df.copy()

    for column, filter_value in filters.items():
        if column in df.columns:
            try:
                if isinstance(filter_value, dict):
                    operator = filter_value.get('operator')
                    value = filter_value.get('value')
                    
                    if operator == '<':
                        filtered_df = filtered_df[filtered_df[column] < value]
                    elif operator == '>':
                        filtered_df = filtered_df[filtered_df[column] > value]
                    elif operator == 'contains':
                        filtered_df = filtered_df[filtered_df[column].str.contains(value, case=False, na=False)]
                else:
                    if df[column].dtype == 'O':  
                        if isinstance(filter_value, str):
                            filter_value = filter_value.strip()  
                            filtered_df = filtered_df[filtered_df[column].str.strip() == filter_value]
                        elif pd.isna(filter_value):  
                            filtered_df = filtered_df[filtered_df[column].isna()]

                    elif pd.api.types.is_numeric_dtype(df[column]):  
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                        if isinstance(filter_value, (int, float)):
                            filter_value = float(filter_value)
                        elif isinstance(filter_value, list):  
                            filter_value = [float(v) if isinstance(v, (int, float, str)) and not pd.isna(v) else v for v in filter_value]
                        
                        if isinstance(filter_value, list):
                            filtered_df = filtered_df[filtered_df[column].isin(filter_value)]
                        else:
                            filtered_df = filtered_df[filtered_df[column] == filter_value]

                    elif pd.api.types.is_datetime64_any_dtype(df[column]): 
                        df[column] = pd.to_datetime(df[column], errors='coerce')  
                        if isinstance(filter_value, str):  
                            filter_value = pd.to_datetime(filter_value, errors='coerce')
                            filtered_df = filtered_df[filtered_df[column] == filter_value]
                        elif isinstance(filter_value, list):  
                            if len(filter_value) == 2:
                                start_date, end_date = pd.to_datetime(filter_value[0], errors='coerce'), pd.to_datetime(filter_value[1], errors='coerce')
                                filtered_df = filtered_df[(filtered_df[column] >= start_date) & (filtered_df[column] <= end_date)]
                        elif isinstance(filter_value, str) and len(filter_value) == 10:  
                            filter_value = pd.to_datetime(filter_value, errors='coerce')
                            filtered_df = filtered_df[filtered_df[column].dt.date == filter_value.date()]
                        elif pd.isna(filter_value):  
                            filtered_df = filtered_df[filtered_df[column].isna()]

                logger.info(f"Filtering {column} with value {filter_value}, remaining rows: {len(filtered_df)}")

            except (ValueError, TypeError) as e:
                logger.error(f"Error processing column {column}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error for column {column}: {e}")
                continue

    return filtered_df

def apply_filters_to_table(original_table_name, sheet_name, filters):
    """Apply filters to a table and update the copy table with filtered data."""
    
    db = Database()
    conn = None
    cursor = None
    
    try:
        copy_table_name = f"{original_table_name}_{sheet_name}_copy".lower()
        logger.info(f"Applying filters to table: {copy_table_name}")
        logger.info(f"Filters: {filters}")
        
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()

        # Get data from the copy table
        cursor.execute(f'SELECT * FROM "{copy_table_name}";')
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=colnames)
        logger.info(f"Original data shape: {df.shape}")
        
        # Convert numeric columns
        for col in df.columns:
            if col in df.columns and df[col].dtype in ['int64', 'float64']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Apply filters
        filtered_df = filter_dataframe_multiple(df, filters)
        
        if filtered_df.empty:
            return {
                "message": f"No data found for the given filters: {filters}",
                "data": []
            }, None

        # Update the copy table with filtered data
        cursor.execute(f'TRUNCATE TABLE "{copy_table_name}";')
        output = StringIO()
        filtered_df.to_csv(output, index=False, header=False)
        output.seek(0)
        cursor.copy_expert(f'COPY "{copy_table_name}" FROM STDIN WITH (FORMAT csv)', output)
        conn.commit()
        
        result = filtered_df.to_dict(orient='records')
        return {
            "message": "Data filtered successfully",
            "filtered_count": len(filtered_df),
            "data": result  
        }, None

    except Exception as e:
        logger.error(f"Error applying filters: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)
