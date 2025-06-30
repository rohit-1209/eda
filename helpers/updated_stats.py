#updated_statistics_helper_function.py

from full_test_autoeda.autoeda_back_flask.db.config import Database
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def get_table_statistic(table_name):
    db = Database()
    conn = None
    cursor = None
    
    try:
        # Ensure we're using the correct table name (with _copy suffix)
        if not table_name.endswith('_copy'):
            table_name = f"{table_name}_copy"

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

        # Get column names and types with more specific type checking
        cursor.execute("""
            SELECT column_name, udt_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND column_name != 'id'
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns_info = cursor.fetchall()
        logger.debug(f"Columns info: {columns_info}")
        
        statistics = []
        
        for column_name, data_type in columns_info:
            stats = {
                "column": column_name,
                "type": "categorical",  
                "missing_values": 0,
                "missing_percentage": "0%",
                "min": "N/A",
                "max": "N/A",
                "mean": "N/A",
                "median": "N/A"
            }
            
            try:
                # Get total row count
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                total_rows = cursor.fetchone()[0]

                # Count missing values with improved NULL checking
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM "{table_name}" 
                    WHERE "{column_name}" IS NULL 
                    OR CAST("{column_name}" AS TEXT) = ''
                    OR CAST("{column_name}" AS TEXT) = 'NaN';
                """)
                missing_values = cursor.fetchone()[0]
                stats["missing_values"] = missing_values

                # Calculate missing percentage
                missing_percentage = (missing_values / total_rows * 100) if total_rows > 0 else 0
                stats["missing_percentage"] = f"{missing_percentage:.1f}%"

                # Determine column type and calculate statistics
                if data_type in ['int2', 'int4', 'int8', 'float4', 'float8', 'numeric']:
                    stats["type"] = "numeric"
                    cursor.execute(f"""
                        SELECT 
                            MIN(CAST("{column_name}" AS NUMERIC))::text,
                            MAX(CAST("{column_name}" AS NUMERIC))::text,
                            AVG(CAST("{column_name}" AS NUMERIC))::text,
                            PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY CAST("{column_name}" AS NUMERIC))::text
                        FROM "{table_name}"
                        WHERE "{column_name}" IS NOT NULL 
                        AND CAST("{column_name}" AS TEXT) != ''
                        AND CAST("{column_name}" AS TEXT) != 'NaN';
                    """)
                    result = cursor.fetchone()
                    if result:
                        min_val, max_val, mean, median = result
                        stats.update({
                            "min": min_val if min_val else "N/A",
                            "max": max_val if max_val else "N/A",
                            "mean": mean if mean else "N/A",
                            "median": median if median else "N/A"
                        })
                
                elif data_type in ['timestamp', 'timestamptz', 'date', 'time']:
                    stats["type"] = "datetime"
                    cursor.execute(f"""
                        SELECT 
                            MIN("{column_name}")::text,
                            MAX("{column_name}")::text
                        FROM "{table_name}"
                        WHERE "{column_name}" IS NOT NULL 
                        AND CAST("{column_name}" AS TEXT) != '';
                    """)
                    result = cursor.fetchone()
                    if result:
                        min_val, max_val = result
                        stats.update({
                            "min": min_val if min_val else "N/A",
                            "max": max_val if max_val else "N/A"
                        })

            except Exception as e:
                logger.error(f"Error processing column {column_name}: {str(e)}")
                continue

            statistics.append(stats)

        return {"columns": statistics}, None

    except Exception as e:
        logger.error(f"Error getting table statistics: {str(e)}")
        return None, str(e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)
