from db.config import Database
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def get_table_data(filename, sheet_name):
    """
    Retrieve data from a table based on filename and sheet name.

    Args:
        filename (str): The name of the file
        sheet_name (str): The name of the sheet

    Returns:
        tuple: (result, error) where result contains data and columns or None if error
    """
    db = Database()
    conn = None
    cursor = None

    try:
        table_name = f"{filename.strip().lower()}_{sheet_name.lower()}"
        logger.info(f"Retrieving data from table: {table_name}")

        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"

        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'postgres' AND table_name = %s
            );
        """, (table_name,))

        if not cursor.fetchone()[0]:
            return None, f"Table {table_name} does not exist"

        # Get column names
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'postgres'
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))

        columns = [col[0] for col in cursor.fetchall()]
        logger.debug(f"Columns before filtering: {columns}")

        # Remove 'id' column if it exists
        try:
            if 'id' in columns:
                cursor.execute(f'ALTER TABLE "{table_name}" DROP COLUMN id;')
                conn.commit()

                # Refresh column list
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """, (table_name,))

                columns = [col[0] for col in cursor.fetchall()]
        except Exception as e:
            logger.warning(f"Could not drop 'id' column: {str(e)}")
            if conn:
                conn.rollback()
                cursor.close()
                cursor = conn.cursor()


            # Continue even if dropping the column fails

        # Get the data
        cursor.execute(f'SELECT * FROM "{table_name}";')
        data = cursor.fetchall()

        # Convert data to list of lists for JSON serialization
        data_list = [list(row) for row in data]

        return {
            'columns': columns,
            'data': data_list
        }, None

    except Exception as e:
        logger.error(f"Error retrieving table data: {str(e)}")
        if conn:
            conn.rollback()
        return None, f"Database error: {str(e)}"

    finally:
        db.close_cursor_and_connection(cursor, conn)