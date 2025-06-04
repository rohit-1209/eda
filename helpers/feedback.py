# feedback_helper_function

from db.config import Database
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# def create_feedback_table():
#     """Create the feedback table if it doesn't exist."""

#     db = Database()
#     conn = None
#     cursor = None
    
#     try:
#         conn = db.get_db_connection()
#         if not conn:
#             logger.error("Database connection failed")
#             return False
            
#         cursor = conn.cursor()
        
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS feedback (
#                 id SERIAL PRIMARY KEY,
#                 username TEXT NOT NULL,
#                 feedback TEXT,
#                 rating INTEGER NOT NULL,
#                 timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             );
#         """)
#         conn.commit()
#         return True
        
#     except Exception as e:
#         logger.error(f"Error creating feedback table: {str(e)}")
#         return db.handle_error(conn, e)
        
#     finally:
#         db.close_cursor_and_connection(cursor, conn)

def create_feedback_table():
    """Create the feedback table if it doesn't exist."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            logger.error("Database connection failed")
            return False, "Database connection failed"
            
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                feedback TEXT,
                rating INTEGER NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        return True, None
        
    except Exception as e:
        logger.error(f"Error creating feedback table: {str(e)}")
        return False, str(e)


def submit_user_feedback(username, feedback, rating):
    """Submit user feedback to the database."""
    
    # First ensure the table exists
    table_created, error = create_feedback_table()
    if error:
        return None, f"Failed to create feedback table: {error}"
    
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor()
        timestamp = datetime.now()
        
        cursor.execute(
            "INSERT INTO feedback (username, feedback, rating, timestamp) VALUES (%s, %s, %s, %s);",
            (username, feedback, rating, timestamp)
        )
        conn.commit()
        
        return {"message": "Feedback submitted successfully!"}, None
        
    except Exception as e:
        logger.error(f"Error submitting feedback: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)
