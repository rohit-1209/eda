# login_helper_function
from db.config import Database
import logging
import uuid
from datetime import datetime, timedelta
import jwt
from psycopg2.extras import RealDictCursor
from flask import current_app

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def authenticate_user(username, password):
    """Authenticate a user and create a session if valid."""
    db = Database()
    conn = None
    cursor = None
    
    try:
        conn = db.get_db_connection()
        if not conn:
            return None, "Database connection failed"
            
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT id, username, password FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()

        if not user:
            return None, "Invalid username or password"
            
        if password != user['password']:  # In production, use proper password hashing
            return None, "Invalid username or password"
            
        # Create session
        session_id = str(uuid.uuid4())
        partial_session = session_id[:3] + session_id[-3:]

        cursor.execute("""
            INSERT INTO user_sessions (session_id, user_id)
            VALUES (%s, %s)
        """, (session_id, user['id']))

        conn.commit()
        
        # Generate JWT token
        token = jwt.encode({
            'user': username,
            'exp': datetime.utcnow() + timedelta(hours=1)
        }, current_app.config['SECRET_KEY'], algorithm='HS256')
        
        return {
            "success": True,
            "message": "Login successful",
            "user_id": user['id'],
            "partial_session": partial_session,
            "username": username,
            "token": token
        }, None

    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        return db.handle_error(conn, e)
        
    finally:
        db.close_cursor_and_connection(cursor, conn)
