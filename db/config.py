
import psycopg2
import pandas as pd
from io import BytesIO,StringIO
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'dbname': os.getenv('DB_NAME')  # ✅ This must be 'dbname'
}

class Database:
    def __init__(self):
        self.config = DB_CONFIG

    def get_db_connection(self):
        try:
            print("🔧 DB connection config:", self.config)  # ADD THIS
            conn = psycopg2.connect(**self.config)
            print("✅ Database connection established.")  # ADD THIS
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")  # ADD THIS
            return None

    def close_connection(self, conn):
        if conn:
            conn.close()
            
    def handle_error(self, conn, error):
        if conn:
            conn.rollback()
        return None, str(error)
    
    def close_cursor_and_connection(self, cursor, conn):
        """Safely close both cursor and connection."""
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            
