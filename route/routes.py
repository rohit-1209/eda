from flask import Blueprint, request, jsonify,Flask,make_response,flash,current_app
from io import BytesIO,StringIO
from flask import Blueprint, request, jsonify, Flask, make_response, flash, current_app, redirect
import helpers
import pandas as pd
import re
from helpers.Get_data import get_table_data
from helpers.Updated_Get_O_D import Gets_Data
from helpers.updated_stats import get_table_statistic
import json
from datetime import date, datetime,timedelta
from helpers.upload_insert_data import get_sheet_names
import jwt
from io import StringIO
import psycopg2
from flask_cors import CORS
from openpyxl import load_workbook
from psycopg2.extras import RealDictCursor,execute_values
from psycopg2 import Error
from contextlib import closing
import os
import uuid
import warnings
import logging

import re
from functools import wraps
from jwt import ExpiredSignatureError, InvalidTokenError
from  helpers.login import authenticate_user
from  helpers.remove_column import get_remaining_columns,verify_table_exists,remove_columns
from  helpers.upload_insert_data import insert_data_from_excel
from  helpers.update_overview import get_column_types_from_db
from  helpers.filter_column import filter_dataframe_multiple,apply_filters_to_table
from  helpers.change_datatype import check_table_existence, change_column_data_types, CustomJSONEncoder
from  helpers.logout_delete_file import delete_data_from_table
from  helpers.handle_fill import  handle_missing_data
from  helpers.feature_engineering import process_categorical_features
from  helpers.rename_column import rename_columns_in_copy
from  helpers.rollback import sync_table_structure,sync_data_from_original_to_copy,sync_tables
from  helpers.remove_duplicate_row import remove_duplicates_from_table
from  helpers.data_type import get_table_data_types
from  helpers.feedback import submit_user_feedback
# from helpers.stats import get_table_statistics
from  helpers.stats import get_table_statistics
from  db.config import Database

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
warnings.filterwarnings('ignore', message='Could not infer format, so each element will be parsed individually')


def token_required(func):
    @wraps(func)
    def decorated(*args, **kwargs):

        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Unauthorized'}), 401
        token = auth_header.split(' ')[1]

        if not token:
            return jsonify({'Alert!': 'Token is missing!'}), 401

        try:
            data = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=["HS256"])
        except ExpiredSignatureError:
            return jsonify({'Message': 'Token has expired'}), 401
        except InvalidTokenError:
            return jsonify({'Message': 'Invalid token'}), 403

        return func(*args, **kwargs)

    return decorated


main = Blueprint("main", __name__)


# count Router
@main.route('/get_data', methods=['GET'])
@token_required
def get_data_display():
    """Display dataset overview from the specified table."""
    table_name = request.args.get('table_name')
    sheet_name = request.args.get('sheet_name')

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    result, error = get_table_data(table_name, sheet_name)

    if error:
        return jsonify({"error": error}), 400

    df = pd.DataFrame(result['data'], columns=result['columns'])

    overview = {
        "rows": df.shape[0],
        "columns": df.shape[1],
        "row_duplicates": df.shape[0] - df.drop_duplicates().shape[0],
        "categorical_columns": df.select_dtypes(include=['object']).columns.tolist(),
        "numerical_columns": df.select_dtypes(include=['number']).columns.tolist(),
        "datetime_columns": df.select_dtypes(include=['datetime']).columns.tolist(),
    }

    return jsonify(overview), 200

# datatype_manage Router
@main.route('/check_table/<table_name>', methods=['GET'])
@token_required
def check_table_exists(table_name):
    """Check if a table exists and return its column information."""
    try:
        logger.info(f"Checking existence of table: {table_name}")

        result, error = check_table_existence(table_name)

        if error:
            return jsonify({"error": error}), 500

        return jsonify(result), 200

    except Exception as e:
        # logger.error(f"Unexpected error in check_table_exists: {str(e)}")
        return jsonify({"error": str(e)}), 500


@main.route('/manage_datatype', methods=['POST'])
@token_required
def change_data_types():
    """Change the data types of columns in a table."""
    try:
        data = request.json
        table_name = data.get('table_name')
        columns_to_change = data.get('columns', {})

        # Construct copy table name
        copy_table_name = f"{table_name}".lower()

        logger.info(f"Changing data types for table: {copy_table_name}")
        logger.debug(f"Columns to change: {columns_to_change}")

        if not table_name or not columns_to_change:
            return jsonify({"error": "Missing required fields"}), 400

        result, error = change_column_data_types(copy_table_name, columns_to_change)

        if error:
            if "does not exist" in error:
                return jsonify({"error": error}), 404
            else:
                return jsonify({"error": error}), 400

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in change_data_types: {str(e)}")
        return jsonify({"error": str(e)}), 500


# datatype Router
@main.route('/datatypes', methods=['GET'])
@token_required
def display_data_types():
    """Display the data types of columns in the specified table."""
    try:
        table_name = request.args.get('table_name')
        sheet_name = request.args.get('sheet_name')

        logger.info(f"Received data types request for table: {table_name}, sheet: {sheet_name}")

        if not table_name or not sheet_name:
            return jsonify({"error": "Both table name and sheet name are required as query parameters"}), 400

        result, error = get_table_data_types(table_name, sheet_name)

        if error:
            if "not found" in error:
                return jsonify({"error": error}), 404
            elif "No columns" in error:
                return jsonify({"error": error}), 404
            else:
                return jsonify({"error": error}), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in display_data_types: {str(e)}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# display_f Router
@main.route('/data', methods=['GET'])
@token_required
def fetch_data():
    try:
        filename = request.args.get('Filename')
        sheet_name = request.args.get('sheetName')

        logger.info(f"Fetching data for file: {filename}, sheet: {sheet_name}")

        if not filename or not sheet_name:
            return jsonify({"error": "Filename and sheetName are required"}), 400

        result, error = get_table_data(filename, sheet_name)

        if error:
            if "does not exist" in error:
                return jsonify({"error": error}), 404
            else:
                return jsonify({"error": error}), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in fetch_data: {str(e)}")
        return jsonify({"error": str(e)}), 500


# feature_engineering Router
@main.route('/feature_engineering', methods=['POST'])
@token_required
def handle_categorical():
    try:
        data = request.json
        table_name = data.get('table_name')
        sheet_name = data.get('sheet_name')
        operation = data.get('operation')
        columns = data.get('columns', [])

        logger.info(f"Received feature engineering request: {operation} on {table_name}_{sheet_name}")

        if not all([table_name, sheet_name, operation]):
            return jsonify({"error": "Missing required parameters"}), 400

        result, error = process_categorical_features(table_name, sheet_name, operation, columns)

        if error:
            if isinstance(error, dict) and "available_categorical_columns" in error:
                return jsonify(error), 400
            elif "not found" in str(error):
                return jsonify({"error": str(error)}), 404
            else:
                return jsonify({"error": str(error)}), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in handle_categorical: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Feedback Route
@main.route('/feedback', methods=['POST'])
@token_required
def submit_feedback():
    try:
        data = request.get_json()
        username = data.get("username")
        feedback = data.get("feedback", "")  # Make feedback optional
        rating = data.get("rating")

        if not username or not rating:
            return jsonify({"error": "Username and rating are required"}), 400

        logger.info(f"Received feedback from user: {username}")

        result, error = submit_user_feedback(username, feedback, rating)

        if error:
            logger.error(f"Error submitting feedback: {error}")
            return jsonify({"error": error}), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in submit_feedback: {str(e)}")
        return jsonify({"error": str(e)}), 500


# fill_missing Router
@main.route('/handle/fill', methods=['POST'])
@token_required
def handle_missing_values():
    try:
        data = request.get_json()
        table_name = data.get('table_name')
        columns = data.get('columns', [])
        action = data.get('action')
        method = data.get('method')
        copy_table_name = f"{table_name}".lower()

        logger.info(f"Received missing values request for table: {copy_table_name}")
        logger.info(f"Action: {action}, Method: {method}, Columns: {columns}")

        if not table_name or not columns or not action:
            return jsonify({"error": "Missing required parameters"}), 400

        if action not in ['fill', 'remove']:
            return jsonify({"error": "Invalid action. Use 'fill' or 'remove'"}), 400

        result, error = handle_missing_data(copy_table_name, columns, action, method)

        if error:
            if "Non-numeric columns" in error or "No numeric columns" in error:
                return jsonify({"error": error}), 400
            elif "does not exist" in error:
                return jsonify({"error": error}), 404
            else:
                return jsonify({"error": error}), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in handle_missing_values: {str(e)}")
        return jsonify({"error": str(e)}), 500


# filtering Router
@main.route('/filtering', methods=['POST'])
@token_required
def filter_data():
    try:
        data = request.json
        original_table_name = data.get('table_name')
        sheet_name = data.get('sheet_name')
        filters = data.get('filters')

        logger.info(f"Received filtering request for {original_table_name}_{sheet_name}")
        logger.debug(f"Filters: {filters}")

        if not original_table_name or not filters or not sheet_name:
            return jsonify({"error": "Missing required fields: table_name, sheet_name or filters"}), 400

        result, error = apply_filters_to_table(original_table_name, sheet_name, filters)

        if error:
            return jsonify({"error": error}), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in filter_data: {str(e)}")
        return jsonify({"error": str(e)}), 500


# login Route
@main.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return jsonify({"error": "Username and password are required"}), 400

        logger.info(f"Login attempt for user: {username}")

        result, error = authenticate_user(username, password)

        if error:
            logger.warning(f"Login failed for user {username}: {error}")
            return jsonify({"error": error}), 401 if "Invalid" in error else 500

        # Create response with the authentication result
        response = make_response(jsonify(result))
        return response, 200

    except Exception as e:
        logger.error(f"Unexpected error in login route: {str(e)}")
        return jsonify({"error": str(e)}), 500


# logout Route
@main.route('/logout', methods=['POST'])
@token_required
def logout_data():
    if request.method == 'POST':
        data = request.get_json()
        if not data or 'tableName' not in data or 'sheetName' not in data:
            return jsonify({'error': 'Missing tableName or sheetName in request'}), 400

        table_name = data['tableName']
        sheet_name = data['sheetName']
        logger.info(f"Received logout request for tableName: {table_name}, sheetName: {sheet_name}")

        try:
            # Call the function to handle table deletion
            if delete_data_from_table(table_name, sheet_name):
                return jsonify({
                    'message': f'Data from table {table_name} and sheet {sheet_name} deleted successfully',
                    'success': True
                }), 200
            else:
                return jsonify({'error': 'Failed to delete data - tables may not exist'}), 404

        except Exception as e:
            logger.error(f"Error in logout_data: {str(e)}")
            return jsonify({'error': f'An error occurred: {str(e)}'}), 500

    return jsonify({'message': 'Please use POST method to delete data.'}), 405


# pre_stats Route
@main.route('/stats', methods=['GET'])
@token_required
def display_statistics():
    logger.info("Received request for statistics")
    try:
        filename = request.args.get('Filename')
        sheet_name = request.args.get('sheet_name')

        if not filename or not sheet_name:
            return jsonify({"error": "Both Filename and sheet_name are required"}), 400

        result, error = get_table_statistics(filename, sheet_name)

        if error:
            if "does not exist" in error:
                return jsonify({"error": error}), 404
            return jsonify({"error": error}), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in display_statistics: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


# remove_column Router
@main.route('/manage_columns', methods=['POST'])
@token_required
def manage_columns():
    """Remove columns in the specified table based on user input."""
    try:
        data = request.json
        original_table_name = data.get('table_name')
        sheet_name = data.get('sheet_name')
        columns_to_remove = data.get('columns', [])

        if not original_table_name or not columns_to_remove or not sheet_name:
            return jsonify({"error": "Missing required parameters"}), 400

        # Log request details
        logger.info(f"Received request to remove columns from {original_table_name}_{sheet_name}")
        logger.info(f"Columns to remove: {columns_to_remove}")

        result, error = remove_columns(original_table_name, sheet_name, columns_to_remove)

        if error:
            if "does not exist" in error:
                return jsonify({"error": error}), 404
            return jsonify({"error": error}), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in manage_columns: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


# rename_column Route
@main.route('/rename_column', methods=['POST'])
@token_required
def rename_columns_copy_api():
    try:
        data = request.json
        table_name = data.get('table_name')
        sheet_name = data.get('sheet_name')
        column_mappings = data.get('column_mappings')  # Expecting a dictionary {old_column: new_column}

        if not all([table_name, sheet_name, column_mappings]) or not isinstance(column_mappings, dict):
            return jsonify({'error': 'Missing or invalid required parameters'}), 400

        result = rename_columns_in_copy(table_name, sheet_name, column_mappings)

        if 'error' in result:
            return jsonify(result), 400

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error in rename_columns_copy_api: {str(e)}")
        return jsonify({'error': str(e)}), 500


# rollback Router
@main.route('/sync', methods=['POST'])
@token_required
def sync_tables_route():
    try:
        # Validate input
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        table_name = data.get('table_name')
        sheet_name = data.get('sheet_name')

        if not all([table_name, sheet_name]):
            return jsonify({"error": "Missing required parameters"}), 400

        # Call the helper function
        result, error = sync_tables(table_name, sheet_name)

        if error:
            return jsonify({
                "status": "error",
                "message": f"Error: {error}"
            }), 500

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Unexpected error in sync_tables_route: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }), 500


# updated_count Router
@main.route('/updated_overview', methods=['GET'])
@token_required
def display_overview():
    """Display dataset overview from the specified table's copy."""
    try:
        Filename = request.args.get('Filename')

        if not Filename:
            return jsonify({"error": "Filename is required as a query parameter"}), 400

        # Get data from the table using Gets_Data
        result = Gets_Data(Filename)

        # Check if result is an error response
        if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict) and 'error' in result[0]:
            return jsonify(result[0]), result[1]

        data, columns = result

        # Format data as a DataFrame for analysis
        df = pd.DataFrame(data)

        # Remove the 'id' column if it exists
        if 'id' in df.columns:
            df = df.drop(columns=['id'])

        # Fetch column types from the database
        column_types, error = get_column_types_from_db(Filename)

        if error:
            return jsonify({"error": f"Error getting column types: {error}"}), 500

        # Calculate overview statistics
        overview = {
            "total_rows": df.shape[0],
            "total_columns": df.shape[1],
            "duplicate_rows": df.shape[0] - df.drop_duplicates().shape[0],
            "unique_rows": df.drop_duplicates().shape[0],
            "column_types": column_types  # Include column types from DB schema
        }

        return jsonify(overview), 200

    except Exception as e:
        logger.error(f"Error in display_overview: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Updated_display Router
@main.route('/updated_display', methods=['GET'])
@token_required
def fetched_data():
    try:
        # Get filename from query parameter
        filename = request.args.get('Filename')
        if not filename:
            return jsonify({"error": "Filename parameter is required"}), 400

        logger.info(f"Received request for filename: {filename}")

        # Get data from the table
        result = Gets_Data(filename)

        # Check if result is an error response
        if isinstance(result, tuple) and len(result) == 2:
            if isinstance(result[0], dict) and 'error' in result[0]:
                logger.error(f"Error in Gets_Data: {result[0]['error']}")
                return jsonify(result[0]), result[1]
            data, columns = result
        else:
            data, columns = result

        logger.info(f"Successfully processed data with {len(data)} rows")
        return jsonify(data)

    except Exception as e:
        logger.error(f"Error in fetched_data: {str(e)}")
        return jsonify({"error": str(e)}), 500


# updtaed_statistics
@main.route('/updated_statistics', methods=['GET'])
@token_required
def get_updated_statistics():
    try:
        table_name = request.args.get('Filename')
        logger.debug(f"Received request for table: {table_name}")

        if not table_name:
            return jsonify({"error": "Filename parameter is required"}), 400

        result, error = get_table_statistic(table_name)

        if error:
            return jsonify({"error": error}), 404 if "does not exist" in error else 500

        return jsonify(result)

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


# upload Router
import os
from flask import request, jsonify
from route.routes import main
  # adjust import as needed

@main.route('/upload/', methods=['POST', 'GET'])
@token_required  # Uncomment if you want to enable token checking later
def upload_excel():
    if request.method == 'POST':
        print("Request files:", request.files)
        print("Request form keys:", request.form.keys())

        if 'file' not in request.files:
            print("Key not found in request files")
            return jsonify({'error': 'No file part in the request'}), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        if not file.filename.endswith(('.xls', '.xlsx', '.csv')):
            return jsonify({'error': 'Invalid file format. Only .xls, .csv and .xlsx are allowed.'}), 400

        # Get sheetName if present
        sheetname = request.form.get('sheetName')

        if sheetname:
            # Case 2: sheetName present -> process file with this sheet
            import re
            base_name = os.path.splitext(file.filename)[0]
            safe_base_name = re.sub(r'\W+', '_', base_name)
            safe_sheetname = re.sub(r'\W+', '_', sheetname)

            original_table_name = f"{safe_base_name}_{safe_sheetname}"
            copy_table_name = f"{original_table_name}_copy"

            try:
                success, error = insert_data_from_excel(file, original_table_name, copy_table_name, sheetname)

                if success:
                    return jsonify({
                        'message': 'File uploaded and data saved successfully',
                        'success': True,
                        'tableName': original_table_name  # ✅ important for frontend
                    }), 200
                else:
                    return jsonify({'error': f'Failed to save data: {error}'}), 400

            except Exception as e:
                print(f"Error: {str(e)}")
                return jsonify({'error': f'An error occurred: {str(e)}'}), 500

        else:
            # Case 1: initial upload -> return available sheet names
            try:
                sheet_names = get_sheet_names(file)
                return jsonify({'sheetNames': sheet_names}), 200

            except Exception as e:
                print(f"Error getting sheet names: {str(e)}")
                return jsonify({'error': f'Failed to get sheet names: {str(e)}'}), 500

    return jsonify({'message': 'Please use POST method to upload files.'}), 405




# remove_duplicate Router
@main.route('/remove_duplicates', methods=['POST'])
@token_required
def remove_duplicates():
    try:
        data = request.json
        original_table_name = data.get('table_name')
        sheet_name = data.get('sheet_name')
        duplicate_columns = data.get('duplicate_columns')  # List of columns to check for duplicates

        if not original_table_name or not sheet_name or not duplicate_columns:
            return jsonify({"error": "Missing required fields: table_name, sheet_name, or duplicate_columns"}), 400

        copy_table_name = f"{original_table_name}_{sheet_name}_copy".lower()

        result, error = remove_duplicates_from_table(copy_table_name, duplicate_columns)

        if error:
            return jsonify({"error": error}), 500

        return jsonify({
            "message": "Duplicates removed successfully",
            "original_count": result["original_count"],
            "deduped_count": result["deduped_count"]
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



