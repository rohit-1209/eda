FROM python:3.11-slim

# Set the working directory
WORKDIR /updated_flask_autoeda

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Expose the application port
EXPOSE 8000

# Set environment variable
ENV PYTHONUNBUFFERED=1

ENV FLASK_APP=run.py

# Command to run the application
# CMD ["python", "app.py"]
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", "--limit-request-field_size", "16380", "--worker-class", "gevent", "--timeout", "300", "run:app"]
