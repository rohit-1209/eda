FROM dockerproxy.repos.tech.orange/python:3.12

# Set the working directory
WORKDIR /updated_flask_autoeda

# Copy the requirements file
COPY requirements-rohit.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements-rohit.txt

# Copy the entire project
COPY . .

# Expose the application port
EXPOSE 8000

# Set environment variable
ENV PYTHONUNBUFFERED=1

ENV FLASK_APP=app.py

# Command to run the application
CMD ["python", "app.py"]
# CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "4", "--threads", "10", "--limit-request-field_size", "16380", "--worker-class", "gevent", "run:app"]