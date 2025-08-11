# syntax=docker/dockerfile:1

# Use a slim Python image
# syntax=docker/dockerfile:1

# Use a slim Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (if any, e.g., for mysql-connector)
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the application code (exclude unnecessary files)
COPY . .

# Set environment variables for Flask
ENV FLASK_APP=app.py
ENV FLASK_ENV=development

# Expose port
EXPOSE 5000

# Run the application
CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0", "--port=5000"]