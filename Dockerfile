FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for numpy/matplotlib
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libfreetype6-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create directories
RUN mkdir -p logs results

# Set Python path
ENV PYTHONPATH=/app

# Run demo
CMD ["python3", "quick_demo.py"]
