FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy package files
COPY setup.py requirements.txt README.md ./
COPY src/wdlp ./src/wdlp

RUN pip install --upgrade pip

# Install the package and all dependencies
RUN pip install --no-cache-dir -e ".[email]"

# Create data directories
RUN mkdir -p /data/input /data/output

# Add debug logging
ENV PYTHONUNBUFFERED=1
ENV LOGLEVEL=DEBUG

# Set the entrypoint to run the main module
ENTRYPOINT ["python", "-m", "wdlp.main"]
CMD ["--help"]
