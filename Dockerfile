FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy package files
COPY setup.py requirements.txt ./
COPY src/wdlp ./src/wdlp

RUN pip install --upgrade pip

# Install the package and dependencies
RUN pip install --no-cache-dir -e .

# Create data directory
RUN mkdir /data

# Define the default command
ENTRYPOINT ["wdlp"]
CMD ["--help"]
