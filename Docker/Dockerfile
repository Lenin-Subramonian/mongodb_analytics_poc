# docker/Dockerfile - Using official Spark image
FROM apache/spark:3.5.3-java17-python3

# Switch to root for installation, Only root can install packages & create directories
USER root

# Maintainer information
LABEL maintainer="Data Team"
LABEL description="MongoDB to Iceberg Analytics Pipeline"
LABEL version="1.0.0"

# Install system dependencies, python3, pip, wget, curl, vim
# wget - downloads JAR files, curl - health check + debugging, vim - only for debugging/editing
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    vim \
    python3 python3-venv python3-pip ca-certificates \
    ln -s /usr/bin/python3 /usr/bin/python || true && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create application directories
WORKDIR /app
RUN mkdir -p /app/src /app/config /app/scripts /app/logs /app/data /app/jars

# Copy Python requirements and install dependencies
COPY docker/requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt

# ensure ivy & m2 directories exist and owned by spark inside image
# use named Docker volumes (or a container-local path). This prevents permission problems on Windows/WSL.
RUN mkdir -p /home/spark/.ivy2 /home/spark/.m2 \
    && chown -R spark:spark /home/spark/.ivy2 /home/spark/.m2

# Copy entrypoint script into container & make it executable
COPY docker/entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh
# normalize CRLF -> LF and make executable (sed is portable)
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

# Copy application source code
COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY notebooks/ /app/notebooks/
COPY sql/ /app/sql/

# Set permissions
RUN chmod +x /app/scripts/*.py
RUN chown -R spark:spark /app

# Switch back to non-root (spark) for security
# Build time: Use root to install dependencies, Runtime: Switch to non-root user for security
USER spark

# Removing ENTRYPOINT from the image prevents every container made from the image automatically running  ETL job
# ENTRYPOINT ["/entrypoint.sh"]

# Expose ports
EXPOSE 4040 4041 7077 8080 8081 8888

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:4040 || exit 1

# Default command
# CMD ["python3", "src/mongodb_to_iceberg_etl.py"]
# Run via spark-submit (no --packages needed, jars already in classpath)
CMD [ "bash", "-c", "/opt/spark/bin/spark-submit --master local[*] \
  --conf spark.driver.memory=1g --conf spark.executor.memory=1g \
  /app/src/mongodb_to_iceberg_etl.py" ]
