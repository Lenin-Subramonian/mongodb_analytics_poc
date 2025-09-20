# docker/Dockerfile - Main application container
FROM openjdk:17-jdk-slim

# Maintainer information
LABEL maintainer="Data Team"
LABEL description="MongoDB to Iceberg Analytics Pipeline"
LABEL version="1.0.0"

# Set environment variables
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3.3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    procps \
    net-tools \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic link for python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Download and install Apache Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Create application directories
WORKDIR /app
RUN mkdir -p /app/src /app/config /app/scripts /app/logs /app/data /app/jars

# Copy Python requirements and install dependencies
COPY docker/requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt

# Download required JAR files
COPY scripts/download_jars.py /app/scripts/
RUN python3 scripts/download_jars.py

# Copy Spark configuration files
COPY config/spark-defaults.conf ${SPARK_HOME}/conf/
COPY config/log4j2.properties ${SPARK_HOME}/conf/

# Copy application source code
COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY notebooks/ /app/notebooks/
COPY sql/ /app/sql/

# Set proper permissions
RUN chmod +x /app/scripts/*.py
RUN chmod +x ${SPARK_HOME}/bin/*
RUN chmod +x ${SPARK_HOME}/sbin/*

# Create non-root user for security
RUN groupadd -r spark && useradd -r -g spark spark
RUN chown -R spark:spark /app ${SPARK_HOME}
USER spark

# Expose ports
EXPOSE 4040 4041 7077 8080 8081 8888

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:4040 || exit 1

# Default command
CMD ["python3", "src/mongodb_to_iceberg_etl.py"]