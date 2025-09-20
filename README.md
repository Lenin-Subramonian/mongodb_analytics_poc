Build an end-to-end pipeline that:
1. Extracts accounting data from MongoDB
2. Writes to S3 in Apache Iceberg format
3. Transforms it for analytical use
4. Enables analysis via a query engine



What this Dockerfile does:

Base Image: Uses OpenJDK 17 (compatible with Spark 3.5.3)
System Setup: Installs Python 3 and essential tools
Spark Installation: Downloads and configures Spark 3.5.3
Dependencies: Installs Python packages and downloads JAR files
Security: Creates non-root user for running applications
Health Check: Monitors Spark UI for container health


What Docker Compose provides:

Service Orchestration: Manages multiple containers
Network Isolation: Creates dedicated network for services
Volume Management: Persistent storage for data and logs
Environment Management: Loads configuration from .env file
Development Support: Hot-reload for code changes
Optional Services: Local MongoDB for development/testing