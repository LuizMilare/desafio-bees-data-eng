FROM apache/airflow:2.7.1-python3.11

USER root

# Installs Java and Bash
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless bash && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Creates the necessary folders and ensures the 'airflow' user is the owner
RUN mkdir -p /app/scripts /app/data /app/logs && \
    chown -R airflow:root /app /opt/airflow

USER airflow

# Installs Python dependencies
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/airflow