FROM apache/airflow:2.7.1-python3.11

USER root

# Instala Java e Bash (essenciais para o Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless bash && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cria as pastas necessárias e garante que o usuário 'airflow' seja dono delas
RUN mkdir -p /app/scripts /app/data /app/logs && \
    chown -R airflow:root /app /opt/airflow

USER airflow

# Instala as dependências Python
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/airflow