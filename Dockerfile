# 1. Imagem base super estável
FROM python:3.11-slim

# 2. Instala o Java (essencial para o PySpark rodar no Linux)
RUN apt-get update && \
    apt-get install -y default-jdk-headless && \
    apt-get clean

# 3. Define a pasta de trabalho
WORKDIR /app

# 4. Copia os requisitos e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia todo o seu código para dentro do container
COPY . .

# 6. Comando padrão (vamos rodar a Silver para testar)
CMD ["python", "scripts/transform_silver.py"]