# Brewery Data Pipeline (BEES Challenge)

Este projeto implementa um pipeline de dados escalável utilizando a **Arquitetura Medalhão** para consumir, processar e organizar dados da [Open Brewery DB](https://www.openbrewerydb.org/).



## 🛠️ Tecnologias Utilizadas

* **Python 3.11**: Linguagem base para scripts de automação.
* **Apache Spark (PySpark)**: Processamento distribuído para transformações de grande escala.
* **Docker**: Conteinerização para garantir a portabilidade do ambiente (Windows/Linux/Mac).
* **Parquet**: Formato de armazenamento colunar otimizado para leitura e compressão.

## 🏗️ Arquitetura do Pipeline

O fluxo de dados segue os princípios de Engenharia de Dados moderna:

1.  **Bronze (Raw)**: Ingestão de dados brutos da API REST em formato JSON. Foco em persistência e garantia de histórico (Source of Truth).
2.  **Silver (Cleaned)**: Limpeza de dados (tratamento de valores nulos, padronização de tipos) e **particionamento por localização (`country`)**. O uso de Parquet nesta camada reduz drasticamente o tempo de consulta.
3.  **(TO-DO) Gold (Analytics)**: Agregação final pronta para consumo (ex: contagem de cervejarias por tipo e localidade), otimizada para ferramentas de BI.

## 🚀 Como Executar

### Pré-requisitos
* Docker Desktop instalado e rodando.

### Execução via Docker (Recomendado)
Para evitar conflitos de dependências locais (como Java ou Hadoop/Winutils no Windows), utilize o ambiente isolado:

1.  **Build da imagem:**
    ```bash
    docker build -t brewery-pipeline .
    ```

2.  **Execução do pipeline:**
    ```bash
    docker run -v "${PWD}/data:/app/data" brewery-pipeline
    ```
    *(O comando acima mapeia a pasta `data` do seu host para o container, permitindo visualizar os arquivos gerados localmente).*

## 📈 Decisões de Design & Diferenciais

* **Persistência em Parquet**: Escolhido pela eficiência de compressão e suporte a *Predicate Pushdown*, permitindo que o Spark leia apenas as fatias de dados necessárias.
* **Particionamento Estratégico**: Os dados foram particionados por país na camada Silver. Isso evita o *Full Table Scan* em consultas geográficas, um padrão essencial em cenários de Big Data.
* **Robustez no Windows**: Implementação de configurações customizadas de `SparkSession` (`RawLocalFileSystem`) para garantir a escrita de arquivos mesmo em ambientes sem Hadoop nativo.
* **Idempotência**: O pipeline utiliza o modo `overwrite`, permitindo múltiplas execuções sem duplicidade ou corrupção de dados.
