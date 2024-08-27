# Usar uma imagem base que suporte Spark
FROM python:alpine

# Instalar dependências adicionais
RUN pip install pyspark 
RUN pip install mysql-connector-python

# Criar um diretório de trabalho
WORKDIR /app/etl_project/

# Copiar os scripts para o diretório de trabalho
COPY . /app/etl_project/

# Comando para executar o script ao iniciar o container
CMD ["spark-submit", "--master", "local", "etl_script.py"]
