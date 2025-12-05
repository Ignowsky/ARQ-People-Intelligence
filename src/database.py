# SRC/database.py
import os
import sys
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carregando as variáveis de ambiente do arquivo .env assim que o módulo é importado.
load_dotenv()

def get_db_engine():
    """'
    Cria e retorna uma engine de conexão com o banco de dados PostgreSQL usando SQLAlchemy.
    Lendo as configurações descritas no arquivo .env.
    
    :return: engine SQLAlchemy e schema padrão.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")
    schema = os.getenv("DB_SCHEMA")
    
    # Validação para não tentar conectar com parâmetros incompletos
    if not all([user, password, host, port, dbname, schema]):
        print(f"ERRO: Parâmetros de conexão incompletos. Verifique o arquivo .env.")
        sys.exit(1)
    
    # Monta a url de conexão do banco de dados
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    try:
        # Configura o search_path para o schema desejado automaticamente
        engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
        return engine, schema
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        sys.exit(1)