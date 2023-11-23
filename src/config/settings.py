import logging
from os import environ
from dotenv import load_dotenv

load_dotenv()

# Diretório contendo os relatórios CSV
reports_directory = './reports'

# Arquivo de saída combinado
merged_file_path = './reports/merged/reports.csv'

# Nome da tabela no PostgreSQL
table_name = 'reports'

columns_to_check = ['SrcFileName', 'Name', 'Query']

# Configuração da conexão com o PostgreSQL
postgres_conn_str = environ.get('postgres_conn_str')

# Configuração do log
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)
