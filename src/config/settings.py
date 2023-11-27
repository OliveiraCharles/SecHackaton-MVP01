import logging
from os import environ

from dotenv import load_dotenv

load_dotenv()


# Configuração do log
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)


# Configuração da conexão com o Banco de dados
db_conn_str = environ.get('db_conn_str')


# Jira
projeto = 'RA'
jira_url = environ.get('jira_url')
jira_user = environ.get('jira_user')
jira_password = environ.get('jira_password')
jira_auth = (jira_user, jira_password)
