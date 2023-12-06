import logging
from os import environ

from dotenv import load_dotenv

load_dotenv()


# Configuração do log
logging.basicConfig(
    level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='UTF-8',
)


# Configuração da conexão com o Banco de dados
db_conn_str = environ.get('db_conn_str')


# Jira
# jira_project = 'RA'
# jira_project = 'TM'
jira_project = 'CS'
jira_url = environ.get('jira_url')
jira_user = environ.get('jira_user')
jira_password = environ.get('jira_password')
jira_auth = (jira_user, jira_password)
jira_columns = ['key', 'summary', 'status', 'customfield_10200']
