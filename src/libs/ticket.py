import logging

import pandas as pd

import config.settings as st
from jira import JIRA


def connect(server, auth):
    logging.info('Conectando ao Jira...')

    # Conecta ao Jira
    try:
        jira = JIRA(server=server, basic_auth=auth)
        logging.info('Conectado com sucesso')

    except Exception as e:
        logging.info('Erro ao conectar', e)

    return jira


def create(df, jira, project_key, input_file_path=None):
    if input_file_path:
        # Lê o arquivo CSV
        df = pd.read_csv(input_file_path)

    # Itera sobre as linhas do DataFrame e cria um ticket para cada linha
    for index, row in df.iterrows():

        # Verifica se a coluna 'JiraTicketKey' já está preenchida
        if pd.notna(row['JiraTicketKey']):
            logging.info(
                "A coluna 'JiraTicketKey' já está preenchida para o finding "
                + f'{index + 1}. Pulando a criação do ticket.'
            )
            continue

        # Extrai informações relevantes do DataFrame
        summary = '[RAO - Report Analysis Optimization] ' + f"- {row['Query']}"

        description = (
            'Details:\n'
            + f"## {row['SrcFileName']} - {row['Name']}\n"
            + f'{row}'
        )

        # Cria o ticket no Jira
        issue = jira.create_issue(
            project=project_key,
            summary=summary,
            description=description,
            # Pode ser 'Task', 'Story', ou outro tipo de issue
            issuetype={'name': 'Task'},
        )

        # Adiciona a issue_key ao DataFrame
        df.at[index, 'JiraTicketKey'] = issue.key

        logging.info(f'Criação do ticket concluída: {issue.key}')


if __name__ == '__main__':
    # Exemplo de uso
    input_file = 'caminho/do/seu/arquivo.csv'
    project_key = 'RA'
    create(input_file, st.jira_url, project_key)
