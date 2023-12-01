import logging

import pandas as pd

import config.settings as st
from jira import JIRA

# Conecta ao Jira
try:
    jira = JIRA(server=st.jira_url, basic_auth=st.jira_auth)
    logging.info('Conectado com sucesso')

except Exception as e:
    logging.error('Erro ao conectar' + str(e))


def create(row, project_key):

    # Extrai informações relevantes do DataFrame
    summary = '[RAO - Report Analysis Optimization] ' + f"- {row['Query']}"

    description = (
        f"File: {row['SrcFileName']}\nFunction: {row['Name']}\n\n"
        # + 'Details:\n'
        # + f'{row}'
    )

    # Mapeia o valor de Result Severity para Priority no Jira
    priority_mapping = {
        'High': 'High',  # Adapte conforme a correspondência necessária
        'Medium': 'Medium',
        'Low': 'Low',
        'Information': 'Lowest',
    }

    # Obtém a prioridade com base em ResultSeverity
    result_severity = row['Result Severity']

    # Valor padrão 'Medium' se não houver correspondência
    priority = priority_mapping.get(result_severity, 'Medium')

    # Cria o ticket no Jira
    issue = jira.create_issue(
        project=project_key,
        summary=summary,
        description=description,
        # Pode ser 'Task', 'Story', ou outro tipo de issue
        issuetype={'name': 'Task'},
        priority={'name': priority},
    )

    logging.info(f'Criação do ticket concluída: {issue.key}')
    return str(issue.key)


def query(project_key, colunas):

    # Obtenha os issues do projeto
    issues = jira.search_issues(
        f'project={project_key}', startAt=0, maxResults=1000
    )

    # Crie uma lista para armazenar os dados
    dados = []

    # Itere sobre os issues e extraia as colunas desejadas
    for issue in issues:
        item = {}
        for coluna in colunas:
            # Certifique-se de que a coluna existe no objeto fields
            if hasattr(issue.fields, coluna):
                item[coluna] = getattr(issue.fields, coluna)
            else:
                # Se a coluna não existir, adicione None ao DataFrame
                item[coluna] = None
        dados.append(item)

    # Crie um DataFrame do pandas com os dados
    df = pd.DataFrame(dados)

    return df


def import_jira_data(project_key, output_file_path=None):

    # Execute uma consulta JQL para obter os tickets do projeto
    # com as colunas desejadas
    jql_query = f'project={project_key}'
    issues = jira.search_issues(jql_query, maxResults=1000)

    # Crie uma lista para armazenar os dados
    data = []

    # Itere sobre os tickets e extraia as informações desejadas
    for issue in issues:

        item = {
            'Key': issue.key,
            'Summary': issue.fields.summary,
            'Created': issue.fields.created,
            'Updated': issue.fields.updated,
            'Duedate': issue.fields.duedate,
            'Resolutiondate': issue.fields.resolutiondate,
            'Status': issue.fields.status.name,
            'Evaluation': str(
                getattr(issue.fields, 'customfield_10200', 'Not Evaluated')
            ),
        }
        data.append(item)

    # Crie um DataFrame do pandas com os dados
    df = pd.DataFrame(data)
    if output_file_path:
        df.to_csv(output_file_path, index=False)

    return df
