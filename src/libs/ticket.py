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
    return issue


def jql_query(project_key):
    jql_str = f"project={project_key} AND Status = 'Done'"
    try:
        issues = jira.search_issues(
            jql_str,
            startAt=0,
            maxResults=1000
        )

        # Lista para armazenar os dados dos problemas
        issues_data = []

        # Verifica se há pelo menos um problema
        if issues:
            for issue in issues:
                issue_data = {'Issue Key': issue.key}

                custom_field = getattr(issue.fields, "customfield_10200", None)
                issue_data['Evaluation'] = (
                    custom_field.value if custom_field else None
                )
                issue_data['Status'] = issue.fields.status.name
                issue_data['Created'] = issue.fields.created
                issue_data['Updated'] = issue.fields.updated

                issues_data.append(issue_data)

            # Cria um DataFrame pandas a partir da lista de dicionários
            df = pd.DataFrame(issues_data)

            return df

    except Exception as e:
        print(f"Erro ao executar a consulta JQL: {str(e)}")
        return pd.DataFrame()


def jql_query2(project_key, colunas):

    # Obtenha os issues do projeto
    issues = jira.search_issues(
        f"project={project_key} AND Status = 'Done'",
        startAt=0,
        maxResults=1000
    )

    # Verifica se há pelo menos um problema
    if issues:
        first_issue = issues[0].raw['fields']['status']['name']

        print(first_issue)

        # # Obtém e imprime todas as informações do primeiro problema
        # for field_name in first_issue.raw['fields']:
        #     field_value = first_issue.raw['fields'][field_name]
        #     print(f"{field_name}: {field_value}")

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
            'Issue Evaluation': str(
                getattr(issue.fields, 'customfield_10200', 'Not Evaluated')
            ),
        }
        data.append(item)

    # Crie um DataFrame do pandas com os dados
    df = pd.DataFrame(data)
    if output_file_path:
        df.to_csv(output_file_path, index=False)

    return df
