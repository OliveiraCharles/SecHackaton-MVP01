import logging

import config.settings as st
import libs.database as db
import libs.report as dt
import libs.ticket as tk


def scan_reports(directory):
    logging.info('Scanning reports...')
    df = dt.merge(
        directory=directory,
        output_file_path='reports/output/merged.csv',
        column_names=db.get_column_names('reports'),
    )
    return df


def check_report(row, table_name='reports'):
    # if is duplicated or is information, not need to save
    result = not (is_duplicated(row, table_name))  # or is_information(row))
    return result


def create_ticket(query_table, project_key):
    logging.info('Creating Tickets...')

    df = db.execute_sql_query(
        query_sql=f"""
    SELECT * FROM {query_table}
    WHERE "Result Severity" != 'Information'
    AND "Issue Evaluation" NOT IN (
        'Issue Solved', 'Possible Issue', 'False Positive'
        )
    AND ("Issue Key" IS NULL
    OR "Issue Key" ILIKE 'nan')
    """
    )

    for index in df.index:
        issue = tk.create(row=df.loc[index], project_key=project_key)

        df.loc[index, 'Issue Key'] = str(issue.key)
        df.loc[index, 'Issue Status'] = issue.fields.status.name
        df.loc[index, 'Issue Created'] = issue.fields.created
        df.loc[index, 'Issue Updated'] = str(issue.fields.updated)
        df.loc[index, 'Issue Summary'] = issue.fields.summary
        df.loc[index, 'Issue Priority'] = issue.fields.priority.name
        df.loc[index, 'Issue Resolved'] = str(issue.fields.resolutiondate)
        df.loc[index, 'Issue Due Date'] = issue.fields.duedate
        df.loc[index, 'Issue Assignee'] = getattr(
            issue.fields.assignee, 'displayName', None
        )
        df.loc[index, 'Issue Review Note'] = getattr(
            issue.fields, 'customfield_10201', None
        )
        if issue.fields.status.name == 'Done':
            df.loc[index, 'Issue Evaluation'] = getattr(
                issue.fields, 'customfield_10200', None
            ).value

    return df


def save_report(row, table_name):
    db.insert_finding(table_name=table_name, row=row)


def is_information(row):
    result = row['Result Severity'] == 'Information'
    return result


def is_duplicated(row, table_name):
    df = db.execute_sql_query(
        query_sql=f"""
    SELECT * FROM {table_name}
    WHERE "SrcFileName"='{row['SrcFileName']}'
    AND "Name"='{row['Name']}'
    AND "Query"='{row['Query']}'
    """
    )
    result = not df.empty
    return result


def scan_ticket():
    logging.info('Scanning Tickets...')

    try:
        df = tk.jql_query(project_key=st.jira_project)
        return df
    except Exception as e:
        logging.error(f'Erro ao escanear tickets: {str(e)}')


def save_new_report(df, table_name='reports'):
    logging.info('Saving reports...')

    for index, row in df.iterrows():
        if check_report(row=row, table_name=table_name):
            db.insert_finding(table_name=table_name, row=row)


def update_report(df):
    logging.info('Updating reports...')
    if df is None or df.empty:
        logging.warning('No tickets to update.')
        return

    for index, row in df.iterrows():
        db.update_report(
            row=row,
        )


def update_ticket(df):
    if df is None or df.empty:
        logging.warning('No tickets to update.')
        return
    logging.info('Updating tickets...')
    for index, row in df.iterrows():
        db.update_ticket(
            row=row,
        )
