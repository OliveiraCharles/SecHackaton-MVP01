import logging
import libs.database as db
import libs.report as dt
import libs.ticket as tk


def scan_reports(directory):
    logging.info('Scanning reports...')
    new_report = dt.merge(
        directory=directory,
        output_file_path='reports/output/merged.csv',
        column_names=db.get_column_names('reports'),
    )
    return new_report


def check_report(row, table_name='reports'):
    # if is duplicated or is information, not need to save
    result = not (is_duplicated(row, table_name) or is_information(row))
    # print("Needs to save?", result)
    return result


def create_ticket(row, project_key):
    issue_key = tk.create(row=row, project_key=project_key)
    return issue_key


def save_report(row, table_name):
    db.insert_finding(table_name=table_name, row=row)


def is_information(row):
    result = row['Result Severity'] == 'Information'
    # print("Is Information?", result)
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
    # print("Is duplicated?", result)
    return result


def scan_ticket():
    # Verifica tickets resolvidos
    df = db.execute_sql_query(
        query_sql="""
        SELECT * FROM tickets
        WHERE "Status" = 'Done'
        """
    )
    for index, row in df.iterrows():
        db.update_report_evaluation(row=row)

    return True


def save_new_report(new_report, table_name='reports'):

    for index, row in new_report.iterrows():

        df = db.execute_sql_query(
            query_sql=f"""
            SELECT * FROM {table_name}
            WHERE "SrcFileName"='{row['SrcFileName']}'
            AND "Name"='{row['Name']}'
            AND "Query"='{row['Query']}'
            """
        )

        if df.empty:
            issue_key = tk.create(row=row, project_key='RA')

            # Agora, atualize a coluna 'JiraTicketKey' no DataFrame
            new_report.at[index, 'JiraTicketKey'] = issue_key

            db.insert_finding(table_name=table_name, row=row)
