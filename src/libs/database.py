import logging

import pandas as pd
import sqlalchemy as sql

import config.settings as st

engine = sql.create_engine(
    st.db_conn_str,
    # echo=True
)

metadata = sql.MetaData()

reports_table = sql.Table(
    'reports',
    metadata,
    autoload_with=engine,
)


def get_column_names(table_name):

    inspector = sql.inspect(engine)
    columns = inspector.get_columns(table_name)
    column_names = [column['name'] for column in columns]

    return column_names


def fetch_all_records():
    logging.info('Buscando todos os registros no banco de dados...')
    with engine.connect() as conn:
        query = reports_table.select()
        result = conn.execute(query)
        records = result.fetchall()
        logging.info('Registros recuperados com sucesso.')
        return records


def save_to_database2(df, table_name, index_columns):
    logging.info(f"Salvando dados no Banco de dados em '{table_name}'...")
    with engine.connect() as conn:
        try:
            df.set_index(index_columns, inplace=True)
            df.to_sql(
                table_name,
                con=conn,
                if_exists='append',
                index=True,
                method='multi',
            )
            logging.info('Dados salvos no Banco de dados com sucesso.')

        except Exception as e:
            logging.error('Erro ao salvar dados no Banco de dados\n' + str(e))


def save_to_database(row, table_name):
    logging.info(f"!!!Salvando dados no Banco de dados em '{table_name}'...")

    with engine.connect() as conn:
        try:
            query = sql.text(
                f"""
                UPDATE {table_name}
                SET "Issue Evaluation" = '{row['Issue Evaluation']}'
                WHERE "Issue Key"='{row['Key']}'
                """
            )
            conn.execute(query)
            conn.commit()
            logging.info('Dados salvos no Banco de dados com sucesso.')

        except Exception as e:
            logging.error('Erro ao salvar dados no Banco de dados\n' + str(e))


def execute_sql_query(query_sql, output_file_path=None):
    # logging.info('Executando comando SQL...')
    try:
        with engine.connect() as conn:
            query = sql.text(query_sql)
            df = pd.read_sql(query, conn)
        if output_file_path:
            df.to_csv(output_file_path, index=False)
        # logging.info('Comando SQL Finalizado.')
    except Exception as e:
        logging.error('Erro ao efetuar query SQL:\n' + str(e))

    return df


def update_report(row):
    """
    Update the 'Issue Key' in the database for a specific row.

    Parameters:
    - issue_key: The issue key to be added.
    - row: Row information to identify the specific row to be updated.

    Returns:
    - Result of the SQL query.
    """
    # logging.info('Updating Issue Key in the database...')

    query = sql.text(
        """
        UPDATE reports
        SET "Issue Key" = :issue_key,
            "Issue Status" = :issue_status,
            "Issue Created" = :issue_created,
            "Issue Updated" = :issue_updated,
            "Issue Summary" = :issue_summary,
            "Issue Priority" = :issue_priority,
            "Issue Resolved" = :issue_resolved,
            "Issue Due Date" = :issue_due_date,
            "Issue Assignee" = :issue_assignee,
            "Issue Review Note" = :issue_review_note,
            "Issue Evaluation" = :issue_evaluation
        WHERE reports."Query" = :query
        AND reports."SrcFileName" = :src_file_name
        AND reports."Name" = :name
        """
    )

    try:
        with engine.connect() as conn:
            result = conn.execute(
                query,
                {
                    'issue_key': row['Issue Key'],
                    'issue_status': row['Issue Status'],
                    'issue_created': row['Issue Created'],
                    'issue_updated': row['Issue Updated'],
                    'issue_summary': row['Issue Summary'],
                    'issue_priority': row['Issue Priority'],
                    'issue_resolved': row['Issue Resolved'],
                    'issue_due_date': row['Issue Due Date'],
                    'issue_assignee': row['Issue Assignee'],
                    'issue_review_note': row['Issue Review Note'],
                    'issue_evaluation': row['Issue Evaluation'],
                    'query': row['Query'],
                    'src_file_name': row['SrcFileName'],
                    'name': row['Name'],
                },
            )
            conn.commit()
        return result

    except Exception as e:
        logging.error('Error updating reports\n' + str(e))


def update_ticket(row):
    query = sql.text(
        """
        UPDATE reports
        SET "Issue Key" = :issue_key,
            "Issue Status" = :issue_status,
            "Issue Created" = :issue_created,
            "Issue Updated" = :issue_updated,
            "Issue Summary" = :issue_summary,
            "Issue Priority" = :issue_priority,
            "Issue Resolved" = :issue_resolved,
            "Issue Due Date" = :issue_due_date,
            "Issue Assignee" = :issue_assignee,
            "Issue Review Note" = :issue_review_note,
            "Issue Evaluation" = :issue_evaluation
        WHERE "Issue Key" = :issue_key
        """
    )
    try:
        with engine.connect() as conn:
            result = conn.execute(
                query,
                {
                    'issue_key': row['Issue Key'],
                    'issue_evaluation': row['Issue Evaluation'],
                    'issue_status': row['Issue Status'],
                    'issue_created': row['Issue Created'],
                    'issue_updated': row['Issue Updated'],
                    'issue_summary': row['Issue Summary'],
                    'issue_priority': row['Issue Priority'],
                    'issue_resolved': row['Issue Resolved'],
                    'issue_due_date': row['Issue Due Date'],
                    'issue_assignee': row['Issue Assignee'],
                    'issue_review_note': row['Issue Review Note'],
                },
            )
            conn.commit()
        return result

    except Exception as e:
        logging.error('Error updating ticket on reports table\n' + str(e))


def add_issue_key(issue_key, row):
    logging.info('Adicionando issue Key...')

    query = sql.text(
        """
        UPDATE reports
        SET "Issue Key" = :issue_key
        WHERE reports."Query" = :query
        AND reports."SrcFileName" = :src_file_name
        AND reports."Name" = :name
        """
    )
    try:
        with engine.connect() as conn:
            result = conn.execute(
                query,
                {
                    'issue_key': issue_key,
                    'query': row['Query'],
                    'src_file_name': row['SrcFileName'],
                    'name': row['Name'],
                },
            )
            conn.commit()

    except Exception as e:
        logging.error('Erro ao executar comando SQL\n' + str(e))

    return result


def update_report_evaluation(row):
    logging.info('Updating Issue Evaluation...')
    with engine.connect() as conn:
        query = sql.text(
            """
        UPDATE reports
        SET "Issue Evaluation" = :evaluation
        WHERE reports."Issue Key" = :key
        """
        )
        parameters = {
            'evaluation': row['Evaluation'],
            'key': row['Issue Key'],
        }
        try:
            result = conn.execute(query, parameters)
            conn.commit()
            return result

        except Exception as e:
            logging.error('Erro ao executar comando SQL\n' + str(e))


def delete_record(query, src_file_name, name):
    logging.info('Excluindo registro do banco de dados...')
    with engine.connect() as conn:
        stmt = reports_table.delete().where(
            sql.and_(
                reports_table.c.Query == sql.bindparam('query'),
                reports_table.c.SrcFileName == sql.bindparam('src_file_name'),
                reports_table.c.Name == sql.bindparam('name'),
            )
        )
        result = conn.execute(
            stmt,
            {
                'query': query,
                'src_file_name': src_file_name,
                'name': name,
            },
        )
        logging.info('Registro excluído com sucesso.')
        return result


def insert_finding(table_name, row):
    try:
        with engine.connect() as conn:
            statement = sql.text(
                f"""
                INSERT INTO {table_name} (
                    "Query", "QueryPath", "Custom", "PCI DSS v3.2.1",
                    "OWASP Top 10 2013", "FISMA 2014", "NIST SP 800-53",
                    "OWASP Top 10 2017", "OWASP Mobile Top 10 2016",
                    "ASD STIG 4.10", "OWASP Top 10 API", "OWASP Top 10 2010",
                    "CWE top 25", "MOIS(KISA) Secure Coding 2021",
                    "OWASP ASVS", "OWASP Top 10 2021", "SANS top 25",
                    "SrcFileName", "Line", "Column", "NodeId", "Name",
                    "DestFileName", "DestLine", "DestColumn", "DestNodeId",
                    "DestName", "Result State", "Result Severity",
                    "Issue Key", "Assigned To", "Comment", "Link",
                    "Result Status", "Detection Date", "Issue Evaluation",
                    "Dev Team Review Note",
                    "Issue Status", "Issue Created", "Issue Updated",
                    "Issue Summary", "Issue Priority", "Issue Review Note",
                    "Issue Resolved", "Issue Due Date", "Issue Assignee"
                )
                VALUES (
                    :query, :query_path, :custom, :pci_dss, :owasp_2013,
                    :fisma, :nist, :owasp_2017, :owasp_mobile_2016, :asd_stig,
                    :owasp_api, :owasp_2010, :cwe_top_25, :mois, :owasp_asvs,
                    :owasp_2021, :sans_top_25, :src_file_name, :line, :column,
                    :node_id, :name, :dest_file_name, :dest_line,
                    :dest_column, :dest_node_id, :dest_name, :result_state,
                    :result_severity, :jira_ticket_key, :assigned_to, :comment,
                    :link, :result_status, :detection_date, :evaluation,
                    :dev_team_review_note,
                    :issue_status, :issue_created, :issue_updated,
                    :issue_summary, :issue_priority, :issue_review_note,
                    :issue_resolved, :issue_due_date, :issue_assignee
                )
            """
            )

            parameters = {
                'query': row['Query'],
                'query_path': row['QueryPath'],
                'custom': row['Custom'],
                'pci_dss': row['PCI DSS v3.2.1'],
                'owasp_2013': row['OWASP Top 10 2013'],
                'fisma': row['FISMA 2014'],
                'nist': row['NIST SP 800-53'],
                'owasp_2017': row['OWASP Top 10 2017'],
                'owasp_mobile_2016': row['OWASP Mobile Top 10 2016'],
                'asd_stig': row['ASD STIG 4.10'],
                'owasp_api': row['OWASP Top 10 API'],
                'owasp_2010': row['OWASP Top 10 2010'],
                'cwe_top_25': row['CWE top 25'],
                'mois': row['MOIS(KISA) Secure Coding 2021'],
                'owasp_asvs': row['OWASP ASVS'],
                'owasp_2021': row['OWASP Top 10 2021'],
                'sans_top_25': row['SANS top 25'],
                'src_file_name': row['SrcFileName'],
                'line': row['Line'],
                'column': row['Column'],
                'node_id': row['NodeId'],
                'name': row['Name'],
                'dest_file_name': row['DestFileName'],
                'dest_line': row['DestLine'],
                'dest_column': row['DestColumn'],
                'dest_node_id': row['DestNodeId'],
                'dest_name': row['DestName'],
                'result_state': row['Result State'],
                'result_severity': row['Result Severity'],
                'jira_ticket_key': row['Issue Key'],
                'assigned_to': row['Assigned To'],
                'comment': row['Comment'],
                'link': row['Link'],
                'result_status': row['Result Status'],
                'detection_date': row['Detection Date'],
                'evaluation': row['Issue Evaluation'],
                'dev_team_review_note': row['Dev Team Review Note'],
                'issue_status': row.get('Issue Status', None),
                'issue_created': row.get('Issue Created', None),
                'issue_updated': row.get('Issue Updated', None),
                'issue_summary': row.get('Issue Summary', None),
                'issue_priority': row.get('Issue Priority', None),
                'issue_review_note': row.get('Issue Review Note', None),
                'issue_resolved': row.get('Issue Resolved', None),
                'issue_due_date': row.get('Issue Due Date', None),
                'issue_assignee': row.get('Issue Assignee', None),
            }

            conn.execute(statement, parameters)
            conn.commit()

    except Exception as e:
        logging.error('Erro ao inserir finding:\n{}'.format(str(e)))
