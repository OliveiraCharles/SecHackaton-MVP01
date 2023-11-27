import config.settings as st
import libs.database as db
import libs.dataset as dt
import libs.ticket as tk

if __name__ == '__main__':

    # Baixa tabela
    db.query(
        db_conn_str=st.db_conn_str,
        query_sql="""
        SELECT *
        FROM reports
        """,
        output_file_path='reports/_db_import.csv',
    )

    # Mescla os reports
    df = dt.merge(
        directory='reports', output_file_path='reports/output/merged.csv'
    )

    # Remove duplicatas
    df = dt.deduplicate(
        df=df,
        columns_to_check=['SrcFileName', 'Name', 'Query'],
        output_file_path='reports/output/deduplicated.csv',
    )

    # Salva novos findings no banco de dados
    db.save(df=df, table_name='reports', db_conn_str=st.db_conn_str)

    # Prepara tabela de findings a serem avaliados
    df = db.query(
        db_conn_str=st.db_conn_str,
        query_sql="""
        SELECT *
        FROM reports
        WHERE "Evaluation" IS NULL
        AND "JiraTicketKey" IS NULL
        limit 5
        """,
        output_file_path='reports/output/sql_import.csv',
    )

    jira = tk.connect(server=st.jira_url, auth=st.jira_auth)

    tk.create(df=df, jira=jira, project_key='RA')
