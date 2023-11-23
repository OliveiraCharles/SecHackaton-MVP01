import libs.functions as fc
from config.settings import (
    columns_to_check,
    merged_file_path,
    postgres_conn_str,
    reports_directory,
    table_name,
)

if __name__ == '__main__':

    # Chama a função para mesclar os arquivos CSV
    merged_dataframe = fc.merge_csv_files(reports_directory, merged_file_path)

    # Chama a função para criar a tabela 'reports' no PostgreSQL
    fc.create_reports_table(postgres_conn_str, columns_to_check)

    # Chama a função para salvar os dados no PostgreSQL
    fc.save_reports_to_postgres(
        merged_dataframe, table_name, postgres_conn_str
    )
