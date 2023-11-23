import logging
import os

import pandas as pd
from sqlalchemy import (
    Column,
    Date,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    create_engine,
)


def merge_csv_files(directory, output_file):
    logging.info('Iniciando a mesclagem de arquivos CSV.')

    # Lista todos os arquivos CSV no diretório
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    # Inicializa um DataFrame vazio
    dfs = []

    # Itera sobre todos os arquivos CSV no diretório e lê-os incrementalmente
    for csv_file in csv_files:
        csv_path = os.path.join(directory, csv_file)
        df_chunk = pd.read_csv(csv_path)  # Leitura completa de cada arquivo
        dfs.append(df_chunk)

    # Concatena os DataFrames
    merged_df = pd.concat(dfs, ignore_index=True)

    # Colunas para verificar duplicatas
    columns_to_check = ['SrcFileName', 'Name', 'Query']

    # Reseta o índice
    merged_df.reset_index(drop=True, inplace=True)

    # Adiciona a tag "Duplicated" à coluna de rótulos para linhas duplicadas
    duplicates_mask = merged_df.duplicated(subset=columns_to_check, keep=False)
    merged_df['Labels'] = ''
    merged_df.loc[duplicates_mask, 'Labels'] = 'Duplicated'

    # Salva o DataFrame combinado em um único arquivo CSV
    merged_df.to_csv(output_file, index=False)

    return merged_df


def create_reports_table(postgres_conn_str, primary_key_columns):
    logging.info("Criando a tabela 'reports' no PostgreSQL.")

    engine = create_engine(postgres_conn_str)
    metadata = MetaData()

    reports_table = Table(
        'reports',
        metadata,
        Column('Query', String(255)),
        Column('QueryPath', String(255)),
        Column('Custom', String(255)),
        Column('PCI DSS v3.2.1', String(255)),
        Column('OWASP Top 10 2013', String(255)),
        Column('FISMA 2014', String(255)),
        Column('NIST SP 800-53', String(255)),
        Column('OWASP Top 10 2017', String(255)),
        Column('OWASP Mobile Top 10 2016', String(255)),
        Column('ASD STIG 4.10', String(255)),
        Column('OWASP Top 10 API', String(255)),
        Column('OWASP Top 10 2010', String(255)),
        Column('CWE top 25', String(255)),
        Column('MOIS(KISA) Secure Coding 2021', String(255)),
        Column('OWASP ASVS', String(255)),
        Column('OWASP Top 10 2021', String(255)),
        Column('SANS top 25', String(255)),
        Column('SrcFileName', String(255)),
        Column('Line', Integer),
        Column('Column', Integer),
        Column('NodeId', Integer),
        Column('Name', String(255)),
        Column('DestFileName', String(255)),
        Column('DestLine', Integer),
        Column('DestColumn', Integer),
        Column('DestNodeId', Integer),
        Column('DestName', String(255)),
        Column('Result State', String(255)),
        Column('Result Severity', String(255)),
        Column('Assigned To', String(255)),
        Column('Comment', Text),
        Column('Link', String(255)),
        Column('Result Status', String(255)),
        Column('Detection Date', Date),
        Column('Evaluation', String(255)),
        Column('Dev Team Review Note', String(255)),
        Column('Labels', String(255)),
    )

    # Adicionando uma restrição de chave primária nas colunas especificadas
    primary_key_constraint = PrimaryKeyConstraint(
        *[reports_table.columns[col] for col in primary_key_columns]
    )

    reports_table.append_constraint(primary_key_constraint)

    metadata.create_all(engine)
    logging.info("Tabela 'reports' criada com sucesso.")


def save_reports_to_postgres(df, table_name, postgres_conn_str):
    logging.info(f"Salvando dados no PostgreSQL na tabela '{table_name}'.")

    engine = create_engine(postgres_conn_str)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    logging.info('Dados salvos no PostgreSQL com sucesso.')


if __name__ == '__main__':
    # Diretório contendo os relatórios CSV
    reports_directory = './reports'

    # Configuração da conexão com o PostgreSQL
    postgres_conn_str = 'postgresql://postgres:postgres@localhost:5432/hcktn'

    # Chamada da função para criar a tabela reports
    create_reports_table(postgres_conn_str)

    # Chamada da função para salvar os relatórios no PostgreSQL
    save_reports_to_postgres(reports_directory, postgres_conn_str)
