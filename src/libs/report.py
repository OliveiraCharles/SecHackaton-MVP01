import logging
import os

import pandas as pd


def merge(directory, column_names, output_file_path=None):
    # logging.info('Iniciando a mesclagem de arquivos CSV....')

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
    df = pd.concat(dfs, ignore_index=True)
    df = df.reindex(columns=column_names, fill_value=None)

    if output_file_path:
        # Salva o DataFrame combinado em um único arquivo CSV
        df.to_csv(output_file_path, index=False)

    # logging.info('mesclagem de arquivos CSV finalizado.')

    return df


def deduplicate(
    df, columns_to_check, input_file_path=None, output_file_path=None
):

    logging.info('Iniciando a deduplicação...')
    if input_file_path:
        # Lê o arquivo CSV
        df = pd.read_csv(input_file_path)

    # Ordena o DataFrame pela coluna 'Evaluation'
    df = df.sort_values(by='Evaluation')

    # Remove as linhas duplicadas com base nas colunas especificadas
    df = df.drop_duplicates(subset=columns_to_check, keep=False)

    if output_file_path:
        # Salva o DataFrame combinado em um único arquivo CSV
        df.to_csv(output_file_path, index=False)

    logging.info('Deduplicação finalizada.')
    return df
