import logging
import os

import pandas as pd


def merge(directory, column_names, output_file_path=None):

    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    dfs = []

    for csv_file in csv_files:
        csv_path = os.path.join(directory, csv_file)
        df_chunk = pd.read_csv(csv_path)  # Leitura completa de cada arquivo
        dfs.append(df_chunk)

    df = pd.concat(dfs, ignore_index=True)

    df['Issue Evaluation'] = df.apply(
        lambda row: 'Not Evaluated'
        if pd.isnull(row['Evaluation'])
        else row['Evaluation'],
        axis=1,
    )

    df = df.reindex(columns=column_names, fill_value=None)

    if output_file_path:
        df.to_csv(output_file_path, index=False)

    return df


def deduplicate(
    df, columns_to_check, input_file_path=None, output_file_path=None
):

    logging.info('Iniciando a deduplicação...')
    if input_file_path:
        df = pd.read_csv(input_file_path)

    df = df.sort_values(by='Issue Evaluation')

    df = df.drop_duplicates(subset=columns_to_check, keep=False)

    if output_file_path:
        df.to_csv(output_file_path, index=False)

    logging.info('Deduplicação finalizada.')
    return df
