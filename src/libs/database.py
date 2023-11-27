import logging

import pandas as pd
from sqlalchemy import create_engine, text


def save(df, table_name, db_conn_str):
    logging.info(f"Salvando dados no Banco de dados em '{table_name}'...")

    engine = create_engine(db_conn_str)

    # Itera sobre cada linha do DataFrame e tenta inserir no banco de dados
    for index, row in df.iterrows():
        try:
            # logging.warning(f'inserindo o registro {index + 1}')

            # Inserção individual de cada linha no banco de dados
            row.to_frame().T.to_sql(
                table_name,
                con=engine,
                if_exists='append',
                index=False,
                method='multi',
            )

        except Exception as e:
            # Tratar erro de violação de chave primária
            logging.warning(
                f'Erro ao inserir o registro {index + 2}'
                + f': {str(e)}'
                # + f'Row: {row}'
            )

    logging.info('Dados salvos no Banco de dados com sucesso.')


def query(db_conn_str, query_sql, output_file_path=None):
    logging.info('Executando comando sql...')
    # Configuração da conexão com o PostgreSQL
    engine = create_engine(db_conn_str)

    # Sua consulta SQL
    query = text(query_sql)

    # Executa a consulta e obtém os resultados em um DataFrame
    df = pd.read_sql_query(query, engine)

    if output_file_path:
        # Salva o DataFrame em um arquivo CSV
        df.to_csv(output_file_path, index=False)

    logging.info('Comando sql Finalizado.')

    return df


def create_table_from_sql_file(arquivo_sql, conexao_bd):
    logging.info(f'Creating table from {arquivo_sql}')

    try:
        # Cria uma engine para a conexão com o banco de dados
        engine = create_engine(conexao_bd)

        # Lê o conteúdo do arquivo SQL
        with open(arquivo_sql, 'r') as arquivo:
            sql_script = arquivo.read()
        logging.info(f'SQL Script: \n\n{sql_script}')
        # Executa o script SQL usando a função text do SQLAlchemy
        with engine.connect() as con:
            con.execute(text(sql_script))

        logging.info('Tabela criada com sucesso!')

    except Exception as e:
        logging.info(f'Erro ao criar a tabela: {str(e)}')
