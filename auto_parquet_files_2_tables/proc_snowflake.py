create procedure PROC_PARQUET_FILES_2_TABLES()
    returns String
    language python
    runtime_version = 3.11
    packages =('snowflake-snowpark-python')
    handler = 'main' comment = 'Procedure para criar tabelas baseadas em arquivos parquet e fazer Copy Into dos dados.'
    as '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, regexp_extract, lit, lower, upper, when, regexp_replace


STAGE_PATH = """''@"AUTO_PARQUET_FILES_2_TABLES"."PUBLIC"."PARQUET_FILES"/''"""
FILE_FORMAT = ''AUTO_PARQUET_FILES_2_TABLES.PUBLIC.FF_PARQUET''
TARGET_SCHEMA = ''AUTO_PARQUET_FILES_2_TABLES.PUBLIC''


def list_files(session):
    df = session.sql(f"LIST {STAGE_PATH}")

    files_df = df.select(
        col(''"name"'').alias("NAME"),
        col(''"last_modified"'').alias("LAST_MODIFIED"),

        # Nome completo do arquivo
        regexp_extract(col(''"name"''), r"/([^/]+)$", 1).alias("FILE_NAME"),
        upper(
            regexp_extract(col(''"name"''), r"/([^/]+)/[^/]+$", 1)
        ).alias("TABLE_NAME") # Pegando da pasta que antecede o arquivo
        # Nome da tabela (sem extensão, sem espaços), pegando do arquivo
        # upper(
        #     regexp_replace(
        #         regexp_replace(
        #             regexp_extract(col(''"name"''), r"/([^/]+)$", 1),
        #             r"\\.[^.]+$",
        #             ""
        #         ),
        #         r"\\s+",
        #         ""
        #     )
        # ).alias("TABLE_NAME")
    )

    return files_df


def infer_schema(session):
    return session.sql(f"""
        SELECT
            COLUMN_NAME,
            TYPE,
            FILENAMES
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => {STAGE_PATH},
                FILE_FORMAT => ''{FILE_FORMAT}'',
                IGNORE_CASE => TRUE
            )
        )
        ORDER BY FILENAMES, ORDER_ID
    """)


def create_tables_if_not_exists(session, df_infer_schema, df_tables):
    tables = [r["TABLE_NAME"] for r in df_tables.collect()]
    
    for table in tables:
        # Filtra colunas apenas do arquivo daquela tabela
        print(table)
        cols_df = (
            df_infer_schema
            .filter(upper(regexp_extract(col("FILENAMES"), r"^([^/]+)/", 1)) == table).select("COLUMN_NAME", "TYPE")
        )
        
        columns = cols_df.collect()
        # print(columns)
        if not columns:
            continue

        # Monta definição das colunas
        cols_sql = ",\\n".join(
            [f''"{c["COLUMN_NAME"]}" {c["TYPE"]}'' for c in columns]
        )

        # Colunas de metadados
        metadata_cols = """
            ,_FILE_NAME STRING
            ,_LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        """

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}."{table}" (
                {cols_sql}
                {metadata_cols}
            )
        """
        print(create_sql)
        session.sql(create_sql).collect()


# Dependendo de como estiver os arquivos essa função deverá ser alterada, por exemplo é possível
# passar por parâmetro no copy into o nome dos arquivos.
# Caso as colunas não possam ter caracteres especiais deverá ser adicionado explicitamente as colunas com os operadores $1, $2.... com o rename da coluna
# Para fazer o Load.
def create_copy_commands(session, df_tables):
    tables = [r["TABLE_NAME"] for r in df_tables.collect()]

    for table in tables:
        stg_path = f''{STAGE_PATH}{table}/''
        copy_sql = f"""
            COPY INTO {TARGET_SCHEMA}."{table}" 
                FROM {STAGE_PATH}
            FILE_FORMAT = (FORMAT_NAME = ''{FILE_FORMAT}'')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE 
            INCLUDE_METADATA = (_FILE_NAME = METADATA$FILENAME, _LOAD_TS = METADATA$FILE_LAST_MODIFIED)
            PATTERN = ''.*{table}.*''
            
        """
            # Lembrar de adicionar o Purge
            # PURGE = TRUE
        session.sql(copy_sql).collect()


def main(session: snowpark.Session):

    df_list_f = list_files(session)
    df_infer_schema = infer_schema(session)
    # table = ''IRISFLOWERDATASET''

    # cols_df = (
    #         df_infer_schema
    #         .filter(upper(regexp_extract(col("FILENAMES"), r"^([^/]+)/", 1)) == table).select("COLUMN_NAME", "TYPE")
    #     )
    # return cols_df
    
    df_tables = (
        df_list_f
        .select("TABLE_NAME")
        .distinct()
    )

    # Cria as tabelas
    create_tables_if_not_exists(session, df_infer_schema, df_tables)

    # Faz o load e purge
    create_copy_commands(session, df_tables)

    return "Processo finalizado com sucesso"
    ';