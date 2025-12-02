import pandas as pd
from sqlalchemy import create_engine, text, inspect
# Importa os tipos de dados do SQLAlchemy
from sqlalchemy.types import String, Date, Numeric
from decimal import Decimal, InvalidOperation
import io
import re
import os
from dotenv import load_dotenv

print("Iniciando processo de carga INCREMENTAL da FOPAG para o Postgres...")

# --- PASSO 1: CONFIGURAÇÃO DO BANCO ---
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_SCHEMA = os.getenv("DB_SCHEMA") # Schema que vou usar

connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(connection_string)
    inspector = inspect(engine)
    print(f"Conexão com o banco '{DB_NAME}' estabelecida com sucesso.")

    # Garante que o schema exista
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS \"{DB_SCHEMA}\""))
        print(f"Schema '{DB_SCHEMA}' verificado/criado com sucesso.")

except Exception as e:
    print(f"Erro ao conectar ou criar schema: {e}")
    exit()

# --- PASSO 2: DEFINIÇÃO EXPLÍCITA DOS SCHEMAS DAS TABELAS ---
# Define os tipos de coluna corretos para o SQL

schema_totais = {
    'competencia': Date(),
    'tipo_calculo': String(),
    'departamento': String(),
    'vinculo': String(),
    'nome_funcionario': String(),
    'situacao': String(),
    'data_demissao': Date(),
    'motivo_demissao': String(),
    'cargo': String(),
    'data_admissao': Date(),
    'cpf': String(11),
    'salario_contratual': Numeric(10, 2),
    'total_proventos': Numeric(10, 2),
    'total_descontos': Numeric(10, 2),
    'valor_liquido': Numeric(10, 2),
    'base_inss': Numeric(10, 2),
    'base_fgts': Numeric(10, 2),
    'valor_fgts': Numeric(10, 2),
    'base_irrf': Numeric(10, 2)
}

schema_rubricas = {
    'competencia': Date(),
    'tipo_calculo': String(),
    'departamento': String(),
    'vinculo': String(),
    'nome_funcionario': String(),
    'cpf': String(11),
    'codigo_rubrica': String(),
    'nome_rubrica': String(),
    'tipo_rubrica': String(),
    'valor_rubrica': Numeric(10, 2)
}

# --- PASSO 3: FUNÇÕES DE TRATAMENTO DE TIPOS (VERSÃO CORRIGIDA) ---
# (Suas funções clean_text, para_decimal, tratar_tipos_dataframe_csv permanecem IDÊNTICAS)
def clean_text(series):
    """Limpa uma série de texto (object) de forma segura."""
    if series.dtype == 'object':
        series = series.str.strip()
        series = series.str.replace(u'\xa0', '', regex=False)
        # Substitui "N/A" e strings vazias por None (que vira NULL)
        series = series.replace(['N/A', '', 'nan', 'None'], None)
    return series

def para_decimal(valor_str):
    """Converte uma string (já limpa) para Decimal."""
    if valor_str is None or pd.isna(valor_str):
        return None
    try:
        # A limpeza de \xa0 e strip já foi feita
        # Apenas removemos pontos e trocamos vírgula
        return Decimal(valor_str.replace(',', '.'))
    except (InvalidOperation, ValueError, TypeError):
        return None

def tratar_tipos_dataframe_csv(df, nome_arquivo):
    """
    Função de tratamento final, ponto-a-ponto, baseada na análise dos arquivos.
    """
    print(f"Iniciando tratamento de tipos para {nome_arquivo}...")

    # --- [CORREÇÃO] COLUNAS DE DATA (TODAS AS DATAS) ---
    colunas_data = ['competencia', 'data_admissao', 'data_demissao']

    for col in colunas_data:
        if col in df.columns:
            # 1. Limpa a coluna de texto
            print(f"Tratando tipo de data: {col}")
            df[col] = clean_text(df[col])

            # 2. Pega o primeiro valor para checar o formato
            first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None

            # 3. Aplica a regra de conversão correta
            if first_valid and '/' in first_valid:
                print(f"  -> Formato com '/' detectado. Usando regra DD/MM/YYYY ou MM/YYYY.")
                if col == 'competencia':
                    df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
                else:
                    df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

            elif first_valid and '-' in first_valid:
                print(f"  -> Formato com '-' detectado. Usando parser nativo (YYYY-MM-DD).")
                df[col] = pd.to_datetime(df[col], errors='coerce') # Parser nativo

            else:
                 print(f"  -> Nenhum formato detectado ou coluna vazia. Convertendo com parser nativo.")
                 df[col] = pd.to_datetime(df[col], errors='coerce') # Parser nativo

            # 4. Converte para AAAA-MM-DD para o banco
            df[col] = df[col].dt.date

    # --- COLUNAS MONETÁRIAS ---
    colunas_monetarias = [
        'salario_contratual', 'total_proventos', 'total_descontos',
        'valor_liquido', 'base_inss', 'base_fgts', 'valor_fgts',
        'base_irrf', 'valor_rubrica'
    ]
    for col in colunas_monetarias:
        if col in df.columns:
            print(f"Tratando tipo: {col} (String -> Decimal)")
            df[col] = clean_text(df[col]) # Limpa primeiro
            df[col] = df[col].apply(para_decimal) # Converte depois

    # --- COLUNA CPF ---
    if 'cpf' in df.columns:
        print("Tratando tipo: cpf (String -> String Limpa)")
        df['cpf'] = clean_text(df['cpf'])
        df['cpf'] = df['cpf'].str.replace(r'[^\d]', '', regex=True)

    # --- COLUNAS DE TEXTO RESTANTES ---
    colunas_texto = [
        'tipo_calculo', 'departamento', 'vinculo', 'nome_funcionario',
        'situacao', 'motivo_demissao', 'cargo', 'codigo_rubrica',
        'nome_rubrica', 'tipo_rubrica'
    ]
    for col in colunas_texto:
        if col in df.columns:
            print(f"Limpando coluna de texto: {col}")
            df[col] = clean_text(df[col])

    print("Tratamento de tipos finalizado.")
    return df

# --- [NOVA FUNÇÃO] PASSO 4: FUNÇÃO DE UPSERT PARA A DIMENSÃO BASE ---

# --- [FUNÇÃO CORRIGIDA] PASSO 4: FUNÇÃO DE UPSERT PARA A DIMENSÃO BASE ---

def atualizar_dim_colaboradores_base(engine, df_colaboradores, schema_name):
    """
    Cria a tabela dim_colaboradores_base (se não existir) e
    faz o UPSERT (INSERT ... ON CONFLICT) dos dados de colaboradores.
    """
    NOME_TABELA_BASE = "dim_colaboradores_base"
    NOME_TABELA_STAGING_TEMP = "stg_colab_pdf_temp"

    if df_colaboradores is None or df_colaboradores.empty:
        print("Nenhum dado de colaborador para atualizar na dim_colaboradores_base.")
        return

    print(f"\n--- Iniciando UPSERT para '{NOME_TABELA_BASE}' ---")

    # SQL para criar a tabela base (permanece o mesmo)
    sql_create_base = text(f"""
        CREATE TABLE IF NOT EXISTS \"{schema_name}\".\"{NOME_TABELA_BASE}\" (
            colaborador_sk SERIAL PRIMARY KEY,
            nome_colaborador VARCHAR(255) NOT NULL,
            cpf VARCHAR(20) UNIQUE NOT NULL
        );
        INSERT INTO \"{schema_name}\".\"{NOME_TABELA_BASE}\" (colaborador_sk, nome_colaborador, cpf)
        VALUES (0, 'Desconhecido', 'N/A')
        ON CONFLICT (colaborador_sk) DO NOTHING;
    """)

    # --- [SQL CORRIGIDO] ---
    # Adicionamos 'DISTINCT ON (src.cpf)' para garantir que apenas um
    # registro por CPF seja selecionado do staging.
    # O 'ORDER BY src.cpf, src.nome_colaborador DESC' garante que,
    # se houver nomes diferentes, ele pegue o último em ordem alfabética.
    sql_upsert = text(f"""
        INSERT INTO \"{schema_name}\".\"{NOME_TABELA_BASE}\" (nome_colaborador, cpf)
        SELECT
            DISTINCT ON (src.cpf) -- <-- CORREÇÃO APLICADA
            src.nome_colaborador,
            src.cpf
        FROM
            \"{schema_name}\".\"{NOME_TABELA_STAGING_TEMP}\" AS src
        WHERE
            src.cpf IS NOT NULL AND src.cpf != 'N/A'
        ORDER BY
            src.cpf, src.nome_colaborador DESC -- <-- 'ORDER BY' é necessário para o 'DISTINCT ON'
        ON CONFLICT (cpf) DO UPDATE SET
            nome_colaborador = EXCLUDED.nome_colaborador;
    """)
    # --- [FIM DA CORREÇÃO] ---

    try:
        with engine.begin() as conn:
            # 1. Cria a tabela base (se não existir)
            conn.execute(sql_create_base)

            # 2. Carga dos dados do DataFrame para a tabela temporária de staging
            # (Não precisamos mais do pandas .drop_duplicates() aqui, pois o SQL resolve)
            df_colaboradores.to_sql(
                NOME_TABELA_STAGING_TEMP,
                con=conn,
                schema=schema_name,
                if_exists='replace',
                index=False
            )

            # 3. Executa o UPSERT (agora corrigido)
            conn.execute(sql_upsert)

            # 4. (Opcional) Limpa a tabela temporária
            conn.execute(text(f"DROP TABLE \"{schema_name}\".\"{NOME_TABELA_STAGING_TEMP}\""))

        print(f"SUCESSO! '{NOME_TABELA_BASE}' foi atualizada com os dados do PDF.")

    except Exception as e:
        print(f"ERRO ao fazer UPSERT na '{NOME_TABELA_BASE}': {e}")

# (O restante da sua célula 2, incluindo o loop principal, permanece IDÊNTICO)
# ... (clean_text, para_decimal, tratar_tipos_dataframe_csv) ...
# ... (arquivos_para_carregar, Loop de ETL) ...


# --- PASSO 5: DEFINIÇÃO DOS ARQUIVOS ---
arquivos_para_carregar = {
    'BASE_FOPAG_CONSOLIDADA_TOTAIS.csv': 'fopag_totais',
    'BASE_FOPAG_DETALHADA_RUBRICAS.csv': 'fopag_rubricas_detalhe'
}

# --- PASSO 6: LOOP DE ETL COM LÓGICA DE SCHEMA E DTYPE ---
for nome_arquivo, nome_tabela in arquivos_para_carregar.items():

    print(f"\n--- Processando arquivo: {nome_arquivo} ---")

    try:
        # --- (E) EXTRACT ---
        df = pd.read_csv(
            nome_arquivo,
            sep=';',
            na_values=['N/A', 'NaN', ''],
            dtype=str  # Força tudo a ser lido como string
        )

        # --- (T) TRANSFORM ---
        df_tratado = tratar_tipos_dataframe_csv(df.copy(), nome_arquivo)

        print(f"--- Info de {nome_arquivo} (APÓS tratamento):")
        df_tratado.info() # Mostra os tipos de dados convertidos

        # --- [NOVA ETAPA] ATUALIZAR DIM_COLABORADORES_BASE ---
        # Usamos apenas o arquivo de totais para não repetir o processo
        if nome_tabela == 'fopag_totais':
            # Extrai colabores únicos do DF tratado
            df_colabs_unicos = df_tratado[['cpf', 'nome_funcionario']].drop_duplicates().dropna(subset=['cpf'])
            df_colabs_unicos = df_colabs_unicos.rename(columns={'nome_funcionario': 'nome_colaborador'})

            # Chama a nova função de UPSERT
            atualizar_dim_colaboradores_base(engine, df_colabs_unicos, DB_SCHEMA)
        # --- [FIM DA NOVA ETAPA] ---


        dtype_map = schema_totais if nome_tabela == 'fopag_totais' else schema_rubricas

        competencias_no_df = df_tratado['competencia'].dropna().unique()

        if len(competencias_no_df) == 0:
            print(f"AVISO: Nenhuma competência válida encontrada após tratamento. Pulando carga.")
            print("Amostra da coluna 'competencia' ORIGINAL (antes da falha):")
            print(df['competencia'].head(10))
            continue

        print(f"Competências a serem carregadas: {competencias_no_df}")

        # --- (L) LOAD ---

        table_exists = inspector.has_table(nome_tabela, schema=DB_SCHEMA)

        if table_exists:
            print(f"Tabela '\"{DB_SCHEMA}\".\"{nome_tabela}\"' já existe. Executando Delete-then-Append...")

            with engine.begin() as conn:
                sql_delete = text(f"""
                    DELETE FROM "{DB_SCHEMA}"."{nome_tabela}"
                    WHERE competencia IN :competencias_list
                """)
                conn.execute(sql_delete, {"competencias_list": tuple(competencias_no_df)})
                print("DELETE executado. Iniciando INSERT (append)...")

                df_tratado.to_sql(
                    name=nome_tabela,
                    con=conn,
                    if_exists='append',
                    index=False,
                    schema=DB_SCHEMA,
                    dtype=dtype_map
                )

                print(f"SUCESSO! Dados (append) carregados na tabela '\"{DB_SCHEMA}\".\"{nome_tabela}\"'.")

        else:
            print(f"Tabela '\"{DB_SCHEMA}\".\"{nome_tabela}\"' não existe. Criando e inserindo (primeira carga)...")

            df_tratado.to_sql(
                name=nome_tabela,
                con=engine,
                if_exists='fail',
                index=False,
                schema=DB_SCHEMA,
                dtype=dtype_map # Essencial: Cria a tabela com os tipos corretos
            )

            print(f"SUCESSO! Tabela '\"{DB_SCHEMA}\".\"{nome_tabela}\"' criada com tipos corretos e dados carregados.")

    except FileNotFoundError:
        print(f"ERRO: Arquivo '{nome_arquivo}' não encontrado.")
    except Exception as e:
        print(f"ERRO GERAL ao processar o arquivo '{nome_arquivo}': {e}")

print("\nProcesso de carga incremental finalizado.")
