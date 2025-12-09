import pandas as pd
from sqlalchemy import text
from .constants import SCHEMA_TOTAIS, SCHEMA_RUBRICAS


def garantir_schema_banco(engine, schema_name):
    """
    Garante que o schema e a extensão unaccent existam no banco.
    """
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
        conn.execute(text(f'CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA "{schema_name}"'))


# --------------------------------------------------------------------------------
# DIMENSÃO CALENDÁRIO
# --------------------------------------------------------------------------------
def carregar_dim_calendario(engine, schema):
    nome_tabela = "dim_calendario"
    sql = text(f"""
    CREATE TABLE IF NOT EXISTS "{schema}".{nome_tabela} (
        data DATE PRIMARY KEY,
        ano INTEGER, mes INTEGER, dia INTEGER, trimestre INTEGER, semestre INTEGER,
        dia_da_semana INTEGER, nome_dia_da_semana VARCHAR(20),
        nome_mes VARCHAR(20), nome_mes_abrev CHAR(3), ano_mes VARCHAR(7), 
        dia_do_ano INTEGER, semana_do_ano INTEGER
    );
    DO $$
    DECLARE
        data_inicio DATE := '2023-01-01'; 
        data_fim DATE := '2030-12-31';
    BEGIN
        BEGIN
            SET LOCAL lc_time = 'pt_BR.UTF-8';
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                SET LOCAL lc_time = 'pt_BR';
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Não foi possível definir o locale pt_BR.';
            END;
        END;

        INSERT INTO "{schema}".{nome_tabela} (
            data, ano, mes, dia, trimestre, semestre,
            dia_da_semana, nome_dia_da_semana, nome_mes, nome_mes_abrev,
            ano_mes, dia_do_ano, semana_do_ano
        )
        SELECT
            d AS data,
            EXTRACT(YEAR FROM d) AS ano,
            EXTRACT(MONTH FROM d) AS mes,
            EXTRACT(DAY FROM d) AS dia,
            EXTRACT(QUARTER FROM d) AS trimestre,
            CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END AS semestre,
            EXTRACT(DOW FROM d) AS dia_da_semana, 
            to_char(d, 'TMDay') AS nome_dia_da_semana,
            to_char(d, 'TMMonth') AS nome_mes,
            to_char(d, 'TMMon') AS nome_mes_abrev,
            to_char(d, 'YYYY-MM') AS ano_mes,
            EXTRACT(DOY FROM d) AS dia_do_ano,
            EXTRACT(WEEK FROM d) AS semana_do_ano
        FROM generate_series(data_inicio, data_fim, '1 day'::interval) d
        ON CONFLICT (data) DO NOTHING; 
    END $$;
    """)

    with engine.begin() as conn:
        conn.execute(sql)
    print("Dimensão Calendário verificada/atualizada.")


# --------------------------------------------------------------------------------
# CARGA FATOS DE FOLHA (PDFs)
# --------------------------------------------------------------------------------
def carregar_fatos_folha(df_consol, df_detalhe, engine, schema):
    """
    Carrega as tabelas fato_folha_consolidada e fato_folha_detalhada.
    """

    # --- Parte A: Popular/Atualizar Dimensão Base com dados do CSV ---
    if not df_consol.empty:
        cols_base = ['cpf', 'nome_funcionario', 'data_admissao', 'data_demissao', 'situacao', 'departamento', 'cargo']
        df_base_load = df_consol[cols_base].copy().rename(columns={
            'nome_funcionario': 'nome_colaborador',
            'data_admissao': 'data_admissao_csv',
            'data_demissao': 'data_demissao_csv',
            'situacao': 'situacao_csv',
            'departamento': 'departamento_csv',
            'cargo': 'cargo_csv'
        })

        # Staging
        # O Pandas agora manda None real (NULL), então o SQL pode ser direto
        df_base_load.to_sql("stg_base_csv_temp", engine, schema=schema, if_exists='replace', index=False)

        sql_base = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."dim_colaboradores_base" (
            colaborador_sk SERIAL PRIMARY KEY,
            nome_colaborador VARCHAR(255) NOT NULL,
            cpf VARCHAR(20) UNIQUE NOT NULL,
            data_admissao_csv DATE, data_demissao_csv DATE,
            situacao_csv VARCHAR(100), departamento_csv VARCHAR(255), cargo_csv VARCHAR(255)
        );
        INSERT INTO "{schema}"."dim_colaboradores_base" (colaborador_sk, nome_colaborador, cpf)
        VALUES (0, 'Desconhecido', 'N/A') ON CONFLICT (colaborador_sk) DO NOTHING;

        INSERT INTO "{schema}"."dim_colaboradores_base" (
            nome_colaborador, cpf, 
            data_admissao_csv, data_demissao_csv, situacao_csv, 
            departamento_csv, cargo_csv
        )
        SELECT DISTINCT ON (cpf)
            nome_colaborador, cpf,
            data_admissao_csv, 
            data_demissao_csv, 
            situacao_csv, departamento_csv, cargo_csv
        FROM "{schema}"."stg_base_csv_temp"
        WHERE cpf IS NOT NULL AND cpf != 'N/A'
        ORDER BY cpf, nome_colaborador DESC
        ON CONFLICT (cpf) DO UPDATE SET
            nome_colaborador = EXCLUDED.nome_colaborador,
            data_admissao_csv = COALESCE(EXCLUDED.data_admissao_csv, "{schema}"."dim_colaboradores_base".data_admissao_csv),
            data_demissao_csv = COALESCE(EXCLUDED.data_demissao_csv, "{schema}"."dim_colaboradores_base".data_demissao_csv),
            situacao_csv = COALESCE(EXCLUDED.situacao_csv, "{schema}"."dim_colaboradores_base".situacao_csv),
            departamento_csv = COALESCE(EXCLUDED.departamento_csv, "{schema}"."dim_colaboradores_base".departamento_csv),
            cargo_csv = COALESCE(EXCLUDED.cargo_csv, "{schema}"."dim_colaboradores_base".cargo_csv);

        DROP TABLE IF EXISTS "{schema}"."stg_base_csv_temp";
        """

        with engine.begin() as conn:
            conn.execute(text(sql_base))
            print("Dimensão Colaboradores Base atualizada via CSV.")

    # --- Parte B: Fato Consolidada ---
    if not df_consol.empty:
        comps_consol = tuple(df_consol['competencia'].dropna().unique())
        if comps_consol:
            # dtype=SCHEMA_TOTAIS agora é seguro porque o Python limpou as datas
            df_consol.to_sql("stg_folha_consol", engine, schema=schema, if_exists='replace', index=False,
                             dtype=SCHEMA_TOTAIS)

            sql_consol = f"""
                CREATE TABLE IF NOT EXISTS "{schema}"."fato_folha_consolidada" (
                    fato_folha_id SERIAL PRIMARY KEY,
                    colaborador_sk INTEGER, competencia DATE,
                    nome_funcionario_csv VARCHAR(255), centro_de_custo VARCHAR(255), 
                    cargo_nome_csv VARCHAR(255), cpf_csv VARCHAR(11),
                    situacao_csv VARCHAR(100), tipo_calculo_csv VARCHAR(100),
                    salario_contratual NUMERIC(12, 2), total_proventos NUMERIC(12, 2),
                    total_descontos NUMERIC(12, 2), valor_liquido NUMERIC(12, 2),
                    base_inss NUMERIC(12, 2), base_fgts NUMERIC(12, 2),
                    valor_fgts NUMERIC(12, 2), base_irrf NUMERIC(12, 2),
                    FOREIGN KEY (colaborador_sk) REFERENCES "{schema}"."dim_colaboradores_base"(colaborador_sk)
                );

                DELETE FROM "{schema}"."fato_folha_consolidada" WHERE competencia IN :comps;

                INSERT INTO "{schema}"."fato_folha_consolidada" (
                    colaborador_sk, competencia, nome_funcionario_csv, centro_de_custo, 
                    cargo_nome_csv, cpf_csv, situacao_csv, tipo_calculo_csv,
                    salario_contratual, total_proventos, total_descontos, valor_liquido,
                    base_inss, base_fgts, valor_fgts, base_irrf
                )
                SELECT
                    COALESCE(base.colaborador_sk, 0), 
                    stg.competencia, 
                    stg.nome_funcionario, stg.departamento,
                    stg.cargo, stg.cpf, stg.situacao, stg.tipo_calculo,
                    stg.salario_contratual, stg.total_proventos, stg.total_descontos, stg.valor_liquido,
                    stg.base_inss, stg.base_fgts, stg.valor_fgts, stg.base_irrf
                FROM "{schema}"."stg_folha_consol" stg
                LEFT JOIN "{schema}"."dim_colaboradores_base" base ON stg.cpf = base.cpf;
            """
            with engine.begin() as conn:
                conn.execute(text(sql_consol), {'comps': comps_consol})
            print("Fato Consolidada carregada.")

    # --- Parte C: Fato Detalhada ---
    if not df_detalhe.empty:
        comps_det = tuple(df_detalhe['competencia'].dropna().unique())
        if comps_det:
            df_detalhe.to_sql("stg_folha_detalhe", engine, schema=schema, if_exists='replace', index=False,
                              dtype=SCHEMA_RUBRICAS)

            sql_detalhe = f"""
                CREATE TABLE IF NOT EXISTS "{schema}"."fato_folha_detalhada" (
                    fato_rubrica_id SERIAL PRIMARY KEY,
                    colaborador_sk INTEGER, competencia DATE,
                    nome_funcionario_csv VARCHAR(255), centro_de_custo VARCHAR(255), cpf_csv VARCHAR(11),
                    situacao_csv VARCHAR(100), tipo_calculo_csv VARCHAR(100),
                    codigo_rubrica VARCHAR(100), nome_rubrica VARCHAR(255), tipo_rubrica VARCHAR(100),
                    valor_rubrica NUMERIC(12, 2),
                    FOREIGN KEY (colaborador_sk) REFERENCES "{schema}"."dim_colaboradores_base"(colaborador_sk)
                );

                DELETE FROM "{schema}"."fato_folha_detalhada" WHERE competencia IN :comps;

                INSERT INTO "{schema}"."fato_folha_detalhada" (
                    colaborador_sk, competencia, nome_funcionario_csv, centro_de_custo, cpf_csv,
                    situacao_csv, tipo_calculo_csv, codigo_rubrica, nome_rubrica, tipo_rubrica, valor_rubrica
                )
                SELECT
                    COALESCE(base.colaborador_sk, 0), 
                    stg.competencia, 
                    stg.nome_funcionario, stg.departamento, stg.cpf,
                    stg.situacao, stg.tipo_calculo, stg.codigo_rubrica, stg.nome_rubrica, stg.tipo_rubrica, 
                    stg.valor_rubrica
                FROM "{schema}"."stg_folha_detalhe" stg
                LEFT JOIN "{schema}"."dim_colaboradores_base" base ON stg.cpf = base.cpf;
            """
            with engine.begin() as conn:
                conn.execute(text(sql_detalhe), {'comps': comps_det})
            print("Fato Detalhada carregada.")


# --------------------------------------------------------------------------------
# CARGA API (COLABORADORES + BENEFÍCIOS) - UPSERT COMPLETO
# --------------------------------------------------------------------------------
def carregar_dados_api(df_staging, df_beneficios, engine, schema):
    if df_staging.empty:
        print("DataFrame de colaboradores vazio. Nada a carregar.")
        return

    # Garante que os DataFrames tenham as colunas esperadas
    df_staging['cpf'] = df_staging['cpf'].astype(str).replace(['nan', 'None'], None)
    df_beneficios['colaborador_id_solides'] = df_beneficios['colaborador_id_solides'].astype(float).astype('Int64')

    # Helper SQL para tratamento numérico seguro
    def to_num(col):
        return f"CAST(NULLIF(REGEXP_REPLACE(CAST({col} AS TEXT), '[^0-9.-]', '', 'g'), '') AS NUMERIC)"

    NOME_TABELA_RICA = "dim_colaboradores"
    NOME_TABELA_BASE = "dim_colaboradores_base"
    NOME_TABELA_STAGING = "staging_colaboradores"
    NOME_STAGING_BEN = "staging_beneficios_api"
    NOME_FATO_BEN = "fato_beneficios_api"

    try:
        # Carga Staging
        print(f"Carregando {NOME_TABELA_STAGING}...")
        df_staging.to_sql(NOME_TABELA_STAGING, engine, if_exists='replace', index=False, schema=schema)

        print(f"Carregando {NOME_STAGING_BEN}...")
        df_beneficios.to_sql(NOME_STAGING_BEN, engine, if_exists='replace', index=False, schema=schema)

        sql = f"""
        -- 1. Base (Garante existência dos CPFs)
        CREATE TABLE IF NOT EXISTS "{schema}".{NOME_TABELA_BASE} (
            colaborador_sk SERIAL PRIMARY KEY, nome_colaborador VARCHAR(255), cpf VARCHAR(20) UNIQUE,
            data_admissao_csv DATE, data_demissao_csv DATE, situacao_csv VARCHAR(100),
            departamento_csv VARCHAR(255), cargo_csv VARCHAR(255)
        );
        INSERT INTO "{schema}".{NOME_TABELA_BASE} (colaborador_sk, nome_colaborador, cpf)
        VALUES (0, 'Desconhecido', 'N/A') ON CONFLICT (colaborador_sk) DO NOTHING;

        ALTER TABLE "{schema}".{NOME_TABELA_BASE}
            ADD COLUMN IF NOT EXISTS data_admissao_csv DATE, ADD COLUMN IF NOT EXISTS data_demissao_csv DATE,
            ADD COLUMN IF NOT EXISTS situacao_csv VARCHAR(100), ADD COLUMN IF NOT EXISTS departamento_csv VARCHAR(255),
            ADD COLUMN IF NOT EXISTS cargo_csv VARCHAR(255);

        INSERT INTO "{schema}".{NOME_TABELA_BASE} (nome_colaborador, cpf)
        SELECT DISTINCT ON (stg.cpf) stg.nome_completo, stg.cpf
        FROM "{schema}".{NOME_TABELA_STAGING} AS stg
        WHERE stg.cpf IS NOT NULL AND stg.cpf != 'N/A' AND stg.cpf != 'nan'
        ORDER BY stg.cpf, stg.colaborador_id_solides DESC 
        ON CONFLICT (cpf) DO UPDATE SET nome_colaborador = EXCLUDED.nome_colaborador;

        -- 2. Dimensão Rica (Criação se não existir)
        CREATE TABLE IF NOT EXISTS "{schema}".{NOME_TABELA_RICA} (
            colaborador_sk INTEGER PRIMARY KEY, colaborador_id_solides INTEGER UNIQUE NOT NULL, 
            cpf VARCHAR(11), nome_completo VARCHAR(255), data_nascimento DATE, genero VARCHAR(50),
            data_admissao DATE, data_demissao DATE, ativo BOOLEAN,
            departamento_nome_api VARCHAR(255), cargo_nome_api VARCHAR(255), email VARCHAR(255),
            data_ultima_atualizacao TIMESTAMP DEFAULT current_timestamp,
            FOREIGN KEY (colaborador_sk) REFERENCES "{schema}".{NOME_TABELA_BASE}(colaborador_sk)
        );

        ALTER TABLE "{schema}".{NOME_TABELA_RICA} DROP COLUMN IF EXISTS total_benefits_api;

        ALTER TABLE "{schema}".{NOME_TABELA_RICA}
            ADD COLUMN IF NOT EXISTS matricula VARCHAR(50),
            ADD COLUMN IF NOT EXISTS email_corporativo VARCHAR(255),
            ADD COLUMN IF NOT EXISTS estado_civil VARCHAR(50),
            ADD COLUMN IF NOT EXISTS saudacao VARCHAR(50),
            ADD COLUMN IF NOT EXISTS nacionalidade VARCHAR(100),
            ADD COLUMN IF NOT EXISTS tipo_necessidade_especial VARCHAR(100),
            ADD COLUMN IF NOT EXISTS naturalidade VARCHAR(100),
            ADD COLUMN IF NOT EXISTS nome_pai VARCHAR(255),
            ADD COLUMN IF NOT EXISTS nome_mae VARCHAR(255),
            ADD COLUMN IF NOT EXISTS pcd BOOLEAN,
            ADD COLUMN IF NOT EXISTS salario_api NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS turno_trabalho VARCHAR(100),
            ADD COLUMN IF NOT EXISTS tipo_contrato VARCHAR(100),
            ADD COLUMN IF NOT EXISTS data_contrato DATE,
            ADD COLUMN IF NOT EXISTS escolaridade VARCHAR(100),
            ADD COLUMN IF NOT EXISTS curso_formacao VARCHAR(255),
            ADD COLUMN IF NOT EXISTS nivel_hierarquico VARCHAR(100),
            ADD COLUMN IF NOT EXISTS duracao_contrato VARCHAR(100),
            ADD COLUMN IF NOT EXISTS data_expiracao_contrato DATE,
            ADD COLUMN IF NOT EXISTS periodo_experiencia_dias INTEGER,
            ADD COLUMN IF NOT EXISTS forma_demissao VARCHAR(100),
            ADD COLUMN IF NOT EXISTS decisao_demissao VARCHAR(100),
            ADD COLUMN IF NOT EXISTS valor_rescisao NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS total_beneficios_api NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS etnia VARCHAR(50),
            ADD COLUMN IF NOT EXISTS nome_lider_imediato VARCHAR(255),
            ADD COLUMN IF NOT EXISTS lider_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS unidade_nome VARCHAR(255),
            ADD COLUMN IF NOT EXISTS unidade_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS cargo_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS departamento_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS cep VARCHAR(20),
            ADD COLUMN IF NOT EXISTS logradouro VARCHAR(255),
            ADD COLUMN IF NOT EXISTS numero_endereco VARCHAR(50),
            ADD COLUMN IF NOT EXISTS complemento_endereco VARCHAR(100),
            ADD COLUMN IF NOT EXISTS bairro VARCHAR(100),
            ADD COLUMN IF NOT EXISTS cidade VARCHAR(100),
            ADD COLUMN IF NOT EXISTS estado VARCHAR(50),
            ADD COLUMN IF NOT EXISTS celular VARCHAR(50),
            ADD COLUMN IF NOT EXISTS email_pessoal VARCHAR(255),
            ADD COLUMN IF NOT EXISTS telefone_emergencia VARCHAR(50),
            ADD COLUMN IF NOT EXISTS rg VARCHAR(50),
            ADD COLUMN IF NOT EXISTS data_emissao_rg DATE,
            ADD COLUMN IF NOT EXISTS orgao_emissor_rg VARCHAR(50),
            ADD COLUMN IF NOT EXISTS titulo_eleitor VARCHAR(50),
            ADD COLUMN IF NOT EXISTS zona_eleitoral VARCHAR(50),
            ADD COLUMN IF NOT EXISTS secao_eleitoral VARCHAR(50),
            ADD COLUMN IF NOT EXISTS ctps_numero VARCHAR(50),
            ADD COLUMN IF NOT EXISTS ctps_serie VARCHAR(50),
            ADD COLUMN IF NOT EXISTS pis VARCHAR(50),
            ADD COLUMN IF NOT EXISTS banco_nome VARCHAR(100),
            ADD COLUMN IF NOT EXISTS banco_agencia VARCHAR(50),
            ADD COLUMN IF NOT EXISTS banco_conta VARCHAR(50),
            ADD COLUMN IF NOT EXISTS data_ultima_atualizacao_api DATE;

        INSERT INTO "{schema}".{NOME_TABELA_RICA} (colaborador_sk, colaborador_id_solides)
        VALUES (0, -1) ON CONFLICT (colaborador_sk) DO NOTHING;

        -- UPSERT MASSIVO
        INSERT INTO "{schema}".{NOME_TABELA_RICA} (
            colaborador_sk, colaborador_id_solides, cpf, nome_completo, data_nascimento, genero,
            nacionalidade, escolaridade, nome_mae, nome_pai, estado_civil, etnia,
            data_admissao, data_demissao, ativo, departamento_nome_api, cargo_nome_api,
            matricula, salario_api, turno_trabalho, tipo_contrato, curso_formacao,
            nivel_hierarquico, nome_lider_imediato, unidade_nome, email_corporativo, telefone_pessoal,
            celular, logradouro, numero_endereco, complemento_endereco, bairro, cidade,
            estado, cep, saudacao, tipo_necessidade_especial, naturalidade, pcd,
            telefone_emergencia, email_pessoal, rg, data_emissao_rg,
            orgao_emissor_rg, titulo_eleitor, zona_eleitoral, secao_eleitoral, ctps_numero,
            ctps_serie, pis, data_contrato, duracao_contrato,
            data_expiracao_contrato, periodo_experiencia_dias, forma_demissao, decisao_demissao,
            valor_rescisao, total_beneficios_api, cargo_id_solides, departamento_id_solides,
            banco_nome, banco_agencia, banco_conta, lider_id_solides, unidade_id_solides,
            data_ultima_atualizacao_api, data_ultima_atualizacao
        )
        SELECT
            base.colaborador_sk, stg.colaborador_id_solides, stg.cpf, stg.nome_completo, stg.data_nascimento, stg.genero,
            stg.nacionalidade, stg.escolaridade, stg.nome_mae, stg.nome_pai, stg.estado_civil, stg.etnia,
            stg.data_admissao, stg.data_demissao, stg.ativo, stg.departamento_nome_api, stg.cargo_nome_api,
            stg.matricula, 
            {to_num('stg.salario_api')}, 
            stg.turno_trabalho, stg.tipo_contrato, stg.curso_formacao,
            stg.nivel_hierarquico, stg.nome_lider_imediato, stg.unidade_nome, stg.email_corporativo, NULL,
            stg.celular, stg.logradouro, stg.numero_endereco, stg.complemento_endereco, stg.bairro, stg.cidade,
            stg.estado, stg.cep, stg.saudacao, stg.tipo_necessidade_especial, stg.naturalidade, stg.pcd,
            stg.telefone_emergencia, stg.email_pessoal, stg.rg, stg.data_emissao_rg,
            stg.orgao_emissor_rg, stg.titulo_eleitor, stg.zona_eleitoral, stg.secao_eleitoral, stg.ctps_numero,
            stg.ctps_serie, stg.pis, stg.data_contrato, stg.duracao_contrato,
            stg.data_expiracao_contrato, stg.periodo_experiencia_dias, stg.forma_demissao, stg.decisao_demissao,
            {to_num('stg.valor_rescisao')}, {to_num('stg.total_beneficios_api')}, 
            stg.cargo_id_solides, stg.departamento_id_solides,
            stg.banco_nome, stg.banco_agencia, stg.banco_conta, stg.lider_id_solides, stg.unidade_id_solides,
            stg.data_ultima_atualizacao_api, current_timestamp
        FROM "{schema}".{NOME_TABELA_STAGING} AS stg
        JOIN "{schema}".{NOME_TABELA_BASE} AS base ON stg.cpf = base.cpf
        WHERE stg.colaborador_id_solides IS NOT NULL
        ON CONFLICT (colaborador_id_solides) DO UPDATE SET
            cpf = EXCLUDED.cpf,
            nome_completo = EXCLUDED.nome_completo,
            matricula = EXCLUDED.matricula,
            rg = EXCLUDED.rg,
            pis = EXCLUDED.pis,
            data_nascimento = EXCLUDED.data_nascimento,
            genero = EXCLUDED.genero,
            estado_civil = EXCLUDED.estado_civil,
            nacionalidade = EXCLUDED.nacionalidade,
            naturalidade = EXCLUDED.naturalidade,
            etnia = EXCLUDED.etnia,
            nome_pai = EXCLUDED.nome_pai,
            nome_mae = EXCLUDED.nome_mae,
            email_corporativo = EXCLUDED.email_corporativo,
            email_pessoal = EXCLUDED.email_pessoal,
            celular = EXCLUDED.celular,
            telefone_emergencia = EXCLUDED.telefone_emergencia,
            cep = EXCLUDED.cep,
            logradouro = EXCLUDED.logradouro,
            numero_endereco = EXCLUDED.numero_endereco,
            complemento_endereco = EXCLUDED.complemento_endereco,
            bairro = EXCLUDED.bairro,
            cidade = EXCLUDED.cidade,
            estado = EXCLUDED.estado,
            cargo_nome_api = EXCLUDED.cargo_nome_api,
            nivel_hierarquico = EXCLUDED.nivel_hierarquico,
            departamento_nome_api = EXCLUDED.departamento_nome_api,
            salario_api = EXCLUDED.salario_api,
            data_admissao = EXCLUDED.data_admissao,
            ativo = EXCLUDED.ativo,
            nome_lider_imediato = EXCLUDED.nome_lider_imediato,
            unidade_nome = EXCLUDED.unidade_nome,
            banco_nome = EXCLUDED.banco_nome,
            banco_agencia = EXCLUDED.banco_agencia,
            banco_conta = EXCLUDED.banco_conta,
            data_ultima_atualizacao = current_timestamp;

        -- FATO BENEFICIOS
        CREATE TABLE IF NOT EXISTS "{schema}".{NOME_FATO_BEN} (
            beneficio_id SERIAL PRIMARY KEY, colaborador_sk INTEGER,
            tipo_beneficio VARCHAR(100), nome_beneficio VARCHAR(255),
            valor_beneficio NUMERIC(12,2), valor_desconto NUMERIC(12,2),
            periodicidade VARCHAR(50), opcao_desconto VARCHAR(50), aplicado_como VARCHAR(50),
            data_atualizacao TIMESTAMP DEFAULT current_timestamp,
            FOREIGN KEY (colaborador_sk) REFERENCES "{schema}".{NOME_TABELA_BASE}(colaborador_sk)
        );
        TRUNCATE TABLE "{schema}".{NOME_FATO_BEN};

        INSERT INTO "{schema}".{NOME_FATO_BEN} (
            colaborador_sk, tipo_beneficio, nome_beneficio, valor_beneficio, valor_desconto, periodicidade, opcao_desconto, aplicado_como
        )
        SELECT 
            base.colaborador_sk, stg.tipo_beneficio, stg.nome_beneficio,
            {to_num('stg.valor_beneficio')}, {to_num('stg.valor_desconto')}, 
            stg.periodicidade, stg.opcao_desconto, stg.aplicado_como
        FROM "{schema}".{NOME_STAGING_BEN} stg
        JOIN "{schema}".{NOME_TABELA_STAGING} stg_colab ON stg.colaborador_id_solides = stg_colab.colaborador_id_solides
        JOIN "{schema}".{NOME_TABELA_BASE} base ON stg_colab.cpf = base.cpf;
        """

        with engine.begin() as conn:
            conn.execute(text(sql))
        print("Carga API concluída com sucesso.")

    except Exception as e:
        print(f"Erro Carga API: {e}")


# --------------------------------------------------------------------------------
# PÓS PROCESSAMENTO
# --------------------------------------------------------------------------------
def processar_status_transferidos(engine, schema):
    print("Executando pós-processamento de transferidos...")
    sql = text(f"""
        UPDATE "{schema}".dim_colaboradores_base
        SET situacao_csv = 'Transferido'
        WHERE cpf IN (
            SELECT base.cpf FROM "{schema}".dim_colaboradores_base base
            LEFT JOIN "{schema}".staging_colaboradores api ON base.cpf = api.cpf
            LEFT JOIN "{schema}"."stg_folha_consol" csv ON base.cpf = csv.cpf
            WHERE api.cpf IS NULL AND csv.cpf IS NULL 
            AND base.data_demissao_csv IS NULL
            AND base.situacao_csv NOT IN ('Transferido', 'Desligado')
        );
        DO $$
        BEGIN
            IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = 'dim_colaboradores') THEN
                UPDATE "{schema}".dim_colaboradores
                SET ativo = False, data_ultima_atualizacao = current_timestamp
                FROM "{schema}".dim_colaboradores_base base
                WHERE "{schema}".dim_colaboradores.colaborador_sk = base.colaborador_sk
                AND base.situacao_csv = 'Transferido' AND "{schema}".dim_colaboradores.ativo = True;
            END IF;
        END $$;
    """)
    try:
        with engine.begin() as conn:
            conn.execute(sql)
        print("Status 'Transferido' processado.")
    except Exception as e:
        print(f"Erro Transferidos: {e}")