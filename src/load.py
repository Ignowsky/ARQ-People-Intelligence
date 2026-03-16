import pandas as pd
from sqlalchemy import text
from .constants import SCHEMA_TOTAIS, SCHEMA_RUBRICAS

# --- NOVO: Import do Logger ---
from .logger import setup_logger

logger = setup_logger(__name__)


def garantir_schema_banco(engine, schema_name):
    """
    Garante que o schema e a extensão unaccent existam no banco.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
            #conn.execute(text(f'CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA "{schema_name}"'))
        # --- LOG ---
        logger.info(f"Schema '{schema_name}' verificado/garantido com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao criar schema/extensão: {e}", exc_info=True)
        raise e


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

    try:
        with engine.begin() as conn:
            conn.execute(sql)
        # --- LOG ---
        logger.info("Dimensão Calendário verificada/atualizada.")
    except Exception as e:
        logger.error(f"Erro ao carregar Dimensão Calendário: {e}", exc_info=True)


# --------------------------------------------------------------------------------
# CARGA FATOS DE FOLHA (PDFs)
# --------------------------------------------------------------------------------
def carregar_fatos_folha(df_consol, df_detalhe, engine, schema):
    """
    Carrega as tabelas fato_folha_consolidada e fato_folha_detalhada.
    """
    # --- LOG ---
    logger.info("Iniciando carga de Fatos de Folha (PDF)...")

    # --- Parte A: Popular/Atualizar Dimensão Base ---
    if not df_consol.empty:
        try:
            cols_base = ['cpf', 'nome_funcionario', 'data_admissao', 'data_demissao', 'situacao', 'departamento',
                         'cargo']
            df_base_load = df_consol[cols_base].copy().rename(columns={
                'nome_funcionario': 'nome_colaborador',
                'data_admissao': 'data_admissao_csv',
                'data_demissao': 'data_demissao_csv',
                'situacao': 'situacao_csv',
                'departamento': 'departamento_csv',
                'cargo': 'cargo_csv'
            })

            # Staging: O Pandas agora manda None (NULL) real, então o SQL não precisa de CAST
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
                data_admissao_csv,  -- Inserção direta (Python já tratou)
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
            # --- LOG ---
            logger.info("Dimensão Colaboradores Base atualizada via CSV.")

        except Exception as e:
            logger.error(f"Erro na carga da Dimensão Base: {e}", exc_info=True)

    # --- Parte B: Fato Consolidada ---
    if not df_consol.empty:
        try:
            comps_consol = tuple(df_consol['competencia'].dropna().unique())
            if comps_consol:
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
                # --- LOG ---
                logger.info("Fato Consolidada carregada com sucesso.")
        except Exception as e:
            logger.error(f"Erro na carga da Fato Consolidada: {e}", exc_info=True)

    # --- Parte C: Fato Detalhada ---
    if not df_detalhe.empty:
        try:
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
                # --- LOG ---
                logger.info("Fato Detalhada carregada com sucesso.")
        except Exception as e:
            logger.error(f"Erro na carga da Fato Detalhada: {e}", exc_info=True)


# -----------------------------------------------------------------------------
# FUNÇÃO UNIFICADA E CORRIGIDA (COLABORADORES + BENEFÍCIOS + DEPENDENTES)
# -----------------------------------------------------------------------------
def carregar_dados_api(df_staging, df_beneficios, df_dependentes, df_profiler, engine, schema):
    """
    Carga UNIFICADA de Colaboradores, Benefícios e Dependentes.
    CORREÇÃO: Não deleta a staging_colaboradores ao final, pois ela é usada no pós-processamento.
    """
    if df_staging.empty:
        logger.warning("DataFrame de colaboradores vazio. Nada a carregar.")
        return

    # --- 1. PREPARAÇÃO DOS DATAFRAMES ---
    df_staging['cpf'] = df_staging['cpf'].astype(str).replace(['nan', 'None'], None)

    def to_num(col):
        return f"CAST(NULLIF(REGEXP_REPLACE(CAST({col} AS TEXT), '[^0-9.-]', '', 'g'), '') AS NUMERIC)"

    NOME_TABELA_RICA = "dim_colaboradores"
    NOME_TABELA_BASE = "dim_colaboradores_base"
    NOME_TABELA_STAGING = "staging_colaboradores"
    NOME_STAGING_BEN = "staging_beneficios_api"
    NOME_DIM_BEN = "dim_beneficios"
    NOME_STAGING_DEP = "staging_dependentes_api"
    NOME_DIM_DEP = "dim_dependentes"

    # [NOVO] - Nomes para o Profiler
    NOME_STAGING_PROF = "staging_profiler_api"
    NOME_DIM_PROF = "dim_profiler"


    try:
        # --- 2. CARGA DAS TABELAS STAGING (Pandas -> Banco) ---
        logger.info(f"Carregando {NOME_TABELA_STAGING}...")
        df_staging.to_sql(NOME_TABELA_STAGING, engine, if_exists='replace', index=False, schema=schema)

        if not df_beneficios.empty:
            logger.info(f"Carregando {NOME_STAGING_BEN}...")
            df_beneficios.to_sql(NOME_STAGING_BEN, engine, if_exists='replace', index=False, schema=schema)

        if not df_dependentes.empty:
            logger.info(f"Carregando {NOME_STAGING_DEP}...")
            df_dependentes.to_sql(NOME_STAGING_DEP, engine, if_exists='replace', index=False, schema=schema)

        # [NOVO] - Staging do Profiler
        if not df_profiler.empty:
            logger.info(f"Carregando {NOME_DIM_PROF}...")
            df_profiler.to_sql(NOME_STAGING_PROF, engine, if_exists='replace', index=False, schema=schema)

        # --- 3. EXECUÇÃO DO SQL COMPLEXO ---
        sql = f"""
        -- ====================================================================
        -- ETAPA A: DIMENSÃO BASE
        -- ====================================================================
        CREATE TABLE IF NOT EXISTS "{schema}".{NOME_TABELA_BASE} (
            colaborador_sk SERIAL PRIMARY KEY, nome_colaborador VARCHAR(255), cpf VARCHAR(20) UNIQUE,
            data_admissao_csv DATE, data_demissao_csv DATE, situacao_csv VARCHAR(100),
            departamento_csv VARCHAR(255), cargo_csv VARCHAR(255)
        );
        INSERT INTO "{schema}".{NOME_TABELA_BASE} (colaborador_sk, nome_colaborador, cpf)
        VALUES (0, 'Desconhecido', 'N/A') ON CONFLICT (colaborador_sk) DO NOTHING;

        INSERT INTO "{schema}".{NOME_TABELA_BASE} (nome_colaborador, cpf)
        SELECT DISTINCT ON (stg.cpf) stg.nome_completo, stg.cpf
        FROM "{schema}".{NOME_TABELA_STAGING} AS stg
        WHERE stg.cpf IS NOT NULL AND stg.cpf != 'N/A' AND stg.cpf != 'nan'
        ORDER BY stg.cpf, stg.colaborador_id_solides DESC 
        ON CONFLICT (cpf) DO UPDATE SET nome_colaborador = EXCLUDED.nome_colaborador;

        -- ====================================================================
        -- ETAPA B: DIMENSÃO RICA
        -- ====================================================================
        CREATE TABLE IF NOT EXISTS "{schema}".{NOME_TABELA_RICA} (
            colaborador_sk INTEGER PRIMARY KEY, colaborador_id_solides INTEGER UNIQUE NOT NULL, 
            cpf VARCHAR(11), nome_completo VARCHAR(255),
            data_ultima_atualizacao TIMESTAMP DEFAULT current_timestamp,
            FOREIGN KEY (colaborador_sk) REFERENCES "{schema}".{NOME_TABELA_BASE}(colaborador_sk)
        );

        -- Garante colunas
        ALTER TABLE "{schema}".{NOME_TABELA_RICA}
            ADD COLUMN IF NOT EXISTS matricula VARCHAR(100),
            ADD COLUMN IF NOT EXISTS email_corporativo VARCHAR(255),
            ADD COLUMN IF NOT EXISTS email_pessoal VARCHAR(255),
            ADD COLUMN IF NOT EXISTS data_nascimento DATE,
            ADD COLUMN IF NOT EXISTS genero VARCHAR(50),
            ADD COLUMN IF NOT EXISTS estado_civil VARCHAR(50),
            ADD COLUMN IF NOT EXISTS saudacao VARCHAR(50),
            ADD COLUMN IF NOT EXISTS nacionalidade VARCHAR(100),
            ADD COLUMN IF NOT EXISTS tipo_necessidade_especial VARCHAR(100),
            ADD COLUMN IF NOT EXISTS naturalidade VARCHAR(100),
            ADD COLUMN IF NOT EXISTS nome_pai VARCHAR(255),
            ADD COLUMN IF NOT EXISTS nome_mae VARCHAR(255),
            ADD COLUMN IF NOT EXISTS pcd BOOLEAN,
            ADD COLUMN IF NOT EXISTS data_admissao DATE, 
            ADD COLUMN IF NOT EXISTS data_demissao DATE, 
            ADD COLUMN IF NOT EXISTS motivo_demissao VARCHAR(255),
            ADD COLUMN IF NOT EXISTS forma_demissao VARCHAR(100),
            ADD COLUMN IF NOT EXISTS decisao_demissao VARCHAR(100),
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
            ADD COLUMN IF NOT EXISTS valor_rescisao NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS ativo BOOLEAN,
            ADD COLUMN IF NOT EXISTS etnia VARCHAR(50),
            ADD COLUMN IF NOT EXISTS data_ultima_atualizacao_api DATE,
            ADD COLUMN IF NOT EXISTS nome_lider_imediato VARCHAR(255),
            ADD COLUMN IF NOT EXISTS lider_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS unidade_nome VARCHAR(255),
            ADD COLUMN IF NOT EXISTS unidade_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS cargo_nome_api VARCHAR(255),
            ADD COLUMN IF NOT EXISTS descricao_cargo TEXT,
            ADD COLUMN IF NOT EXISTS atividades_cargo TEXT,
            ADD COLUMN IF NOT EXISTS cargo_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS departamento_nome_api VARCHAR(255),
            ADD COLUMN IF NOT EXISTS departamento_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS cep VARCHAR(20),
            ADD COLUMN IF NOT EXISTS logradouro VARCHAR(255),
            ADD COLUMN IF NOT EXISTS numero_endereco VARCHAR(50),
            ADD COLUMN IF NOT EXISTS complemento_endereco VARCHAR(255),
            ADD COLUMN IF NOT EXISTS bairro VARCHAR(100),
            ADD COLUMN IF NOT EXISTS cidade VARCHAR(100),
            ADD COLUMN IF NOT EXISTS estado VARCHAR(50),
            ADD COLUMN IF NOT EXISTS celular VARCHAR(50),
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
            ADD COLUMN IF NOT EXISTS banco_conta VARCHAR(50);

        -- UPSERT MASSIVO
        INSERT INTO "{schema}".{NOME_TABELA_RICA} (
            colaborador_sk, colaborador_id_solides, cpf, nome_completo, matricula, 
            email_corporativo, email_pessoal, data_nascimento, genero, estado_civil,
            saudacao, nacionalidade, tipo_necessidade_especial, naturalidade,
            nome_pai, nome_mae, pcd, data_admissao, data_demissao, 
            motivo_demissao, forma_demissao, decisao_demissao,
            salario_api, turno_trabalho, tipo_contrato, data_contrato, escolaridade,
            curso_formacao, nivel_hierarquico, duracao_contrato, data_expiracao_contrato,
            periodo_experiencia_dias, valor_rescisao, ativo, etnia,
            data_ultima_atualizacao_api, nome_lider_imediato, lider_id_solides,
            unidade_nome, unidade_id_solides, cargo_nome_api, 
            descricao_cargo, atividades_cargo, cargo_id_solides,
            departamento_nome_api, departamento_id_solides, cep, logradouro,
            numero_endereco, complemento_endereco, bairro, cidade, estado,
            celular, telefone_emergencia, rg, data_emissao_rg, orgao_emissor_rg,
            titulo_eleitor, zona_eleitoral, secao_eleitoral, ctps_numero, ctps_serie,
            pis, banco_nome, banco_agencia, banco_conta,
            data_ultima_atualizacao
        )
        SELECT
            base.colaborador_sk, stg.colaborador_id_solides, stg.cpf, stg.nome_completo, stg.matricula,
            stg.email_corporativo, stg.email_pessoal, stg.data_nascimento, stg.genero, stg.estado_civil,
            stg.saudacao, stg.nacionalidade, stg.tipo_necessidade_especial, stg.naturalidade,
            stg.nome_pai, stg.nome_mae, stg.pcd, stg.data_admissao, stg.data_demissao,
            stg.motivo_demissao, stg.forma_demissao, stg.decisao_demissao,
            {to_num('stg.salario_api')}, stg.turno_trabalho, stg.tipo_contrato, stg.data_contrato, stg.escolaridade,
            stg.curso_formacao, stg.nivel_hierarquico, stg.duracao_contrato, stg.data_expiracao_contrato,
            stg.periodo_experiencia_dias, {to_num('stg.valor_rescisao')}, stg.ativo, stg.etnia,
            stg.data_ultima_atualizacao_api, stg.nome_lider_imediato, stg.lider_id_solides,
            stg.unidade_nome, stg.unidade_id_solides, stg.cargo_nome_api,
            stg.descricao_cargo, stg.atividades_cargo, stg.cargo_id_solides,
            stg.departamento_nome_api, stg.departamento_id_solides, stg.cep, stg.logradouro,
            stg.numero_endereco, stg.complemento_endereco, stg.bairro, stg.cidade, stg.estado,
            stg.celular, stg.telefone_emergencia, stg.rg, stg.data_emissao_rg, stg.orgao_emissor_rg,
            stg.titulo_eleitor, stg.zona_eleitoral, stg.secao_eleitoral, stg.ctps_numero, stg.ctps_serie,
            stg.pis, stg.banco_nome, stg.banco_agencia, stg.banco_conta,
            current_timestamp
        FROM "{schema}".{NOME_TABELA_STAGING} AS stg
        JOIN "{schema}".{NOME_TABELA_BASE} AS base ON stg.cpf = base.cpf
        WHERE stg.colaborador_id_solides IS NOT NULL
        ON CONFLICT (colaborador_id_solides) DO UPDATE SET
            nome_completo = EXCLUDED.nome_completo,
            cpf = EXCLUDED.cpf,
            matricula = EXCLUDED.matricula,
            email_corporativo = EXCLUDED.email_corporativo,
            email_pessoal = EXCLUDED.email_pessoal,
            celular = EXCLUDED.celular,
            telefone_emergencia = EXCLUDED.telefone_emergencia,
            data_nascimento = EXCLUDED.data_nascimento,
            genero = EXCLUDED.genero,
            estado_civil = EXCLUDED.estado_civil,
            nacionalidade = EXCLUDED.nacionalidade,
            naturalidade = EXCLUDED.naturalidade,
            nome_pai = EXCLUDED.nome_pai,
            nome_mae = EXCLUDED.nome_mae,
            pcd = EXCLUDED.pcd,
            etnia = EXCLUDED.etnia,
            ativo = EXCLUDED.ativo,
            data_admissao = EXCLUDED.data_admissao,
            data_demissao = EXCLUDED.data_demissao,
            motivo_demissao = EXCLUDED.motivo_demissao,
            forma_demissao = EXCLUDED.forma_demissao,
            decisao_demissao = EXCLUDED.decisao_demissao,
            salario_api = EXCLUDED.salario_api,
            tipo_contrato = EXCLUDED.tipo_contrato,
            nivel_hierarquico = EXCLUDED.nivel_hierarquico,
            cargo_nome_api = EXCLUDED.cargo_nome_api,
            descricao_cargo = EXCLUDED.descricao_cargo,
            atividades_cargo = EXCLUDED.atividades_cargo,
            departamento_nome_api = EXCLUDED.departamento_nome_api,
            unidade_nome = EXCLUDED.unidade_nome,
            nome_lider_imediato = EXCLUDED.nome_lider_imediato,
            cep = EXCLUDED.cep,
            logradouro = EXCLUDED.logradouro,
            numero_endereco = EXCLUDED.numero_endereco,
            complemento_endereco = EXCLUDED.complemento_endereco,
            bairro = EXCLUDED.bairro,
            cidade = EXCLUDED.cidade,
            estado = EXCLUDED.estado,
            rg = EXCLUDED.rg,
            pis = EXCLUDED.pis,
            ctps_numero = EXCLUDED.ctps_numero,
            ctps_serie = EXCLUDED.ctps_serie,
            titulo_eleitor = EXCLUDED.titulo_eleitor,
            valor_rescisao = EXCLUDED.valor_rescisao,
            banco_nome = EXCLUDED.banco_nome,
            banco_agencia = EXCLUDED.banco_agencia,
            banco_conta = EXCLUDED.banco_conta,
            data_ultima_atualizacao = current_timestamp;

        -- ====================================================================
        -- ETAPA C: FATO BENEFICIOS
        -- ====================================================================
        """ + (f"""
        CREATE TABLE IF NOT EXISTS "{schema}".{NOME_DIM_BEN} (
            beneficio_id SERIAL PRIMARY KEY, 
            colaborador_sk INTEGER,
            tipo_beneficio VARCHAR(100), 
            nome_beneficio VARCHAR(255),
            valor_beneficio NUMERIC(12,2), 
            valor_desconto NUMERIC(12,2),
            periodicidade VARCHAR(50), 
            opcao_desconto VARCHAR(50), 
            aplicado_como VARCHAR(50),
            data_atualizacao TIMESTAMP DEFAULT current_timestamp,
            FOREIGN KEY (colaborador_sk) REFERENCES "{schema}".{NOME_TABELA_BASE}(colaborador_sk)
        );
        TRUNCATE TABLE "{schema}".{NOME_DIM_BEN};
        
        INSERT INTO "{schema}".{NOME_DIM_BEN} (
            colaborador_sk, tipo_beneficio, nome_beneficio, 
            valor_beneficio, valor_desconto, periodicidade, 
            opcao_desconto, aplicado_como
        )
        SELECT 
            dc.colaborador_sk, -- Puxando a SK cravada direto da Dimensão Rica
            stg.tipo_beneficio, 
            stg.nome_beneficio,
            CAST(NULLIF(REGEXP_REPLACE(REPLACE(CAST(stg.valor_beneficio AS TEXT), ',', '.'), '[^0-9.]', '', 'g'), '') AS NUMERIC),
            CAST(NULLIF(REGEXP_REPLACE(REPLACE(CAST(stg.valor_desconto AS TEXT), ',', '.'), '[^0-9.]', '', 'g'), '') AS NUMERIC), 
            stg.periodicidade, 
            stg.opcao_desconto, 
            stg.aplicado_como
        FROM "{schema}".{NOME_STAGING_BEN} stg
        -- O PULO DO GATO: Join direto com a Tabela Rica pelo ID da API
        INNER JOIN "{schema}".{NOME_TABELA_RICA} dc 
            ON CAST(stg.colaborador_id_solides AS VARCHAR) = CAST(dc.colaborador_id_solides AS VARCHAR)
        WHERE dc.colaborador_sk IS NOT NULL;
        """ if not df_beneficios.empty else "") + f"""

        -- ====================================================================
        -- ETAPA D: DIMENSÃO DEPENDENTES (JOIN POR ID SOLIDES)
        -- ====================================================================
        CREATE TABLE IF NOT EXISTS "{schema}".{NOME_DIM_DEP} (
            dependente_id SERIAL PRIMARY KEY,
            colaborador_sk INTEGER NOT NULL,
            nome_dependente VARCHAR(255),
            cpf_dependente VARCHAR(20),
            rg_dependente VARCHAR(20),
            data_nascimento DATE,
            parentesco VARCHAR(50),
            data_carga TIMESTAMP DEFAULT current_timestamp,
            CONSTRAINT fk_colaborador_dep FOREIGN KEY (colaborador_sk) 
                REFERENCES "{schema}".{NOME_TABELA_BASE} (colaborador_sk)
        );
        CREATE INDEX IF NOT EXISTS idx_dep_colab_sk ON "{schema}".{NOME_DIM_DEP} (colaborador_sk);

        """ + (f"""
        -- 1. DELETE ESCOPADO
        -- Remove dependentes dos colaboradores que vieram nessa carga (usando ID Solides)
        DELETE FROM "{schema}".{NOME_DIM_DEP}
        WHERE colaborador_sk IN (
            SELECT dc.colaborador_sk
            FROM "{schema}".{NOME_STAGING_DEP} stg
            INNER JOIN "{schema}".{NOME_TABELA_RICA} dc 
                ON stg.colaborador_id_solides = dc.colaborador_id_solides
        );

        -- 2. INSERT
        INSERT INTO "{schema}".{NOME_DIM_DEP} (
            colaborador_sk, nome_dependente, cpf_dependente, rg_dependente, data_nascimento, parentesco
        )
        SELECT 
            dc.colaborador_sk, -- Pegamos o SK da dimensão rica
            stg.nome_dependente,
            stg.cpf_dependente,
            stg.rg_dependente,
            CAST(stg.data_nascimento AS DATE),
            stg.parentesco
        FROM "{schema}".{NOME_STAGING_DEP} stg
        INNER JOIN "{schema}".{NOME_TABELA_RICA} dc 
             ON stg.colaborador_id_solides = dc.colaborador_id_solides; -- <--- JOIN CORRETO AQUI
        """ if not df_dependentes.empty else "") + f"""
        
        -- ====================================================================
        -- ETAPA E: DIMENSÃO PROFILER (JOIN DIRETO POR ID)
        -- ====================================================================
        CREATE TABLE IF NOT EXISTS "{schema}".{NOME_DIM_PROF} (
            profiler_sk SERIAL PRIMARY KEY,
            colaborador_sk INTEGER NOT NULL,
            perfil_comportamental VARCHAR(50),
            data_teste DATE,
            data_carga TIMESTAMP DEFAULT current_timestamp,
            CONSTRAINT fk_colaborador_prof FOREIGN KEY (colaborador_sk) 
                REFERENCES "{schema}".{NOME_TABELA_BASE} (colaborador_sk)
        );
        CREATE INDEX IF NOT EXISTS idx_prof_colab_sk ON "{schema}".{NOME_DIM_PROF} (colaborador_sk);

        """ + (f"""
        -- 1. DELETE ESCOPADO
        -- Remove perfis antigos baseando-se no ID da Sólides que está vindo na carga
        DELETE FROM "{schema}".{NOME_DIM_PROF}
        WHERE colaborador_sk IN (
            SELECT dc.colaborador_sk
            FROM "{schema}".{NOME_STAGING_PROF} stg
            INNER JOIN "{schema}".{NOME_TABELA_RICA} dc 
                ON stg.colaborador_id_solides = dc.colaborador_id_solides
        );

        -- 2. INSERT
        -- Join DIRETO: Staging (ID) -> Dimensão Rica (ID) -> Pega o SK
        INSERT INTO "{schema}".{NOME_DIM_PROF} (
            colaborador_sk, perfil_comportamental, data_teste
        )
        SELECT 
            dc.colaborador_sk,             -- SK que precisamos
            stg.perfil_comportamental,
            CAST(stg.data_teste AS DATE)
        FROM "{schema}".{NOME_STAGING_PROF} stg
        INNER JOIN "{schema}".{NOME_TABELA_RICA} dc 
             ON stg.colaborador_id_solides = dc.colaborador_id_solides -- <--- AQUI ESTÁ O VÍNCULO PELO ID
        WHERE dc.colaborador_sk IS NOT NULL;
        """ if not df_profiler.empty else "") + f"""

        -- LIMPEZA FINAL
        DROP TABLE IF EXISTS "{schema}".{NOME_STAGING_BEN};
        DROP TABLE IF EXISTS "{schema}".{NOME_STAGING_DEP};
        DROP TABLE IF EXISTS "{schema}".{NOME_STAGING_PROF};
        """

        with engine.begin() as conn:
            conn.execute(text(sql))

        logger.info("Carga UNIFICADA da API concluída com sucesso!")

    except Exception as e:
        logger.error(f"Erro na Carga API: {e}", exc_info=True)
        raise e
# --------------------------------------------------------------------------------
# PÓS PROCESSAMENTO
# --------------------------------------------------------------------------------
def processar_status_transferidos(engine, schema):
    # --- LOG ---
    logger.info("Iniciando pós-processamento de transferidos...")

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
        # --- LOG ---
        logger.info("Status 'Transferido' processado com sucesso.")
    except Exception as e:
        # --- LOG ---
        logger.error(f"Erro no pós-processamento de transferidos: {e}", exc_info=True)


def limpar_tabelas_staging(engine, schema):
    """
    Remove todas as tabelas temporárias (staging/stg) criadas durante o processo.
    Deve ser chamada APENAS no final do pipeline.
    """
    logger.info("--- Iniciando Faxina: Removendo tabelas de Staging ---")

    # Lista de todas as tabelas temporárias que queremos apagar
    tabelas_para_remover = [
        "staging_colaboradores",
        "staging_beneficios_api",
        "staging_dependentes_api",
        "stg_folha_consol",
        "stg_folha_detalhe",
        "stg_base_csv_temp",
        "stg_dependentes_temp",
       # "dim_colaboradores_base",
        "staging_profiler_api"
    ]

    try:
        with engine.begin() as conn:
            for tabela in tabelas_para_remover:
                # O comando DROP TABLE IF EXISTS não dá erro se a tabela já sumiu
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tabela}" CASCADE'))

        logger.info("Faxina concluída! Banco de dados limpo.")

    except Exception as e:
        # Se der erro na limpeza, não é crítico para o negócio, apenas logamos warning
        logger.warning(f"Erro ao limpar tabelas de staging (não afeta os dados): {e}")