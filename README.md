# 📊 ARQ-People Intelligence: Pipeline de Engenharia de Dados de RH

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-336791?style=for-the-badge&logo=postgresql)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-Red-red?style=for-the-badge)
![Azure DevOps](https://img.shields.io/badge/Azure%20DevOps-0078D7?style=for-the-badge&logo=azure-devops)

## 📋 Visão Geral
Este projeto consiste em um pipeline de **Engenharia de Dados (ETL)** robusto desenvolvido em Python para centralizar, limpar e estruturar dados de Recursos Humanos. O sistema orquestra a ingestão de dados de duas fontes distintas:

1.  **Arquivos Não-Estruturados (PDF):** Holerites, Recibos de Férias e 13º Salário (OCR/Regex).
2.  **API Externa (Sólides):** Dados cadastrais ricos e benefícios (REST).

O objetivo final é alimentar um Data Warehouse (PostgreSQL) modelado em **Star Schema** para análises de *People Analytics* (Turnover, Headcount, Custo de Folha, etc.).

---

## 🏗️ Arquitetura do Projeto

O projeto segue uma arquitetura modular baseada em **Separation of Concerns (SoC)**, onde cada etapa do ETL possui responsabilidade única.

```text
/
├── input/                 # [Staging] Área de entrada dos PDFs brutos.
├── output/                # [Transient] Área de CSVs processados para auditoria/debug.
├── src/                   # Núcleo da Engenharia
│   ├── database.py        # Factory de conexão (Singleton pattern).
│   ├── extract.py         # Ingestão (OCR via pdfplumber + Requests API).
│   ├── transform.py       # Limpeza, Tipagem (Pandas) e Regras de Negócio.
│   ├── load.py            # Persistência (Upserts, DDL e Tratamento de Erros).
│   ├── utils.py           # Sanitização (Texto e Moeda).
│   └── constants.py       # Metadados e Dicionário de Rubricas.
├── main.py                # Orquestrador (Entry Point).
├── renomear_arquivo.py    # Utilitário de padronização de arquivos.
└── .env                   # Variáveis de ambiente (Segurança).
```
----
## ⚙️ Fluxo da Arquitetura do Projeto - Diagramado


``` mermaid
---
config:
  layout: fixed
---
graph LR
    %% --- Fontes ---
    subgraph Sources ["1. Fontes de Dados"]
        FTP["☁️ Servidor FTP<br/>(Arquivos FOPAG (PDF)"]
        API["⚡ API Sólides<br/>(Dados Cadastrais)"]
    end

    %% --- Camada de Ingestão e Processamento ---
    subgraph Engine ["2. Pipeline de Engenharia (Core)"]
        direction TB
        INGEST["extract.ingestar_ftp_paralelo<br/>(15 Workers - FTPS)"]
        INPUT("📂 Local Staging<br/>(input/)")
        PDF_PROC["extract.processar_pdfs<br/>(Multiprocessing CPU)"]
        API_PROC["extract.extrair_api_solides<br/>(Multithreading I/O)"]
        TRANS["transform.py<br/>(Limpeza e Tipagem)"]
    end

    %% --- Armazenamento ---
    subgraph Storage ["3. Data Warehouse"]
        DB[("🗄️ PostgreSQL<br/>Schema FOPAG")]
    end

    %% --- Fluxo ---
    FTP -->|TLS 1.2| INGEST
    INGEST --> INPUT
    INPUT --> PDF_PROC
    API -->|JSON| API_PROC
    
    PDF_PROC --> TRANS
    API_PROC --> TRANS
    
    TRANS -->|Load| DB
    
    %% --- Limpeza ---
    DB -.->|Callback Sucesso| CLEAN["🗑️ Limpeza Automática<br/>(Delete Local Files)"]
    CLEAN -.-> INPUT

    %% --- Estilização ---
    style DB fill:#336791,stroke:#fff,stroke-width:2px,color:#fff
    style FTP fill:#2d8c58,stroke:#fff,stroke-width:2px,color:#fff
    style INGEST stroke-dasharray: 5 5
```
----

# 🚀 Detalhamento Técnico dos Módulos

## 1. Extração (```src/extract.py```)


- **PDFs** : Utiliza a biblioteca ```pdfplumber``` para extração de texto bruto. **Aplica Expressões Regulares (Regex)** complexas para identificar padrões de layout variáveis (Holerite Mensal vs. Recibo de Férias).

- **Estratégia de Fallback**: O extrator possui múltiplas camadas de regex. Se não encontrar o padrão "Competência: MM/AAAA", busca por "Data de Pagamento" ou "Período de Gozo".
- **API**: Implementa paginação automática (```while loop```) para iterar sobre todos os endpoints da API da Solides, garantindo a extração completa da base de colaboradores.

## 2. Transformação (```src/transform.py```)

Focada em **Data Quality** e **Tipagem Forte.**

- **Sanitização**: Converte strings monetárias brasileiras ('R$ 1.000,00') para objetos ``Decimal`` ou ``float`` limpos.

**Tratamento de Datas**: Converte strings para objetos ``datetime.date``, transformando valores inválidos (``NaT``, ``nan``) explicitamente em ``None`` (NULL) para evitar erros no banco.

**Enriquecimento**: Padroniza nomes de rubricas baseados em um dicionário de-para (``constants.py``).

## 3. Carga (``src/load.py``)

Utiliza SQLAlchemy e SQL puro para máxima performance e controle.

**Idempotência**: A carga de fatos utiliza a estratégia *Delete-Insert* baseada na competência. Isso permite reprocessar um mês inteiro sem duplicar dados.

**SCD Tipo 1 (Upsert)**: A dimensão de colaboradores utiliza ``INSERT ... ON CONFLICT DO UPDATE`` para garantir que o cadastro esteja sempre atualizado, mantendo o ID imutável.

**Segurança de Tipos**: Implementa funções ``safe_cast`` no SQL (``CAST(NULLIF(..., '') AS NUMERIC``)) para blindar o banco contra strings vazias ou caracteres sujos vindos da fonte.


### 4. Utilitários (`src/utils.py`)
Módulo transversal de funções auxiliares (Helpers) reutilizáveis:
* **Limpeza de Texto (`clean_text_series`):** Higienização "pesada" de strings. Remove quebras de linha (`\n`), tabulações (`\t`), caracteres invisíveis de PDF (`\xa0`) e normaliza espaços múltiplos.
* **Normalização Monetária (`limpar_valor_moeda`):** Resolve o problema de localização (Locale PT-BR). Transforma formatos complexos como `R$ 1.500,50` ou `1.000,00` em decimais limpos (`1500.50`) prontos para cálculo matemático e inserção no banco.
---

# 🔒 Política de Segurança e Retenção de Dados

Este pipeline lida com Dados Pessoais Sensíveis (LGPD). As seguintes regras são aplicadas via código e processo:

1. **Pasta ``input/`` (PDFs)**: Destinada apenas para leitura momentânea. Após a execução do pipeline e validação, os arquivos devem ser excluídos ou movidos para um armazenamento frio seguro (Cold Storage/S3).

2. **Pasta ``output/`` (CSVs)**: Arquivos gerados apenas para debug e transporte (Staging). Devem ser **excluídos** imediatamente após a confirmação da carga no banco.

3. **Credenciais**: Nenhuma senha é hardcoded. Tudo é gerenciado via variáveis de ambiente (``.env``).

---
# ⚙️ Como Executar
### Pré-requisitos
- Python 3.9+
- PostgreSQL 12+
- Dependências listadas em requirements.txt

### Passo a Passo

1. Configure o arquivo ```.env``` na raiz:
    ```text
    DB_USER=postgres
    DB_PASS=sua_senha
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=dw_rh
    DB_SCHEMA=fopag_prod
    SOLIDES_API_TOKEN=seu_token
    ```
2. Coloque os PDFs na pasta ``input/.``.
3. (Opcional) Padronize os nomes dos arquivos:
   ```bash
   python renomear_arquivo.py
   ```
4. Execute o Pipeline principal:
   ```bash
   python main.py
   ```
5. **Verificação:** Acompanhe os logs no terminal. No final, verifique as tabelas `fato_folha_consolidada` e `dim_colaboradores`