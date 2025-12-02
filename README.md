# Pipeline ETL - ARQ - People Intelligence

# Introduction
Este projeto é uma solução de Engenharia de Dados desenvolvida para centralizar e automatizar o processamento de dados de Recursos Humanos e Folha de Pagamento. O objetivo principal é eliminar a fragmentação de dados ao integrar duas fontes distintas:
1.  **API da Sólides:** Para dados cadastrais mestres de colaboradores e detalhamento de benefícios.
2.  **Arquivos PDF de Holerites (FOPAG):** Para extração de dados financeiros (proventos e descontos) através de "scraping" de documentos não estruturados.

A motivação por trás deste projeto é garantir a consistência dos dados entre o sistema de gestão de RH e os documentos oficiais de pagamento, além de estruturar esses dados em um Data Warehouse PostgreSQL para viabilizar análises avançadas de People Analytics.



# Getting Started
Para configurar e executar este código em seu ambiente local ou servidor, siga o guia abaixo.

### 1. Installation process
1.  **Clone o repositório:**
    ```bash
    git clone <url-do-repositorio>
    cd <diretorio-do-projeto>
    ```
2.  **Configure o Ambiente Virtual (Recomendado):**
    ```bash
    python -m venv .venv
    # Windows:
    .venv\Scripts\activate
    # Linux/Mac:
    source .venv/bin/activate
    ```
3.  **Banco de Dados:**
    Certifique-se de ter uma instância **PostgreSQL** ativa. Os scripts foram desenhados para criar automaticamente o schema `FOPAG` e as tabelas necessárias, caso não existam.

### 2. Software dependencies
Instale as dependências listadas nos notebooks. As principais bibliotecas utilizadas são:

* **Extração de PDF:** `pdfplumber`, `pdfminer.six` (para ler e parsear os holerites).
* **Banco de Dados:** `sqlalchemy`, `psycopg2-binary` (para conexão e ORM com Postgres).
* **Manipulação de Dados:** `pandas`, `numpy`, `decimal`.
* **Integração API:** `requests`.
* **Configuração:** `python-dotenv`, `dotenv`.

Instale todas com o comando:
```bash
pip install pandas sqlalchemy psycopg2-binary pdfplumber requests python-dotenv
```

### 3. Latest releases
O projeto opera em dois scripts principais sequenciais. A versão atual implementa:

- **Extração Incremental:** Identificação de competências já processadas.
- **Tratamento de Tipos:** Conversão robusta de strings monetárias (R$) e datas (DD/MM/YYYY) para formatos compatíveis com SQL**(PostgreSQL)**
- **Modelagem StarSchema:** Criação de tabelas fatos (```fato_folha_consolidada```, ```fato_beneficios_api```) e Dimensão(```dim_colaboradores```).

### 4. API references
Este projeto consome a **Sólides API v1**.
- **Endpoint Principal:** ```/colaboradores``` (Listagem e Detalhes).
- **Autenticação:** Token Bearer via header HTTP.
- **Estrutura JSON:** O script trata especificamente o objeto aninhado ```benefits``` para extrair, linha a linha, is benefícios ativos de cada colaborador.

### 5. Build and Test
O fluxo de execução deve seguir a seguinte ordem abaixo para garantir a integridade dos dados.

- ** Configuração (.evn)**
Crie um arquivo ```.env``` na raiz do projeto com as seguintes variáveis:
```bash
SOLIDES_API_TOKEN=seu_token_aqui
DB_USER=usuario_postgres
DB_PASS=senha_postgres
DB_HOST=localhost
DB_PORT=dw_port
DB_NAME=nome_banco
DB_SCHEMA=schema_utilizado
```


###  Passo 1: Processamento dos PDFs (Build & Extract) 
Execute o script ```Automação_FOPAG.ipynb```
1. **Aponte a variável** ```caminho_pasta``` para o diretório contendo os PDFs da FOPAG.
2. **O que ele faz:** Varre os PDFs, aplica Regex para identificar rubricas e totais, e gera dois arquivos CSV staging:
    - ```BASE_FOPAG_CONSOLIDADA_TOTAIS.csv```
    - ```BASE_FOPAG_DETALHADA_RUBRICAS.csv```
3. **Teste:** Verifique o console para mensagens como "Sucesso ! Foram processados X funcionários neste arquivo".

### Passo 2: Carga API e Banco de Dados (Load)
Execute o script ```Carga_API_Solides.ipynb```.

1. **O que ele faz:**
    - Conecta na API da Sólides para buscar/atualizar a dimensão de colaboradores.

    - Lê os CSVs gerados no Passo 1.

    - Executa o UPSERT (Inserir ou Atualizar) no PostgreSQL.

**Teste:** Ao final, o script exibirá *"Pipeline ETL Concluído com Sucesso!"*. Você pode validar os dados consultando as tabelas no schema ```FOPAG``` do seu banco de dados.
