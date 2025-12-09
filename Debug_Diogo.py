import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Pega as mesmas credenciais do seu projeto
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
SCHEMA = "FOPAG"  # Confirme se é este schema que você usa no constants.py


def teste_banco():
    print("🔌 Testando conexão com o Banco...")

    # String de conexão (mesma do database.py)
    db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    try:
        # 1. Verifica se o Schema existe
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{SCHEMA}'"))
            if not result.fetchone():
                print(f"❌ ERRO: O Schema '{SCHEMA}' NÃO EXISTE no banco!")
                return
            else:
                print(f"✅ Schema '{SCHEMA}' encontrado.")

        # 2. Tenta inserir um dado dummy na tabela BASE (que não tem muitas constraints)
        print("💾 Tentando inserir registro de teste...")
        with engine.begin() as conn:  # .begin() faz o commit automático se não der erro
            # Garante a tabela
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS "{SCHEMA}".teste_debug (
                    id SERIAL PRIMARY KEY,
                    msg VARCHAR(100),
                    data TIMESTAMP DEFAULT current_timestamp
                )
            """))

            # Insere
            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".teste_debug (msg) VALUES ('Teste de Gravação Python')
            """))

        print("✅ Insert rodou sem erro no Python.")

        # 3. Lê de volta para ver se persistiu
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM \"{SCHEMA}\".teste_debug ORDER BY id DESC LIMIT 1"))
            row = result.fetchone()
            if row:
                print(f"🎉 SUCESSO! O banco gravou e devolveu: ID={row[0]}, Msg='{row[1]}'")
            else:
                print("❌ FRACASSO: O insert rodou, mas o SELECT não achou nada. Rollback silencioso?")

    except Exception as e:
        print(f"❌ ERRO DE CONEXÃO/SQL: {e}")


if __name__ == "__main__":
    teste_banco()