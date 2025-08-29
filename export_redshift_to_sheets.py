import os
import psycopg2
import pandas as pd
import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
import time # Importar time para usar sleep

# Carrega variáveis do .env
load_dotenv()

# Credenciais Redshift
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

# Google Sheets
GOOGLE_SHEETS_KEYFILE = "mimo-prod-env-b31af693e832.json"
SPREADSHEET_NAME = "Mimo Live | Business Intelligence"
SHEET_NAME = "dados_redshift"

# Consulta SQL (MANTÉM A VERSÃO SEM FILTRO DE DATA)
QUERY = """
SELECT
    dl.live_id,
    dc.fantasy_name,
    dc.customer_id,
    dl.live_started_at AS live_started,
    dl.ends_at AS live_ended,
    TO_CHAR(dl.live_started_at, 'Day') AS dia_da_semana,
    TO_CHAR(dl.live_started_at, 'HH24:MI') AS horario,
    SUM(fla.total_views) AS total_views,
    SUM(fla.unique_viewers) AS unique_views,
    SUM(fla.total_chat_users) AS total_chat_user,
    ROUND(SUM(fla.unique_viewers)::numeric / NULLIF(SUM(fla.total_chat_users), 0), 2) AS percentual_engajamento,
    SUM(fla.messages_sended) AS mensagens_sended,
    ROUND(SUM(fla.messages_sended)::numeric / NULLIF(SUM(fla.total_chat_users), 0), 2) AS mensagens_por_usuario,
    SUM(fla.likes) AS likes,
    SUM(fla.cart_items_added) AS cart_items_added,
    fla.live_type_id AS live_type_id_col
FROM fact_live_analytics fla
JOIN dim_live dl ON fla.live_id = dl.live_id
JOIN dim_customer dc ON fla.customer_id = dc.customer_id
WHERE dl.deleted_at IS NULL
    AND dc.deleted_at IS NULL
    AND dl.live_started_at IS NOT NULL
    AND fla.live_type_id = '174672c7-e5b4-4878-9da7-e275cbd5d3c9'
    AND dc.fantasy_name NOT ILIKE '%mimo%'
    AND dc.fantasy_name NOT ILIKE '%teste%'
    AND dc.fantasy_name NOT IN ('Nome da Marca', 'Daninha Clow', 'Rivaw','MINHA MARCA','FIT 0/16 2023')


GROUP BY
    dl.live_id,
    dc.fantasy_name,
    dc.customer_id,
    dl.live_started_at,
    DL.ends_at,
    fla.live_type_id
HAVING
    SUM(fla.total_views) >= 10
    AND SUM(fla.unique_viewers) >= 7

UNION ALL


-- SEGUNDA PARTE: DADOS ANTIGOS (live_analytics)
SELECT
    dl.live_id,
    dc.fantasy_name,
    dc.customer_id,
    dl.live_started_at AS live_started,
    dL.ends_at AS live_ended,
    TO_CHAR(dl.live_started_at, 'Day') AS dia_da_semana,
    TO_CHAR(dl.live_started_at, 'HH24:MI') AS horario,
    SUM(la.total_views) AS total_views,
    SUM(la.unique_views) AS unique_views,
    SUM(la.total_chat_users) AS total_chat_user,
    ROUND(SUM(la.unique_views)::numeric / NULLIF(SUM(la.total_chat_users), 0), 2) AS percentual_engajamento,
    SUM(la.total_msgs) AS mensagens_sended,
    ROUND(SUM(la.total_msgs)::numeric / NULLIF(SUM(la.total_chat_users), 0), 2) AS mensagens_por_usuario,
    SUM(la.likes) AS likes,
    SUM(la.total_cart_items_added) AS cart_items_added,
    NULL AS live_type_id_col
FROM live_analytics la
JOIN dim_live dl ON la.live_id = dl.live_id
JOIN dim_customer dc ON dl.customer_id = dc.customer_id
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_live_analytics fla_check
    WHERE fla_check.live_id = la.live_id
)
    AND dl.deleted_at IS NULL
    AND dc.deleted_at IS NULL
    AND dl.live_started_at IS NOT NULL
    AND dc.fantasy_name NOT ILIKE '%mimo%'
    AND dc.fantasy_name NOT ILIKE '%teste%'
    AND dc.fantasy_name NOT IN ('Nome da Marca', 'Daninha Clow', 'Rivaw','MINHA MARCA','FIT 0/16 2023')

GROUP BY
    dl.live_id,
    dc.fantasy_name,
    dc.customer_id,
    dl.live_started_at,
    dl.ends_at
HAVING
    SUM(la.total_views) >= 10
    AND SUM(la.unique_views) >= 7

ORDER BY live_started DESC;
"""

def main():
    print("🔌 Conectando ao Redshift...")
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

    print("📥 Executando a query...")
    df = pd.read_sql(QUERY, conn)
    conn.close()

    print("\n📋 Tipos de dados originais:")
    print(df.dtypes)

    # Verifica se o DataFrame está vazio
    if df.empty:
        print("⚠️ O DataFrame está vazio. Nenhuma linha foi retornada da query do Redshift.")
        print("Verifique sua query SQL ou os filtros de data.")
        return # Encerra o script se não houver dados

    print("\n🧼 Eliminando NaNs e convertendo para texto...")
    df = df.fillna("")  # Remove NaN
    df = df.astype(str)

    print("\n📋 Tipos após conversão:")
    print(df.dtypes)

    print("\n🧾 Exemplo de linhas:")
    print(df.head())

    print("\n--- INICIANDO PROCESSAMENTO DO GOOGLE SHEETS ---")
    time.sleep(1) # Pequena pausa para garantir que o print seja exibido

    # Envia todos os dados do DataFrame (cabeçalho + todas as linhas)
    data = [df.columns.tolist()] + df.values.tolist()

    print("🔢 Total de linhas a enviar:", len(data))
    print("📝 Cabeçalho:", data[0])

    print("\n🔐 Autenticando no Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_KEYFILE, scope)
    client = gspread.authorize(creds)

    print("📑 Buscando abas da planilha...")
    spreadsheet = client.open(SPREADSHEET_NAME)
    worksheets = spreadsheet.worksheets()
    titles = [ws.title for ws in worksheets]
    print("🔍 Abas disponíveis:", titles)

    if SHEET_NAME not in titles:
        print(f"❌ Aba '{SHEET_NAME}' não encontrada! Corrija o nome e tente novamente.")
        return

    google_sheet = spreadsheet.worksheet(SHEET_NAME)

    print("🧪 Teste: escrevendo em A1...")
    try:
        google_sheet.update(range_name="A1", values=[["✅ Teste de escrita OK"]])
        print("✅ Teste A1 concluído.")
    except Exception as e:
        print("❌ Falha ao escrever no A1:", e)
        print("Isso geralmente indica problemas de permissão ou autenticação.")
        return

    print("🚀 Iniciando envio de dados com update()...")
    try:
        google_sheet.clear() 
        google_sheet.update(range_name='A1', values=data) # 'A1' é o ponto de início

        print("✅ Atualização de dados concluída usando update()!")

    except Exception as e: # Captura qualquer erro durante o processo
        print(f"❌ Erro ao enviar dados com update(): {e}")
        print("Isso pode ser um problema de formato dos dados, limite da API ou conexão.")

    print("\n--- FIM DO SCRIPT ---")

if __name__ == "__main__":
    main()