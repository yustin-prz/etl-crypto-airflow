"""
crypto_etl_dag.py
-----------------
DAG de Airflow que ejecuta un pipeline ETL diario:
1. Extrae datos de la API de CoinGecko (top 100 cryptos)
2. Transforma y enriquece los datos
3. Carga en PostgreSQL con timestamp de ejecución

Schedule: diario a las 8:00 AM UTC
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
import os
import logging

# ─── Configuración ────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    'owner':            'yustin-prz',
    'depends_on_past':  False,
    'start_date':       datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
}

DB_CONN = os.getenv(
    'COINGECKO_DB_CONN',
    'postgresql+psycopg2://postgres:admin123@host.docker.internal:5432/gold_analysis'
)

# ─── Funciones del pipeline ───────────────────────────────────────────────────

def extract(**context):
    """
    EXTRACT — Consume la API de CoinGecko y guarda los datos crudos en XCom.
    Extrae las top 100 criptomonedas por capitalización de mercado.
    """
    logging.info("Iniciando extracción desde CoinGecko API...")
    
    all_data = []
    base_url = "https://api.coingecko.com/api/v3/coins/markets"
    
    for page in range(1, 3):  # 2 páginas × 50 = 100 cryptos
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": page,
            "sparkline": False,
            "price_change_percentage": "24h,7d"
        }
        
        response = requests.get(base_url, params=params, timeout=20)
        
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data)
            logging.info(f"Página {page}: {len(data)} cryptos extraídas")
        elif response.status_code == 429:
            logging.warning("Rate limit alcanzado, esperando 60 segundos...")
            time.sleep(60)
            response = requests.get(base_url, params=params, timeout=20)
            if response.status_code == 200:
                all_data.extend(response.json())
        else:
            logging.error(f"Error en página {page}: {response.status_code}")
        
        time.sleep(2)
    
    logging.info(f"Extracción completada: {len(all_data)} registros totales")
    
    # Pasar datos a la siguiente tarea via XCom
    context['ti'].xcom_push(key='raw_data', value=all_data)
    return len(all_data)


def transform(**context):
    """
    TRANSFORM — Limpia, renombra y enriquece los datos crudos.
    Crea columnas derivadas para análisis en el dashboard.
    """
    logging.info("Iniciando transformación de datos...")
    
    # Recuperar datos de XCom
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df = pd.DataFrame(raw_data)
    
    # Seleccionar y renombrar columnas
    columnas = {
        'id':                                    'crypto_id',
        'symbol':                                'symbol',
        'name':                                  'name',
        'current_price':                         'price_usd',
        'market_cap':                            'market_cap_usd',
        'market_cap_rank':                       'rank',
        'total_volume':                          'volume_24h_usd',
        'price_change_percentage_24h':           'change_24h_pct',
        'price_change_percentage_7d_in_currency':'change_7d_pct',
        'high_24h':                              'high_24h_usd',
        'low_24h':                               'low_24h_usd',
        'circulating_supply':                    'circulating_supply',
        'ath':                                   'all_time_high_usd',
        'ath_change_percentage':                 'ath_change_pct',
    }
    
    df = df[list(columnas.keys())].rename(columns=columnas).copy()
    
    # Tipos de datos
    df['rank']   = pd.to_numeric(df['rank'], errors='coerce').fillna(0).astype(int)
    df['symbol'] = df['symbol'].str.upper()
    
    # Timestamp de ejecución del DAG
    df['snapshot_date'] = context['execution_date'].date()
    df['extracted_at']  = datetime.utcnow()
    
    # Feature engineering
    def cap_segment(cap):
        if pd.isna(cap):            return 'Unknown'
        if cap >= 100_000_000_000:  return 'Mega Cap (>$100B)'
        if cap >= 10_000_000_000:   return 'Large Cap ($10B-$100B)'
        if cap >= 1_000_000_000:    return 'Mid Cap ($1B-$10B)'
        return 'Small Cap (<$1B)'

    def performance_label(pct):
        if pd.isna(pct):    return 'Sin datos'
        if pct >= 5:        return 'Fuerte alza (>5%)'
        if pct >= 0:        return 'Alza leve (0-5%)'
        if pct >= -5:       return 'Baja leve (0% a -5%)'
        return 'Fuerte baja (<-5%)'

    df['cap_segment']         = df['market_cap_usd'].apply(cap_segment)
    df['performance_24h']     = df['change_24h_pct'].apply(performance_label)
    df['volume_to_cap_ratio'] = (df['volume_24h_usd'] / df['market_cap_usd']).round(4)

    logging.info(f"Transformación completada: {df.shape[0]} filas x {df.shape[1]} columnas")
    
    # Pasar datos transformados via XCom (como JSON)
    context['ti'].xcom_push(key='clean_data', value=df.to_json(date_format='iso'))
    return df.shape[0]


def load(**context):
    """
    LOAD — Carga los datos transformados en PostgreSQL.
    Usa append para acumular snapshots históricos diarios.
    """
    logging.info("Iniciando carga en PostgreSQL...")
    
    # Recuperar datos transformados
    clean_json = context['ti'].xcom_pull(key='clean_data', task_ids='transform')
    df = pd.read_json(clean_json)
    
    engine = create_engine(DB_CONN)
    
    # Cargar en PostgreSQL — append para conservar histórico
    df.to_sql(
        name='crypto_daily_snapshots',
        con=engine,
        if_exists='append',
        index=False,
    )
    
    # Verificar carga
    with engine.connect() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM crypto_daily_snapshots")
        ).fetchone()[0]
        
        today = conn.execute(text("""
            SELECT COUNT(*) FROM crypto_daily_snapshots
            WHERE extracted_at::date = CURRENT_DATE
        """)).fetchone()[0]
    
    logging.info(f"Carga completada: {today} registros de hoy / {total} registros totales")
    return today


def validate(**context):
    """
    VALIDATE — Verifica la integridad de los datos cargados.
    Lanza un error si la calidad de datos no cumple los criterios mínimos.
    """
    logging.info("Iniciando validación de datos...")
    
    engine = create_engine(DB_CONN)
    
    with engine.connect() as conn:
        # Check 1: registros del día de hoy
        count_today = conn.execute(text("""
            SELECT COUNT(*) FROM crypto_daily_snapshots
            WHERE extracted_at::date = CURRENT_DATE
        """)).fetchone()[0]
        
        # Check 2: no hay precios nulos en top 10
        null_prices = conn.execute(text("""
            SELECT COUNT(*) FROM crypto_daily_snapshots
            WHERE price_usd IS NULL
            AND extracted_at::date = CURRENT_DATE
            AND rank <= 10
        """)).fetchone()[0]
        
        # Check 3: Bitcoin está en el ranking
        btc_exists = conn.execute(text("""
            SELECT COUNT(*) FROM crypto_daily_snapshots
            WHERE symbol = 'BTC'
            AND extracted_at::date = CURRENT_DATE
        """)).fetchone()[0]
    
    # Validaciones
    assert count_today >= 90, f"Se esperaban >=90 registros hoy, se encontraron {count_today}"
    assert null_prices == 0,  f"Hay {null_prices} precios nulos en el top 10"
    assert btc_exists > 0,    "Bitcoin no fue encontrado en el snapshot de hoy"
    
    logging.info(f"Validación exitosa: {count_today} registros · 0 precios nulos · BTC presente")
    return True


# ─── Definición del DAG ───────────────────────────────────────────────────────

with DAG(
    dag_id='crypto_etl_pipeline',
    default_args=DEFAULT_ARGS,
    description='Pipeline ETL diario: CoinGecko API → Transform → PostgreSQL',
    schedule_interval='0 8 * * *',   # todos los días a las 8:00 AM UTC
    catchup=False,
    tags=['crypto', 'etl', 'coingecko', 'portfolio'],
) as dag:

    t_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    t_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    t_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    t_validate = PythonOperator(
        task_id='validate',
        python_callable=validate,
    )

    # Definir orden del pipeline
    t_extract >> t_transform >> t_load >> t_validate
