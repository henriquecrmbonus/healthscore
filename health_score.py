import csv
import mysql.connector
from mysql.connector import Error 

DB_CONFIG = {
    'host': '45.79.204.253',
    'database': 'giver', 
    'user': 'henrique.cardoso', 
    'password': 'fycPDYzvREZt', 
    'port': 3306
}

# --- PARÂMETROS PARA AS CONSULTAS (MODIFICADO PARA INPUT) ---
BRAND_ID = input("Digite o BRAND_ID: ")
DATE_BEGIN = input("Digite a data de início (formato YYYY-MM-DD): ")
DATE_UNTIL = input("Digite a data de término (formato YYYY-MM-DD): ")

def execute_query(query, params):
    """
    Executa uma consulta SQL no banco de dados MySQL e retorna cabeçalhos e dados.
    Utiliza parametrização para segurança.
    """
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            cursor = conn.cursor()

            sql_params = {}
            sql_query_adapted = query
            
            if '{{brand_id}}' in sql_query_adapted:
                sql_query_adapted = sql_query_adapted.replace('{{brand_id}}', '%s')
                sql_params['brand_id'] = params.get('brand_id')

            if '{{date_begin}}' in sql_query_adapted and '{{date_until}}' in sql_query_adapted:
                sql_query_adapted = sql_query_adapted.replace("'{{date_begin}}'", '%s')
                sql_query_adapted = sql_query_adapted.replace("'{{date_until}}'", '%s')
                sql_params['date_begin'] = params.get('date_begin')
                sql_params['date_until'] = params.get('date_until')

            ordered_params = []
            if 'brand_id' in sql_params:
                ordered_params.append(sql_params['brand_id'])
            if 'date_begin' in sql_params:
                ordered_params.append(sql_params['date_begin'])
            if 'date_until' in sql_params:
                ordered_params.append(sql_params['date_until'])

            cursor.execute(sql_query_adapted, tuple(ordered_params))

            headers = [desc[0] for desc in cursor.description]
            data = cursor.fetchall() 

            return headers, data

    except Error as e:
        print(f"Erro ao conectar ou executar consulta no MySQL: {e}")
        return [], []
    except Exception as e: 
        print(f"Ocorreu um erro inesperado: {e}")
        return [], []
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def export_queries_to_single_csv(file_name, queries_data):
    """
    Exporta dados de múltiplas consultas lado a lado como colunas em um único arquivo CSV.
    """
    with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, delimiter=';')

        # Escrever cabeçalhos (nomes das colunas)
        headers = [q['name'] for q in queries_data]
        writer.writerow(headers)
        
        # Escrever dados linha por linha, lado a lado
        for row_idx in range(1):  # Apenas uma linha de dados por coluna
            row_data = []
            for query_result in queries_data:
                if query_result['data']:
                    # Pega o primeiro valor da tupla
                    value = query_result['data'][0][0] if query_result['data'][0] else ''
                    row_data.append(value)
                else:
                    row_data.append('')
            writer.writerow(row_data)

# === QUERIES PARA CAMPANHAS CRIADAS ===
QUERY_CAMPANHAS_EMAIL = """
SELECT COUNT(*) 
FROM cli_campaign
WHERE brand_id = {{brand_id}} AND type_delivery = 1 AND created_at BETWEEN '{{date_begin}}' AND '{{date_until}}';
"""

QUERY_CAMPANHAS_SMS = """
SELECT COUNT(*) 
FROM cli_campaign
WHERE brand_id = {{brand_id}} AND type_delivery = 2 AND created_at BETWEEN '{{date_begin}}' AND '{{date_until}}';
"""

QUERY_CAMPANHAS_AGENDA = """
SELECT COUNT(*) 
FROM cli_campaign
WHERE brand_id = {{brand_id}} AND type_delivery = 5 AND created_at BETWEEN '{{date_begin}}' AND '{{date_until}}';
"""

# === QUERIES PARA BASE IMPACTADA ===
QUERY_BASE_IMPACTADA_EMAIL = """
SELECT COUNT(DISTINCT customer_id) 
FROM cli_campaign_return
WHERE brand_id = {{brand_id}} AND delivery_type = 1 AND delivered_at BETWEEN '{{date_begin}}' AND '{{date_until}}';
"""

QUERY_BASE_IMPACTADA_SMS = """
SELECT COUNT(DISTINCT customer_id) 
FROM cli_campaign_return
WHERE brand_id = {{brand_id}} AND delivery_type = 2 AND delivered_at BETWEEN '{{date_begin}}' AND '{{date_until}}';
"""

QUERY_BASE_IMPACTADA_AGENDA = """
SELECT COUNT(DISTINCT customer_id) 
FROM cli_campaign_return
WHERE brand_id = {{brand_id}} AND delivery_type = 5 AND delivered_at BETWEEN '{{date_begin}}' AND '{{date_until}}';
"""

# === QUERIES PARA LOJAS ===
QUERY_LOJAS_ATIVAS = """
SELECT COUNT(*) 
FROM cli_store
WHERE brand_id = {{brand_id}} AND status_id = 1;
"""

QUERY_LOJAS_ONBOARDING = """
SELECT COUNT(*) 
FROM cli_store
WHERE brand_id = {{brand_id}} AND status_id = 7;
"""

# === QUERIES PARA RETORNO (RFU) ===
QUERY_RFU_GATILHOS = """
SELECT COALESCE(SUM(q2.total_consolidated - q2.debit_sum), 0)
from
  (
    select
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.total_amount,
          cli_order_convertion.resource_name,
          cli_order.store_id
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = {{brand_id}}
          and cli_order_convertion.resource_name in ('cli_email_type', 'cli_trigger')
          and cli_order_convertion.converted_at >= '{{date_begin}} 00:00:00'
          and cli_order_convertion.converted_at <= '{{date_until}} 23:59:59'
          and cli_order.total_amount > 0
          and cli_order_convertion.dropped = 0
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from cli_transaction
        where
          cli_transaction.brand_id = {{brand_id}}
          and cli_transaction.transaction_date >= '{{date_begin}} 00:00:00'
          and cli_transaction.transaction_date <= '{{date_until}} 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > 0
        group by cli_transaction.brand_id, cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by q.customer_id
  ) as q2;
"""

QUERY_RFU_CAMPANHAS = """
SELECT COALESCE(SUM(q2.total_consolidated - q2.debit_sum), 0)
from
  (
    select
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.total_amount,
          cli_order_convertion.resource_name,
          cli_order.store_id
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = {{brand_id}}
          and cli_order_convertion.resource_name in ('cli_campaign')
          and cli_order_convertion.converted_at >= '{{date_begin}} 00:00:00'
          and cli_order_convertion.converted_at <= '{{date_until}} 23:59:59'
          and cli_order.total_amount > 0
          and cli_order_convertion.dropped = 0
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from cli_transaction
        where
          cli_transaction.brand_id = {{brand_id}}
          and cli_transaction.transaction_date >= '{{date_begin}} 00:00:00'
          and cli_transaction.transaction_date <= '{{date_until}} 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > 0
        group by cli_transaction.brand_id, cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by q.customer_id
  ) as q2;
"""

QUERY_RFU_CASHBACK = """
SELECT COALESCE(SUM(q2.total_consolidated - q2.debit_sum), 0)
from
  (
    select
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.total_amount,
          cli_order_convertion.resource_name,
          cli_order.store_id
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = {{brand_id}}
          and cli_order_convertion.resource_name in ('cli_transaction')
          and cli_order_convertion.converted_at >= '{{date_begin}} 00:00:00'
          and cli_order_convertion.converted_at <= '{{date_until}} 23:59:59'
          and cli_order.total_amount > 0
          and cli_order_convertion.dropped = 0
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from cli_transaction
        where
          cli_transaction.brand_id = {{brand_id}}
          and cli_transaction.transaction_date >= '{{date_begin}} 00:00:00'
          and cli_transaction.transaction_date <= '{{date_until}} 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > 0
        group by cli_transaction.brand_id, cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by q.customer_id
  ) as q2;
"""

QUERY_RFU_TELEMARKETING = """
SELECT COALESCE(SUM(q2.total_consolidated - q2.debit_sum), 0)
from
  (
    select
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.total_amount,
          cli_order_convertion.resource_name,
          cli_order.store_id
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = {{brand_id}}
          and cli_order_convertion.resource_name in ('cli_telemarketing_registry')
          and cli_order_convertion.converted_at >= '{{date_begin}} 00:00:00'
          and cli_order_convertion.converted_at <= '{{date_until}} 23:59:59'
          and cli_order.total_amount > 0
          and cli_order_convertion.dropped = 0
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from cli_transaction
        where
          cli_transaction.brand_id = {{brand_id}}
          and cli_transaction.transaction_date >= '{{date_begin}} 00:00:00'
          and cli_transaction.transaction_date <= '{{date_until}} 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > 0
        group by cli_transaction.brand_id, cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by q.customer_id
  ) as q2;
"""

QUERY_RFU_TOTAL = """
SELECT COALESCE(SUM(q2.total_consolidated - q2.debit_sum), 0)
from
  (
    select
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.total_amount,
          cli_order_convertion.resource_name,
          cli_order.store_id
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = {{brand_id}}
          and cli_order_convertion.resource_name in ('cli_campaign', 'cli_trigger', 'cli_email_type', 'cli_telemarketing_registry', 'cli_transaction')
          and cli_order_convertion.converted_at >= '{{date_begin}} 00:00:00'
          and cli_order_convertion.converted_at <= '{{date_until}} 23:59:59'
          and cli_order.total_amount > 0
          and cli_order_convertion.dropped = 0
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from cli_transaction
        where
          cli_transaction.brand_id = {{brand_id}}
          and cli_transaction.transaction_date >= '{{date_begin}} 00:00:00'
          and cli_transaction.transaction_date <= '{{date_until}} 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > 0
        group by cli_transaction.brand_id, cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by q.customer_id
  ) as q2;
"""

# === EXECUTAR QUERIES E CONSTRUIR RESULTADOS ===
all_query_results = []

print("Executando: Campanhas Criadas (Email)...")
headers, data = execute_query(QUERY_CAMPANHAS_EMAIL, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'campanhas_criadas_email', 'headers': headers, 'data': data})

print("Executando: Campanhas Criadas (SMS)...")
headers, data = execute_query(QUERY_CAMPANHAS_SMS, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'campanhas_criadas_sms', 'headers': headers, 'data': data})

print("Executando: Campanhas Criadas (Agenda)...")
headers, data = execute_query(QUERY_CAMPANHAS_AGENDA, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'campanhas_criadas_agenda', 'headers': headers, 'data': data})

print("Executando: Base Impactada (Email)...")
headers, data = execute_query(QUERY_BASE_IMPACTADA_EMAIL, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'base_impactada_email', 'headers': headers, 'data': data})

print("Executando: Base Impactada (SMS)...")
headers, data = execute_query(QUERY_BASE_IMPACTADA_SMS, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'base_impactada_sms', 'headers': headers, 'data': data})

print("Executando: Base Impactada (Agenda)...")
headers, data = execute_query(QUERY_BASE_IMPACTADA_AGENDA, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'base_impactada_agenda', 'headers': headers, 'data': data})

print("Executando: Lojas Ativas...")
headers, data = execute_query(QUERY_LOJAS_ATIVAS, {'brand_id': BRAND_ID})
all_query_results.append({'name': 'lojas_ativas', 'headers': headers, 'data': data})

print("Executando: Lojas Onboarding...")
headers, data = execute_query(QUERY_LOJAS_ONBOARDING, {'brand_id': BRAND_ID})
all_query_results.append({'name': 'lojas_onboarding', 'headers': headers, 'data': data})

print("Executando: Retorno Gatilhos...")
headers, data = execute_query(QUERY_RFU_GATILHOS, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'retorno_gatilhos', 'headers': headers, 'data': data})

print("Executando: Retorno Campanhas...")
headers, data = execute_query(QUERY_RFU_CAMPANHAS, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'retorno_campanhas', 'headers': headers, 'data': data})

print("Executando: Retorno Cashback...")
headers, data = execute_query(QUERY_RFU_CASHBACK, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'retorno_cashback', 'headers': headers, 'data': data})

print("Executando: Retorno Telemarketing...")
headers, data = execute_query(QUERY_RFU_TELEMARKETING, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'retorno_telemarketing', 'headers': headers, 'data': data})

print("Executando: Retorno Total...")
headers, data = execute_query(QUERY_RFU_TOTAL, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({'name': 'retorno_total', 'headers': headers, 'data': data})

OUTPUT_CSV_FILE = 'relatorio_consolidado_mysql.csv'
export_queries_to_single_csv(OUTPUT_CSV_FILE, all_query_results)
print(f"\nRelatório consolidado salvo em '{OUTPUT_CSV_FILE}'")
