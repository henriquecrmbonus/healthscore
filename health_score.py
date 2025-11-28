import csv
import json
import os
import mysql.connector
from mysql.connector import Error 

# --- CARREGAR CONFIGURAÇÃO DE SERVIDORES E MAPEAMENTO ---
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')
if os.path.exists(CONFIG_PATH):
  with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    _cfg = json.load(f)
    SERVERS = _cfg.get('servers', {})
    BRAND_TO_SERVER = _cfg.get('brand_to_server', {})
else:
  SERVERS = {}
  BRAND_TO_SERVER = {}

# Credenciais padrão (mesmas para todos os servidores)
DB_DEFAULT_CONFIG = {
  'user': 'henrique.cardoso',
  'password': 'fycPDYzvREZt',
  'port': 3306
}

# Host/database fallback (usado se não houver mapeamento)
DB_FALLBACK_HOST = '45.79.204.253'
DB_FALLBACK_DATABASE = 'giver'

# --- PARÂMETROS PARA AS CONSULTAS (MODIFICADO PARA INPUT) ---
# Pedir ao usuário qual servidor processar (um único ID: 000..019)
SERVER_ID_INPUT = input("Digite o SERVER_ID a processar (000..019, ex: 001): ").strip()
if ',' in SERVER_ID_INPUT or SERVER_ID_INPUT.lower() == 'all' or SERVER_ID_INPUT == '':
  print("Por favor informe um único SERVER_ID (ex: 000). Não use 'all' nem valores separados por vírgula.")
  raise SystemExit(1)

if SERVER_ID_INPUT not in SERVERS:
  print(f"SERVER_ID '{SERVER_ID_INPUT}' não encontrado em config.json. Verifique e tente novamente.")
  raise SystemExit(1)

DATE_BEGIN = input("Digite a data de início (formato YYYY-MM-DD): ")
DATE_UNTIL = input("Digite a data de término (formato YYYY-MM-DD): ")

# Derivar lista de brands deste servidor a partir do mapeamento
BRAND_IDS = [b for b, sid in BRAND_TO_SERVER.items() if sid == SERVER_ID_INPUT]
if not BRAND_IDS:
  print(f"Nenhuma brand encontrada para o servidor {SERVER_ID_INPUT}.")
  raise SystemExit(1)

def execute_query(query, params):
    """
    Executa uma consulta SQL no banco de dados MySQL e retorna cabeçalhos e dados.
    Utiliza parametrização para segurança.
    """
    conn = None
    cursor = None
    try:
        # determinar configuração do DB com base no brand
        brand_id = str(params.get('brand_id')) if params.get('brand_id') is not None else None
        # obter server id a partir do mapeamento
        server_id = BRAND_TO_SERVER.get(brand_id) if brand_id else None
        if server_id is None:
            # fallback para '000' se não mapeado
            server_id = '000'

        # determinar host
        host = SERVERS.get(server_id, DB_FALLBACK_HOST)
        # determinar nome do database: 'giver' para 000, senão 'giver{server_id}'
        database = DB_FALLBACK_DATABASE if server_id == '000' else f"giver{server_id}"

        db_config = {
            'host': host,
            'database': database
        }
        db_config.update(DB_DEFAULT_CONFIG)

        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            cursor = conn.cursor()

            ordered_params = []
            
            # Contar quantos placeholders há na query
            num_placeholders = query.count('%s')
            
            # Construir lista de parâmetros repetindo conforme necessário
            for _ in range(num_placeholders):
                if 'brand_id' in params and len(ordered_params) % 3 == 0:
                    ordered_params.append(params['brand_id'])
                elif 'date_begin' in params and len(ordered_params) % 3 == 1:
                    ordered_params.append(params['date_begin'])
                elif 'date_until' in params and len(ordered_params) % 3 == 2:
                    ordered_params.append(params['date_until'])

            cursor.execute(query, tuple(ordered_params))

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

def export_queries_to_single_csv(file_name, queries_data_list):
    """
    Exporta dados de múltiplas marcas lado a lado como colunas em um único arquivo CSV.
    queries_data_list é uma lista de listas, onde cada lista contém os resultados de uma marca.
    """
    # Este exportador espera um dict de resultados por brand:
    # results_by_brand = { '8': {metric_name: value, ...}, ... }
    results_by_brand = queries_data_list
    metric_names = [
      'ano_mes',
      'campanhas_ativas', 'campanhas_criadas_email', 'campanhas_criadas_sms', 'campanhas_criadas_agenda',
      'base_impactada_total', 'base_impactada_email', 'base_impactada_sms', 'base_impactada_agenda',
      'lojas_ativas', 'lojas_onboarding',
      'clientes_totais', 'clientes_email_valido', 'clientes_celular_valido',
      'clientes_aniversario_valido',
      'total_vendas', 'total_vendas_associadas',
      'retorno_gatilhos', 'retorno_campanhas', 'retorno_cashback', 'retorno_telemarketing', 'retorno_total'
    ]

    with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
      writer = csv.writer(csv_file, delimiter=';')
      headers = ['brand_id'] + metric_names
      writer.writerow(headers)

      for bid in results_by_brand.keys():
        b = bid.strip()
        row = [b]
        values = results_by_brand.get(b, {})
        for m in metric_names:
          if m == 'ano_mes':
            # ano_mes is computed from DATE_BEGIN in format YYYY-MM
            v = DATE_BEGIN[:7] if DATE_BEGIN else ''
          else:
            v = values.get(m, '')
          row.append(v)
        writer.writerow(row)

# === QUERIES PARA CAMPANHAS CRIADAS ===
QUERY_CAMPANHAS_EMAIL = """
SELECT COUNT(*) 
FROM cli_campaign
WHERE brand_id = %s AND type_delivery = 1 AND created_at BETWEEN %s AND %s;
"""

QUERY_CAMPANHAS_SMS = """
SELECT COUNT(*) 
FROM cli_campaign
WHERE brand_id = %s AND type_delivery = 2 AND created_at BETWEEN %s AND %s;
"""

QUERY_CAMPANHAS_AGENDA = """
SELECT COUNT(*) 
FROM cli_campaign
WHERE brand_id = %s AND type_delivery = 5 AND created_at BETWEEN %s AND %s;
"""

# === QUERIES PARA BASE IMPACTADA ===
QUERY_BASE_IMPACTADA_EMAIL = """
SELECT COUNT(DISTINCT customer_id) 
FROM cli_campaign_return
WHERE brand_id = %s AND delivery_type = 1 AND delivered_at BETWEEN %s AND %s;
"""

QUERY_BASE_IMPACTADA_SMS = """
SELECT COUNT(DISTINCT customer_id) 
FROM cli_campaign_return
WHERE brand_id = %s AND delivery_type = 2 AND delivered_at BETWEEN %s AND %s;
"""

QUERY_BASE_IMPACTADA_AGENDA = """
SELECT COUNT(DISTINCT customer_id) 
FROM cli_campaign_return
WHERE brand_id = %s AND delivery_type = 5 AND delivered_at BETWEEN %s AND %s;
"""

# === QUERIES PARA LOJAS ===
QUERY_LOJAS_ATIVAS = """
SELECT COUNT(*) 
FROM cli_store
WHERE brand_id = %s AND status_id = 1;
"""

QUERY_LOJAS_ONBOARDING = """
SELECT COUNT(*) 
FROM cli_store
WHERE brand_id = %s AND status_id = 7;
"""

# === QUERIES PARA CLIENTES ===
QUERY_CLIENTES_TOTAIS = """
SELECT COUNT(id) 
FROM cli_customer
WHERE brand_id = %s;
"""

QUERY_CLIENTES_EMAIL_VALIDO = """
SELECT COUNT(id)
FROM cli_customer
WHERE brand_id = %s AND email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$';
"""

QUERY_CLIENTES_CELULAR_VALIDO = """
SELECT COUNT(*)
FROM cli_customer
WHERE brand_id = %s AND LENGTH(mobile) = 11;
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
          cli_order_convertion.resource_name
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = %s
          and cli_order_convertion.resource_name in ('cli_email_type', 'cli_trigger')
          and cli_order_convertion.converted_at >= %s
          and cli_order_convertion.converted_at <= %s
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
          cli_transaction.brand_id = %s
          and cli_transaction.transaction_date >= %s
          and cli_transaction.transaction_date <= %s
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
          cli_order_convertion.resource_name
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = %s
          and cli_order_convertion.resource_name in ('cli_campaign')
          and cli_order_convertion.converted_at >= %s
          and cli_order_convertion.converted_at <= %s
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
          cli_transaction.brand_id = %s
          and cli_transaction.transaction_date >= %s
          and cli_transaction.transaction_date <= %s
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
          cli_order_convertion.resource_name
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = %s
          and cli_order_convertion.resource_name in ('cli_transaction')
          and cli_order_convertion.converted_at >= %s
          and cli_order_convertion.converted_at <= %s
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
          cli_transaction.brand_id = %s
          and cli_transaction.transaction_date >= %s
          and cli_transaction.transaction_date <= %s
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
          cli_order_convertion.resource_name
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = %s
          and cli_order_convertion.resource_name in ('cli_telemarketing_registry')
          and cli_order_convertion.converted_at >= %s
          and cli_order_convertion.converted_at <= %s
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
          cli_transaction.brand_id = %s
          and cli_transaction.transaction_date >= %s
          and cli_transaction.transaction_date <= %s
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
          cli_order_convertion.resource_name
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
        where
          cli_order_convertion.brand_id = %s
          and cli_order_convertion.resource_name in ('cli_campaign', 'cli_trigger', 'cli_email_type', 'cli_telemarketing_registry', 'cli_transaction')
          and cli_order_convertion.converted_at >= %s
          and cli_order_convertion.converted_at <= %s
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
          cli_transaction.brand_id = %s
          and cli_transaction.transaction_date >= %s
          and cli_transaction.transaction_date <= %s
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
# === EXECUTAR QUERIES POR SERVIDOR (AGRUPADO) ===
def _placeholders_for(n):
  return ','.join(['%s'] * n)

def _to_ints(lst):
  return [int(x) for x in lst]

def run_grouped():
  # construir mapa contendo apenas o server solicitado e suas brands
  server_map = {SERVER_ID_INPUT: [b for b in BRAND_IDS]}

  for server_id, brands in server_map.items():
    print(f"\n=== Processando servidor {server_id} com brands: {brands} ===")
    host = SERVERS.get(server_id, DB_FALLBACK_HOST)
    database = DB_FALLBACK_DATABASE if server_id == '000' else f"giver{server_id}"
    db_config = {'host': host, 'database': database}
    db_config.update(DB_DEFAULT_CONFIG)

    # converter brands para ints e placeholders
    brand_ints = _to_ints(brands)
    if not brand_ints:
      continue
    ph = _placeholders_for(len(brand_ints))

    try:
      conn = mysql.connector.connect(**db_config)
      cursor = conn.cursor()

      # preparar dicionário local para este servidor
      server_results = {b: {} for b in brands}

      # 1) Campanhas ativas
      sql = f"SELECT brand_id, COUNT(*) FROM cli_campaign WHERE brand_id IN ({ph}) AND active = 1 GROUP BY brand_id"
      cursor.execute(sql, tuple(brand_ints))
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['campanhas_ativas'] = val

      # 1b) Campanhas criadas (Email/SMS/Agenda)
      for m_name, type_delivery, qname in [
          ('campanhas_criadas_email', 1, 'campanhas_criadas_email'),
          ('campanhas_criadas_sms', 2, 'campanhas_criadas_sms'),
          ('campanhas_criadas_agenda', 5, 'campanhas_criadas_agenda')
      ]:
          sql = (
              f"SELECT brand_id, COUNT(*) FROM cli_campaign WHERE brand_id IN ({ph}) "
              "AND type_delivery = %s AND created_at BETWEEN %s AND %s GROUP BY brand_id"
          )
          params = tuple(brand_ints) + (type_delivery, DATE_BEGIN, DATE_UNTIL)
          cursor.execute(sql, params)
          for row in cursor.fetchall():
              bid, val = str(row[0]), row[1]
              server_results.setdefault(bid, {})[m_name] = val

      # 2) Base impactada (Email/SMS/Agenda)
      # 2a) Base impactada total (todas as vias)
      sql = (
          f"SELECT brand_id, COUNT(DISTINCT customer_id) FROM cli_campaign_return WHERE brand_id IN ({ph}) "
          "AND delivered_at BETWEEN %s AND %s GROUP BY brand_id"
      )
      params = tuple(brand_ints) + (DATE_BEGIN, DATE_UNTIL)
      cursor.execute(sql, params)
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['base_impactada_total'] = val

      for m_name, delivery_type in [
          ('base_impactada_email', 1),
          ('base_impactada_sms', 2),
          ('base_impactada_agenda', 5)
      ]:
          sql = (
              f"SELECT brand_id, COUNT(DISTINCT customer_id) FROM cli_campaign_return WHERE brand_id IN ({ph}) "
              "AND delivery_type = %s AND delivered_at BETWEEN %s AND %s GROUP BY brand_id"
          )
          params = tuple(brand_ints) + (delivery_type, DATE_BEGIN, DATE_UNTIL)
          cursor.execute(sql, params)
          for row in cursor.fetchall():
              bid, val = str(row[0]), row[1]
              server_results.setdefault(bid, {})[m_name] = val

      # 3) Lojas (ativas/onboarding)
      sql = f"SELECT brand_id, COUNT(*) FROM cli_store WHERE brand_id IN ({ph}) AND status_id = 1 GROUP BY brand_id"
      cursor.execute(sql, tuple(brand_ints))
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['lojas_ativas'] = val

      sql = f"SELECT brand_id, COUNT(*) FROM cli_store WHERE brand_id IN ({ph}) AND status_id = 7 GROUP BY brand_id"
      cursor.execute(sql, tuple(brand_ints))
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['lojas_onboarding'] = val

      # 4) Clientes (totais, email válido, celular válido)
      sql = f"SELECT brand_id, COUNT(id) FROM cli_customer WHERE brand_id IN ({ph}) GROUP BY brand_id"
      cursor.execute(sql, tuple(brand_ints))
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['clientes_totais'] = val

      sql = f"SELECT brand_id, COUNT(id) FROM cli_customer WHERE brand_id IN ({ph}) AND email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{{2,}}$' GROUP BY brand_id"
      cursor.execute(sql, tuple(brand_ints))
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['clientes_email_valido'] = val

      sql = f"SELECT brand_id, COUNT(*) FROM cli_customer WHERE brand_id IN ({ph}) AND LENGTH(mobile) = 11 GROUP BY brand_id"
      cursor.execute(sql, tuple(brand_ints))
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['clientes_celular_valido'] = val

      # clientes com aniversário válido (não nulo nem '1900-01-01')
      sql = f"SELECT brand_id, COUNT(id) FROM cli_customer WHERE brand_id IN ({ph}) AND birthday IS NOT NULL AND birthday != '1900-01-01' GROUP BY brand_id"
      cursor.execute(sql, tuple(brand_ints))
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['clientes_aniversario_valido'] = val

      # 5) RFU queries: gatilhos, campanhas, cashback, telemarketing, total
      # We'll adapt the original RFU queries to aggregate by brand_id using IN (...) and GROUP BY q2.brand_id
      def run_rfu(resource_names, metric_key):
          resources_list = ','.join([f"'{r}'" for r in resource_names])
          sql = f"""
SELECT q2.brand_id, COALESCE(SUM(q2.total_consolidated - q2.debit_sum), 0) as value
FROM (
  select
    q.brand_id,
    q.customer_id,
    sum(q.total_amount) as total_consolidated,
    SUM(CASE WHEN q.resource_name IN ({resources_list}) THEN IFNULL(debits.total, 0) ELSE 0 END) as debit_sum
  from (
    select
      cli_order_convertion.brand_id,
      cli_order_convertion.store_id,
      cli_order_convertion.order_id,
      cli_order_convertion.customer_id,
      cli_order.total_amount,
      cli_order_convertion.resource_name
    from
      cli_order_convertion
      inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
      and cli_order.customer_id = cli_order_convertion.customer_id
      and cli_order.id = cli_order_convertion.order_id
    where
      cli_order_convertion.brand_id IN ({ph})
      and cli_order_convertion.resource_name in ({resources_list})
      and cli_order_convertion.converted_at >= %s
      and cli_order_convertion.converted_at <= %s
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
      cli_transaction.brand_id IN ({ph})
      and cli_transaction.transaction_date >= %s
      and cli_transaction.transaction_date <= %s
      and cli_transaction.transaction_type_id in ('7')
      and cli_transaction.order_id is not null
      and cli_transaction.order_id > 0
    group by cli_transaction.brand_id, cli_transaction.order_id
  ) as debits on debits.brand_id = q.brand_id
  and debits.order_id = q.order_id
  group by q.brand_id, q.customer_id
) as q2
GROUP BY q2.brand_id;"""

          params = tuple(brand_ints) + (DATE_BEGIN, DATE_UNTIL) + tuple(brand_ints) + (DATE_BEGIN, DATE_UNTIL)
          cursor.execute(sql, params)
          for row in cursor.fetchall():
              bid, val = str(row[0]), row[1]
              server_results.setdefault(bid, {})[metric_key] = val

      # gatilhos
      run_rfu(['cli_email_type', 'cli_trigger'], 'retorno_gatilhos')
      # campanhas
      run_rfu(['cli_campaign'], 'retorno_campanhas')
      # cashback (resource cli_transaction in original)
      run_rfu(['cli_transaction'], 'retorno_cashback')
      # telemarketing
      run_rfu(['cli_telemarketing_registry'], 'retorno_telemarketing')

      # total_vendas: contagem de todas as vendas no período
      sql = (
          f"SELECT brand_id, COUNT(id) FROM cli_order "
          f"WHERE brand_id IN ({ph}) "
          "AND order_date >= %s AND order_date <= %s GROUP BY brand_id"
      )
      params = tuple(brand_ints) + (DATE_BEGIN, DATE_UNTIL)
      cursor.execute(sql, params)
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['total_vendas'] = val

      # total_vendas_associadas: vendas de clientes com name != 'CONSUMIDOR FINAL'
      sql = (
          f"SELECT co.brand_id, COUNT(co.id) FROM cli_order co "
          f"INNER JOIN cli_customer cc ON co.customer_id = cc.id "
          f"WHERE co.brand_id IN ({ph}) AND cc.name <> 'CONSUMIDOR FINAL' "
          "AND co.order_date >= %s AND co.order_date <= %s GROUP BY co.brand_id"
      )
      params = tuple(brand_ints) + (DATE_BEGIN, DATE_UNTIL)
      cursor.execute(sql, params)
      for row in cursor.fetchall():
          bid, val = str(row[0]), row[1]
          server_results.setdefault(bid, {})['total_vendas_associadas'] = val

      # total (multiple resources)
      run_rfu(['cli_campaign','cli_trigger','cli_email_type','cli_telemarketing_registry','cli_transaction'], 'retorno_total')

    except Error as e:
      print(f"Erro ao conectar ou executar consultas no servidor {server_id} ({host}): {e}")
    finally:
      try:
        cursor.close()
      except:
        pass
      try:
        conn.close()
      except:
        pass
    # exportar arquivo para este servidor — incluir período no nome do arquivo
    # normalizar datas para uso em nome de arquivo (YYYYMMDD)
    dbegin = DATE_BEGIN.replace('-', '') if DATE_BEGIN else ''
    duntil = DATE_UNTIL.replace('-', '') if DATE_UNTIL else ''
    out_file = f"relatorio_consolidado_server{server_id}_{dbegin}_to_{duntil}.csv"
    export_queries_to_single_csv(out_file, server_results)
    print(f"Relatório do servidor {server_id} salvo em '{out_file}'")

  return True


results = run_grouped()
print('\nExecução concluída.')
