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
# O sistema agora irá questionar esses valores antes de rodar as consultas
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
    Exporta dados de múltiplas consultas com colunas diferentes para um único arquivo CSV.
    """
    with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)

        for i, query_result in enumerate(queries_data):
            if i > 0:
                writer.writerow([])
                writer.writerow([])
                writer.writerow([])

            writer.writerow([f'{query_result["name"]}'])
            writer.writerow(query_result['headers'])
            writer.writerows(query_result['data'])

QUERY_CAMPANHAS_CRIADAS = """
SELECT
    COUNT(*) AS 'Campanhas criadas',
    CASE
        WHEN type_delivery = 1 THEN 'Email'
        WHEN type_delivery = 2 THEN 'SMS'
        WHEN type_delivery = 5 THEN 'Agenda'
        WHEN type_delivery = 6 THEN 'Engage SMS'
        WHEN type_delivery = 7 THEN 'Engage email'
        WHEN type_delivery = 4 THEN 'Campanha de bônus'
    END AS Tipo_Campanha
FROM
    cli_campaign
WHERE brand_id = {{brand_id}} AND created_at BETWEEN '{{date_begin}}' AND '{{date_until}}'
GROUP BY Tipo_Campanha;
"""

QUERY_BASE_IMPACTADA = """
SELECT
    COUNT(customer_id) as 'Quantidade de clientes impactados',
    CASE
        WHEN delivery_type = 1 THEN 'Email'
        WHEN delivery_type = 2 THEN 'SMS'
        WHEN delivery_type = 5 THEN 'Agenda'
        WHEN delivery_type = 6 THEN 'Engage SMS'
        WHEN delivery_type = 7 THEN 'Engage email'
        WHEN delivery_type = 4 THEN 'Campanha de bônus'
    END AS 'Tipo Campanha'
FROM cli_campaign_return ccr
WHERE brand_id = {{brand_id}}
AND delivered_at BETWEEN '{{date_begin}}' AND '{{date_until}}'
GROUP BY delivery_type;
"""

QUERY_NUMERO_DE_LOJAS = """
SELECT
    COUNT(*) AS 'Quantidade de lojas',
    CASE
        WHEN status_id = 1 THEN 'Ativa pagante'
        ELSE 'Onboarding'
    END AS 'Status'
FROM cli_store
WHERE brand_id = {{brand_id}} AND status_id IN (7,1)
GROUP BY status_id;
"""

QUERY_NUMERO_DE_LOJAS_INADIMPLENTES = """
SELECT
    friendly_name AS 'Nome da Loja',
    cnpj AS 'CNPJ'
FROM cli_store
WHERE brand_id = {{brand_id}} AND status_id = 6;
"""

QUERY_RFU_CAMPANHAS = """
select
  SUM(
    q2.total_consolidated - q2.debit_sum
  ) as 'Retorno via campanhas'
from
  (
    select
      q.return_days as return_days,
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum,
      GROUP_CONCAT(DISTINCT store_name separator '|') AS store_names,
      GROUP_CONCAT(DISTINCT order_date separator '|') AS order_date,
      count(q.order_id) as orders_count,
      q.store_id,
      q.employee_id
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.employee_id,
          cli_order.total_amount,
          DATE(
            cli_order_convertion.converted_at
          ) as converted_at,
          (
            DATEDIFF(
              MIN(
                cli_order_convertion.converted_at
              ),
              (
                SELECT
                  max(cli_order.order_date)
                FROM
                  cli_order
                WHERE
                  cli_order.brand_id = cli_order_convertion.brand_id
                  AND cli_order.customer_id = cli_order_convertion.customer_id
                  AND cli_order.order_date < min(
                    cli_order_convertion.converted_at
                  )
              )
            )
          ) as return_days,
          cli_order_convertion.resource_name,
          cli_store.name AS store_name,
          date(cli_order.order_date) as order_date
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
          inner join cli_store_cache as cli_store on cli_store.brand_id = cli_order_convertion.brand_id
          and cli_store.id = cli_order.store_id
          left join cli_trigger_return on cli_trigger_return.brand_id = cli_order_convertion.brand_id
          and cli_trigger_return.customer_id = cli_order_convertion.customer_id
          and (
            (
              cli_trigger_return.trigger_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_trigger'
            )
            OR (
              cli_trigger_return.email_type_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_email_type'
            )
          )
        where
          cli_order_convertion.brand_id = '8'
          and cli_order_convertion.store_id in (select distinct id from cli_store where brand_id = 8 and status_id in (1,2,5,7))
          and cli_order_convertion.resource_name in ('cli_campaign')
          and cli_order_convertion.converted_at >= '2025-06-01 00:00:00'
          and cli_order_convertion.converted_at <= '2025-06-30 23:59:59'
          and cli_order.total_amount > '0'
          and cli_order_convertion.dropped = '0'
          and CASE WHEN cli_order_convertion.resource_name = 'cli_telemarketing_registry' THEN EXISTS (
            select
              1
            from
              cli_telemarketing_registry
            where
              cli_telemarketing_registry.brand_id = '8'
              and cli_telemarketing_registry.store_id in (select distinct id from cli_store where brand_id = 8 and status_id in (1,2,5,7))
              and cli_telemarketing_registry.id = cli_order_convertion.resource_id
              and cli_telemarketing_registry.store_id = cli_order.store_id
            limit
              1
          ) WHEN cli_order_convertion.resource_name = 'cli_campaign' THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) AND EXISTS (
            SELECT
              1
            FROM
              cli_campaign_store
              JOIN cli_campaign ON cli_campaign.id = cli_campaign_store.campaign_id
              AND cli_campaign.brand_id = cli_campaign_store.brand_id
              AND cli_campaign.active = 1
              AND cli_campaign.type_delivery in (1, 2, 6, 7)
            WHERE
              cli_campaign_store.brand_id = cli_order_convertion.brand_id
              AND cli_campaign_store.campaign_id = cli_order_convertion.resource_id
              AND IFNULL(
                cli_campaign_store.store_id, cli_order.store_id
              ) = cli_order.store_id
              AND CASE WHEN cli_campaign.identity_id = 0 THEN 1 ELSE cli_campaign.identity_id = cli_store.identity_id END
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN (
            'cli_email_type', 'cli_trigger',
            'cli_instagram_media_tracking'
          ) THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN ('cli_transaction') THEN cli_order.store_id IN (
            SELECT
              id
            FROM
              cli_store_cache
            WHERE
              brand_id = '8'
              AND status_id in (1, 2, 5, 7)
          ) ELSE 1 END = 1
          and exists (
            select
              1
            from
              cli_order
            where
              cli_order.brand_id = '8'
              and cli_order.store_id in (select distinct id from cli_store where brand_id = 8 and status_id in (1,2,5,7))
              and cli_order.id = cli_order_convertion.order_id
            limit
              1
          )
        group by
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id
        order by
          order_date desc
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from
          cli_transaction
          left join cli_store_cache as cli_store on cli_store.brand_id = cli_transaction.brand_id
          and cli_store.id = cli_transaction.store_id
        where
          cli_transaction.brand_id = '8'
          and cli_transaction.store_id in (select distinct id from cli_store where brand_id = 8 and status_id in (1,2,5,7))
          and cli_transaction.transaction_date >= '2025-06-01 00:00:00'
          and cli_transaction.transaction_date <= '2025-06-30 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > '0'
        group by
          cli_transaction.brand_id,
          cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by
      q.customer_id
  ) as q2
  inner join cli_brand on cli_brand.id = q2.brand_id
  """

QUERY_RFU_CASHBACK = """
select
  SUM(
    q2.total_consolidated - q2.debit_sum
  ) as orders_sum
from
  (
    select
      q.return_days as return_days,
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum,
      GROUP_CONCAT(DISTINCT store_name separator '|') AS store_names,
      GROUP_CONCAT(DISTINCT order_date separator '|') AS order_date,
      count(q.order_id) as orders_count,
      q.store_id,
      q.employee_id
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.employee_id,
          cli_order.total_amount,
          DATE(
            cli_order_convertion.converted_at
          ) as converted_at,
          (
            DATEDIFF(
              MIN(
                cli_order_convertion.converted_at
              ),
              (
                SELECT
                  max(cli_order.order_date)
                FROM
                  cli_order
                WHERE
                  cli_order.brand_id = cli_order_convertion.brand_id
                  AND cli_order.customer_id = cli_order_convertion.customer_id
                  AND cli_order.order_date < min(
                    cli_order_convertion.converted_at
                  )
              )
            )
          ) as return_days,
          cli_order_convertion.resource_name,
          cli_store.name AS store_name,
          date(cli_order.order_date) as order_date
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
          inner join cli_store_cache as cli_store on cli_store.brand_id = cli_order_convertion.brand_id
          and cli_store.id = cli_order.store_id
          left join cli_trigger_return on cli_trigger_return.brand_id = cli_order_convertion.brand_id
          and cli_trigger_return.customer_id = cli_order_convertion.customer_id
          and (
            (
              cli_trigger_return.trigger_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_trigger'
            )
            OR (
              cli_trigger_return.email_type_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_email_type'
            )
          )
        where
          cli_order_convertion.brand_id = '8'
          and cli_order_convertion.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
          and cli_order_convertion.resource_name in ('cli_transaction')
          and cli_order_convertion.converted_at >= '2025-06-01 00:00:00'
          and cli_order_convertion.converted_at <= '2025-06-30 23:59:59'
          and cli_order.total_amount > '0'
          and cli_order_convertion.dropped = '0'
          and CASE WHEN cli_order_convertion.resource_name = 'cli_telemarketing_registry' THEN EXISTS (
            select
              1
            from
              cli_telemarketing_registry
            where
              cli_telemarketing_registry.brand_id = '8'
              and cli_telemarketing_registry.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
              and cli_telemarketing_registry.id = cli_order_convertion.resource_id
              and cli_telemarketing_registry.store_id = cli_order.store_id
            limit
              1
          ) WHEN cli_order_convertion.resource_name = 'cli_campaign' THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) AND EXISTS (
            SELECT
              1
            FROM
              cli_campaign_store
              JOIN cli_campaign ON cli_campaign.id = cli_campaign_store.campaign_id
              AND cli_campaign.brand_id = cli_campaign_store.brand_id
              AND cli_campaign.active = 1
              AND cli_campaign.type_delivery in (1, 2, 6, 7)
            WHERE
              cli_campaign_store.brand_id = cli_order_convertion.brand_id
              AND cli_campaign_store.campaign_id = cli_order_convertion.resource_id
              AND IFNULL(
                cli_campaign_store.store_id, cli_order.store_id
              ) = cli_order.store_id
              AND CASE WHEN cli_campaign.identity_id = 0 THEN 1 ELSE cli_campaign.identity_id = cli_store.identity_id END
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN (
            'cli_email_type', 'cli_trigger',
            'cli_instagram_media_tracking'
          ) THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN ('cli_transaction') THEN cli_order.store_id IN (
            SELECT
              id
            FROM
              cli_store_cache
            WHERE
              brand_id = '8'
              AND status_id in (1, 2, 5, 7)
          ) ELSE 1 END = 1
          and exists (
            select
              1
            from
              cli_order
            where
              cli_order.brand_id = '8'
              and cli_order.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
              and cli_order.id = cli_order_convertion.order_id
            limit
              1
          )
        group by
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id
        order by
          order_date desc
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from
          cli_transaction
          left join cli_store_cache as cli_store on cli_store.brand_id = cli_transaction.brand_id
          and cli_store.id = cli_transaction.store_id
        where
          cli_transaction.brand_id = '8'
          and cli_transaction.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
          and cli_transaction.transaction_date >= '2025-06-01 00:00:00'
          and cli_transaction.transaction_date <= '2025-06-30 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > '0'
        group by
          cli_transaction.brand_id,
          cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by
      q.customer_id
  ) as q2
  inner join cli_brand on cli_brand.id = q2.brand_id
  """

QUERY_RFU_TOTAL = """
select
  SUM(
    q2.total_consolidated - q2.debit_sum
  ) as orders_sum
from
  (
    select
      q.return_days as return_days,
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum,
      GROUP_CONCAT(DISTINCT store_name separator '|') AS store_names,
      GROUP_CONCAT(DISTINCT order_date separator '|') AS order_date,
      count(q.order_id) as orders_count,
      q.store_id,
      q.employee_id
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.employee_id,
          cli_order.total_amount,
          DATE(
            cli_order_convertion.converted_at
          ) as converted_at,
          (
            DATEDIFF(
              MIN(
                cli_order_convertion.converted_at
              ),
              (
                SELECT
                  max(cli_order.order_date)
                FROM
                  cli_order
                WHERE
                  cli_order.brand_id = cli_order_convertion.brand_id
                  AND cli_order.customer_id = cli_order_convertion.customer_id
                  AND cli_order.order_date < min(
                    cli_order_convertion.converted_at
                  )
              )
            )
          ) as return_days,
          cli_order_convertion.resource_name,
          cli_store.name AS store_name,
          date(cli_order.order_date) as order_date
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
          inner join cli_store_cache as cli_store on cli_store.brand_id = cli_order_convertion.brand_id
          and cli_store.id = cli_order.store_id
          left join cli_trigger_return on cli_trigger_return.brand_id = cli_order_convertion.brand_id
          and cli_trigger_return.customer_id = cli_order_convertion.customer_id
          and (
            (
              cli_trigger_return.trigger_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_trigger'
            )
            OR (
              cli_trigger_return.email_type_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_email_type'
            )
          )
        where
          cli_order_convertion.brand_id = '8'
          and cli_order_convertion.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
          and cli_order_convertion.resource_name in (
            'cli_campaign', 'cli_trigger', 'cli_email_type',
            'cli_telemarketing_registry', 'cli_transaction'
          )
          and cli_order_convertion.converted_at >= '2025-06-01 00:00:00'
          and cli_order_convertion.converted_at <= '2025-06-30 23:59:59'
          and cli_order.total_amount > '0'
          and cli_order_convertion.dropped = '0'
          and CASE WHEN cli_order_convertion.resource_name = 'cli_telemarketing_registry' THEN EXISTS (
            select
              1
            from
              cli_telemarketing_registry
            where
              cli_telemarketing_registry.brand_id = '8'
              and cli_telemarketing_registry.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
              and cli_telemarketing_registry.id = cli_order_convertion.resource_id
              and cli_telemarketing_registry.store_id = cli_order.store_id
            limit
              1
          ) WHEN cli_order_convertion.resource_name = 'cli_campaign' THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) AND EXISTS (
            SELECT
              1
            FROM
              cli_campaign_store
              JOIN cli_campaign ON cli_campaign.id = cli_campaign_store.campaign_id
              AND cli_campaign.brand_id = cli_campaign_store.brand_id
              AND cli_campaign.active = 1
              AND cli_campaign.type_delivery in (1, 2, 6, 7)
            WHERE
              cli_campaign_store.brand_id = cli_order_convertion.brand_id
              AND cli_campaign_store.campaign_id = cli_order_convertion.resource_id
              AND IFNULL(
                cli_campaign_store.store_id, cli_order.store_id
              ) = cli_order.store_id
              AND CASE WHEN cli_campaign.identity_id = 0 THEN 1 ELSE cli_campaign.identity_id = cli_store.identity_id END
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN (
            'cli_email_type', 'cli_trigger',
            'cli_instagram_media_tracking'
          ) THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN ('cli_transaction') THEN cli_order.store_id IN (
            SELECT
              id
            FROM
              cli_store_cache
            WHERE
              brand_id = '8'
              AND status_id in (1, 2, 5, 7)
          ) ELSE 1 END = 1
          and exists (
            select
              1
            from
              cli_order
            where
              cli_order.brand_id = '8'
              and cli_order.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
              and cli_order.id = cli_order_convertion.order_id
            limit
              1
          )
        group by
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id
        order by
          order_date desc
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from
          cli_transaction
          left join cli_store_cache as cli_store on cli_store.brand_id = cli_transaction.brand_id
          and cli_store.id = cli_transaction.store_id
        where
          cli_transaction.brand_id = '8'
          and cli_transaction.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
          and cli_transaction.transaction_date >= '2025-06-01 00:00:00'
          and cli_transaction.transaction_date <= '2025-06-30 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > '0'
        group by
          cli_transaction.brand_id,
          cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by
      q.customer_id
  ) as q2
  inner join cli_brand on cli_brand.id = q2.brand_id
  """

QUERY_RFU_TELEMARKETING = """
  select
  SUM(
    q2.total_consolidated - q2.debit_sum
  ) as orders_sum
from
  (
    select
      q.return_days as return_days,
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum,
      GROUP_CONCAT(DISTINCT store_name separator '|') AS store_names,
      GROUP_CONCAT(DISTINCT order_date separator '|') AS order_date,
      count(q.order_id) as orders_count,
      q.store_id,
      q.employee_id
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.employee_id,
          cli_order.total_amount,
          DATE(
            cli_order_convertion.converted_at
          ) as converted_at,
          (
            DATEDIFF(
              MIN(
                cli_order_convertion.converted_at
              ),
              (
                SELECT
                  max(cli_order.order_date)
                FROM
                  cli_order
                WHERE
                  cli_order.brand_id = cli_order_convertion.brand_id
                  AND cli_order.customer_id = cli_order_convertion.customer_id
                  AND cli_order.order_date < min(
                    cli_order_convertion.converted_at
                  )
              )
            )
          ) as return_days,
          cli_order_convertion.resource_name,
          cli_store.name AS store_name,
          date(cli_order.order_date) as order_date
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
          inner join cli_store_cache as cli_store on cli_store.brand_id = cli_order_convertion.brand_id
          and cli_store.id = cli_order.store_id
          left join cli_trigger_return on cli_trigger_return.brand_id = cli_order_convertion.brand_id
          and cli_trigger_return.customer_id = cli_order_convertion.customer_id
          and (
            (
              cli_trigger_return.trigger_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_trigger'
            )
            OR (
              cli_trigger_return.email_type_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_email_type'
            )
          )
        where
          cli_order_convertion.brand_id = '8'
          and cli_order_convertion.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
          and cli_order_convertion.resource_name in ('cli_telemarketing_registry')
          and cli_order_convertion.converted_at >= '2025-06-01 00:00:00'
          and cli_order_convertion.converted_at <= '2025-06-30 23:59:59'
          and cli_order.total_amount > '0'
          and cli_order_convertion.dropped = '0'
          and CASE WHEN cli_order_convertion.resource_name = 'cli_telemarketing_registry' THEN EXISTS (
            select
              1
            from
              cli_telemarketing_registry
            where
              cli_telemarketing_registry.brand_id = '8'
              and cli_telemarketing_registry.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
              and cli_telemarketing_registry.id = cli_order_convertion.resource_id
              and cli_telemarketing_registry.store_id = cli_order.store_id
            limit
              1
          ) WHEN cli_order_convertion.resource_name = 'cli_campaign' THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) AND EXISTS (
            SELECT
              1
            FROM
              cli_campaign_store
              JOIN cli_campaign ON cli_campaign.id = cli_campaign_store.campaign_id
              AND cli_campaign.brand_id = cli_campaign_store.brand_id
              AND cli_campaign.active = 1
              AND cli_campaign.type_delivery in (1, 2, 6, 7)
            WHERE
              cli_campaign_store.brand_id = cli_order_convertion.brand_id
              AND cli_campaign_store.campaign_id = cli_order_convertion.resource_id
              AND IFNULL(
                cli_campaign_store.store_id, cli_order.store_id
              ) = cli_order.store_id
              AND CASE WHEN cli_campaign.identity_id = 0 THEN 1 ELSE cli_campaign.identity_id = cli_store.identity_id END
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN (
            'cli_email_type', 'cli_trigger',
            'cli_instagram_media_tracking'
          ) THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN ('cli_transaction') THEN cli_order.store_id IN (
            SELECT
              id
            FROM
              cli_store_cache
            WHERE
              brand_id = '8'
              AND status_id in (1, 2, 5, 7)
          ) ELSE 1 END = 1
          and exists (
            select
              1
            from
              cli_order
            where
              cli_order.brand_id = '8'
              and cli_order.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
              and cli_order.id = cli_order_convertion.order_id
            limit
              1
          )
        group by
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id
        order by
          order_date desc
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from
          cli_transaction
          left join cli_store_cache as cli_store on cli_store.brand_id = cli_transaction.brand_id
          and cli_store.id = cli_transaction.store_id
        where
          cli_transaction.brand_id = '8'
          and cli_transaction.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
          and cli_transaction.transaction_date >= '2025-06-01 00:00:00'
          and cli_transaction.transaction_date <= '2025-06-30 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > '0'
        group by
          cli_transaction.brand_id,
          cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by
      q.customer_id
  ) as q2
  inner join cli_brand on cli_brand.id = q2.brand_id
  """

QUERY_RFU_GATILHOS = """
select
  SUM(
    q2.total_consolidated - q2.debit_sum
  ) as orders_sum
from
  (
    select
      q.return_days as return_days,
      q.customer_id,
      q.brand_id,
      sum(q.total_amount) as total_consolidated,
      SUM(
        CASE WHEN q.resource_name IN ('cli_transaction') THEN IFNULL(debits.total, 0) ELSE 0 END
      ) as debit_sum,
      GROUP_CONCAT(DISTINCT store_name separator '|') AS store_names,
      GROUP_CONCAT(DISTINCT order_date separator '|') AS order_date,
      count(q.order_id) as orders_count,
      q.store_id,
      q.employee_id
    from
      (
        select
          cli_order_convertion.brand_id,
          cli_order_convertion.store_id,
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id,
          cli_order.employee_id,
          cli_order.total_amount,
          DATE(
            cli_order_convertion.converted_at
          ) as converted_at,
          (
            DATEDIFF(
              MIN(
                cli_order_convertion.converted_at
              ),
              (
                SELECT
                  max(cli_order.order_date)
                FROM
                  cli_order
                WHERE
                  cli_order.brand_id = cli_order_convertion.brand_id
                  AND cli_order.customer_id = cli_order_convertion.customer_id
                  AND cli_order.order_date < min(
                    cli_order_convertion.converted_at
                  )
              )
            )
          ) as return_days,
          cli_order_convertion.resource_name,
          cli_store.name AS store_name,
          date(cli_order.order_date) as order_date
        from
          cli_order_convertion
          inner join cli_order on cli_order.brand_id = cli_order_convertion.brand_id
          and cli_order.customer_id = cli_order_convertion.customer_id
          and cli_order.id = cli_order_convertion.order_id
          inner join cli_store_cache as cli_store on cli_store.brand_id = cli_order_convertion.brand_id
          and cli_store.id = cli_order.store_id
          left join cli_trigger_return on cli_trigger_return.brand_id = cli_order_convertion.brand_id
          and cli_trigger_return.customer_id = cli_order_convertion.customer_id
          and (
            (
              cli_trigger_return.trigger_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_trigger'
            )
            OR (
              cli_trigger_return.email_type_id = cli_order_convertion.resource_id
              AND cli_order_convertion.resource_name = 'cli_email_type'
            )
          )
        where
          cli_order_convertion.brand_id = '8'
          and cli_order_convertion.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
          and cli_order_convertion.resource_name in ('cli_email_type', 'cli_trigger')
          and cli_order_convertion.converted_at >= '2025-06-01 00:00:00'
          and cli_order_convertion.converted_at <= '2025-06-30 23:59:59'
          and cli_order.total_amount > '0'
          and cli_order_convertion.dropped = '0'
          and CASE WHEN cli_order_convertion.resource_name = 'cli_telemarketing_registry' THEN EXISTS (
            select
              1
            from
              cli_telemarketing_registry
            where
              cli_telemarketing_registry.brand_id = '8'
              and cli_telemarketing_registry.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
              and cli_telemarketing_registry.id = cli_order_convertion.resource_id
              and cli_telemarketing_registry.store_id = cli_order.store_id
            limit
              1
          ) WHEN cli_order_convertion.resource_name = 'cli_campaign' THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) AND EXISTS (
            SELECT
              1
            FROM
              cli_campaign_store
              JOIN cli_campaign ON cli_campaign.id = cli_campaign_store.campaign_id
              AND cli_campaign.brand_id = cli_campaign_store.brand_id
              AND cli_campaign.active = 1
              AND cli_campaign.type_delivery in (1, 2, 6, 7)
            WHERE
              cli_campaign_store.brand_id = cli_order_convertion.brand_id
              AND cli_campaign_store.campaign_id = cli_order_convertion.resource_id
              AND IFNULL(
                cli_campaign_store.store_id, cli_order.store_id
              ) = cli_order.store_id
              AND CASE WHEN cli_campaign.identity_id = 0 THEN 1 ELSE cli_campaign.identity_id = cli_store.identity_id END
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN (
            'cli_email_type', 'cli_trigger',
            'cli_instagram_media_tracking'
          ) THEN EXISTS (
            SELECT
              1
            FROM
              cli_store_log_status
            WHERE
              cli_store_log_status.brand_id = cli_order_convertion.brand_id
              AND cli_store_log_status.store_id = cli_order.store_id
              AND cli_store_log_status.status_id in (1, 2, 5, 7)
              AND EXTRACT(
                YEAR_MONTH
                FROM
                  cli_store_log_status.created_at
              ) >= EXTRACT(
                YEAR_MONTH
                FROM
                  cli_order_convertion.converted_at
              )
            LIMIT
              1
          ) WHEN cli_order_convertion.resource_name IN ('cli_transaction') THEN cli_order.store_id IN (
            SELECT
              id
            FROM
              cli_store_cache
            WHERE
              brand_id = '8'
              AND status_id in (1, 2, 5, 7)
          ) ELSE 1 END = 1
          and exists (
            select
              1
            from
              cli_order
            where
              cli_order.brand_id = '8'
              and cli_order.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
              and cli_order.id = cli_order_convertion.order_id
            limit
              1
          )
        group by
          cli_order_convertion.order_id,
          cli_order_convertion.customer_id
        order by
          order_date desc
      ) as q
      left join (
        select
          cli_transaction.order_id,
          cli_transaction.brand_id,
          sum(cli_transaction.amount) AS total
        from
          cli_transaction
          left join cli_store_cache as cli_store on cli_store.brand_id = cli_transaction.brand_id
          and cli_store.id = cli_transaction.store_id
        where
          cli_transaction.brand_id = '8'
          and cli_transaction.store_id in (select id from cli_store where brand_id = '8' and cli_store.status_id in ('1','2','5','7'))
          and cli_transaction.transaction_date >= '2025-06-01 00:00:00'
          and cli_transaction.transaction_date <= '2025-06-30 23:59:59'
          and cli_transaction.transaction_type_id in ('7')
          and cli_transaction.order_id is not null
          and cli_transaction.order_id > '0'
        group by
          cli_transaction.brand_id,
          cli_transaction.order_id
      ) as debits on debits.brand_id = q.brand_id
      and debits.order_id = q.order_id
    group by
      q.customer_id
  ) as q2
  inner join cli_brand on cli_brand.id = q2.brand_id
  """

all_query_results = []

print("Executando: Quantidade de Campanhas Criadas...")
headers1, data1 = execute_query(QUERY_CAMPANHAS_CRIADAS, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({
    'name': 'QUANTIDADE DE CAMPANHAS CRIADAS NO PERÍODO',
    'headers': headers1,
    'data': data1
})

print("Executando: Base Impactada por Canal...")
headers2, data2 = execute_query(QUERY_BASE_IMPACTADA, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({
    'name': 'BASE IMPACTADA POR CANAL NO PERÍODO',
    'headers': headers2,
    'data': data2
})

print("Executando: Número de Lojas...")
headers3, data3 = execute_query(QUERY_NUMERO_DE_LOJAS, {'brand_id': BRAND_ID})
all_query_results.append({
    'name': 'NÚMERO DE LOJAS',
    'headers': headers3,
    'data': data3
})

print("Executando: Número de Lojas Inadimplentes...")
headers4, data4 = execute_query(QUERY_NUMERO_DE_LOJAS_INADIMPLENTES, {'brand_id': BRAND_ID})
all_query_results.append({
    'name': 'NÚMERO DE LOJAS INADIMPLENTES',
    'headers': headers4,
    'data': data4 
})

print("Executando: RFU Campanhas...")
headers5, data5 = execute_query(QUERY_RFU_CAMPANHAS, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({
    'name': 'RFU CAMPANHAS',
    'headers': headers5,
    'data': data5
})

print("Executando: RFU Cashback...")
headers6, data6 = execute_query(QUERY_RFU_CASHBACK, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL}) 
all_query_results.append({
    'name': 'RFU CASHBACK',
    'headers': headers6,
    'data': data6
})  

print("Executando: RFU Total...")
headers7, data7 = execute_query(QUERY_RFU_TOTAL, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({
    'name': 'RFU TOTAL',
    'headers': headers7,
    'data': data7
})

print("Executando: RFU Telemarketing...")
headers8, data8 = execute_query(QUERY_RFU_TELEMARKETING, {'brand  _id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({
    'name': 'RFU TELEMARKETING',
    'headers': headers8,
    'data': data8
})

print("Executando: RFU Gatilhos...")
headers9, data9 = execute_query(QUERY_RFU_GATILHOS, {'brand_id': BRAND_ID, 'date_begin': DATE_BEGIN, 'date_until': DATE_UNTIL})
all_query_results.append({
    'name': 'RFU GATILHOS',
    'headers': headers9,
    'data': data9
})

OUTPUT_CSV_FILE = 'relatorio_consolidado_mysql.csv'
export_queries_to_single_csv(OUTPUT_CSV_FILE, all_query_results)
print(f"\nRelatório consolidado salvo em '{OUTPUT_CSV_FILE}'")
