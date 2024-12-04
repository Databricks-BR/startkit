-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ### Version Code Control
-- MAGIC
-- MAGIC | versão | data | autor | e-mail | alterações |
-- MAGIC | --- | --- | --- | --- | --- |
-- MAGIC | 1.0 | 02-DEZ-2024 | Rodrigo Oliveira | rodrigo.oliveira@databricks.com | Primeira versão  |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Referência:
-- MAGIC * [Publicação Linkedin - 25 queries para Gestão de Custos e Auditoria no Databricks](https://www.linkedin.com/pulse/25-queries-para-gest%C3%A3o-de-custos-e-auditoria-rodrigo-oliveira-qarpf/?trackingId=%2FxJBuqhKS8mzR3tPluvJQA%3D%3D)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 01. Quanto é o consumo diário de Databricks?

-- COMMAND ----------

SELECT date(usage_date) as `Data`, 
               sum(usage_quantity) as `DBUs Consumido`,
               sum(u.usage_quantity * lp.pricing.default) as `Custo`
FROM system.billing.usage u
      LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
        u.sku_name = lp.sku_name AND
        u.usage_start_time >= lp.price_start_time AND
        (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
GROUP BY date(usage_date)
ORDER BY date(usage_date) ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 02. Quanto é o consumo de Databricks por ano/mês?

-- COMMAND ----------

SELECT
  u.workspace_id,
  CASE
      WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN 'ALL_PURPOSE'
      WHEN u.sku_name LIKE '%JOBS_COMPUTE%' THEN 'JOBS'
      WHEN u.sku_name LIKE '%DLT%' THEN 'DLT'
      WHEN u.sku_name LIKE '%ENTERPRISE_SQL_COMPUTE%' THEN 'SQL'
      WHEN u.sku_name LIKE '%ENTERPRISE_SQL_PRO_COMPUTE%' THEN 'SQL'
      WHEN u.sku_name LIKE '%SERVERLESS_SQL_COMPUTE%' THEN 'SQL_SERVERLESS'
      WHEN u.sku_name LIKE '%INFERENCE%' THEN 'MODEL_INFERENCE'
      ELSE 'OTHER'
  END AS sku,
  u.cloud,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_quantity * lp.pricing.default as actual_cost
FROM
  system.billing.usage u 
  LEFT JOIN system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 03. Quanto é o consumo de cada SKU no mês atual?
-- MAGIC

-- COMMAND ----------

SELECT
   CASE
      WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN 'ALL_PURPOSE'
      WHEN u.sku_name LIKE '%JOBS_COMPUTE%' THEN 'JOBS'
      WHEN u.sku_name LIKE '%DLT%' THEN 'DLT'
      WHEN u.sku_name LIKE '%ENTERPRISE_SQL_COMPUTE%' THEN 'SQL'
      WHEN u.sku_name LIKE '%ENTERPRISE_SQL_PRO_COMPUTE%' THEN 'SQL'
      WHEN u.sku_name LIKE '%SERVERLESS_SQL_COMPUTE%' THEN 'SQL_SERVERLESS'
      WHEN u.sku_name LIKE '%INFERENCE%' THEN 'MODEL_INFERENCE'
      ELSE 'OTHER'
   END AS sku,
   sum(u.usage_quantity) as DBU,
   sum(u.usage_quantity * lp.pricing.default) as dolares
  FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
      u.sku_name = lp.sku_name AND
      u.usage_start_time >= lp.price_start_time AND
      (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
WHERE
  month(u.usage_date) = month(CURRENT_DATE)
GROUP BY sku;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 04. Qual é a comparação de consumo do mês atual com o mês anterior?

-- COMMAND ----------

SELECT
  after.sku_name,
  before_dbus,
  after_dbus,
  ((after_dbus - before_dbus) / before_dbus * 100) AS growth_rate
FROM
  (
    SELECT
      sku_name,
      sum(usage_quantity) as before_dbus
    FROM system.billing.usage
    WHERE month(usage_date) = month(CURRENT_DATE) -1
    GROUP BY sku_name
  ) as before
  JOIN (
    SELECT
      sku_name,
      sum(usage_quantity) as after_dbus
    FROM system.billing.usage
    WHERE month(usage_date) = month(CURRENT_DATE)
    GROUP BY sku_name
  ) as
after
WHERE
  before.sku_name = after.sku_name SORT by growth_rate DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 05. Qual a quantidade de usuários ativos mensalmente?

-- COMMAND ----------

SELECT year_month,
      active_users,
      LAG(active_users, 1) OVER (ORDER BY year_month) AS active_users_previous_month,
      CASE
        WHEN LAG(active_users, 1) OVER (ORDER BY year_month) IS null THEN 0 
        ELSE active_users - LAG(active_users, 1) OVER (ORDER BY year_month)
      END AS growth,
      round(((active_users-active_users_previous_month)*100)/active_users_previous_month, 2) as perc_grow
FROM (
 SELECT date_format(event_time, 'MM/yy') as year_month,
        count(DISTINCT user_identity.email) as active_users
   FROM system.access.audit
  WHERE year(event_time) = year(current_date()) AND action_name IS NOT NULL
  GROUP BY year_month
  ORDER BY year_month)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 06. Quais são os clusters que mais consomem?
-- MAGIC

-- COMMAND ----------

WITH clusters AS (
  SELECT *
    FROM (SELECT *, row_number() OVER(PARTITION BY cluster_id ORDER BY change_time DESC) as rn
            FROM system.compute.clusters c)
   WHERE rn = 1
)

SELECT 
       c.cluster_name,
       sum(u.usage_quantity) as `DBUs Consumed`,
       sum(u.usage_quantity * lp.pricing.default) as `Total Dollar Cost`
  FROM system.billing.usage u
      LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
        u.sku_name = lp.sku_name AND
        u.usage_start_time >= lp.price_start_time AND
        (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
      INNER JOIN clusters c on u.usage_metadata.cluster_id = c.cluster_id
GROUP BY ALL
ORDER BY `Total Dollar Cost` DESC
LIMIT 15

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 07. Quem são os owners (donos) dos clusters interativos?

-- COMMAND ----------

SELECT 
  cluster_id, 
  owned_by AS usuario
FROM 
  system.compute.clusters
GROUP BY 
  cluster_id, 
  owned_by;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 08. Quais clusters interativos os usuários estão acessando?

-- COMMAND ----------

WITH user_cluster AS (
  SELECT DISTINCT a.user_identity.email,
         element_at(a.request_params, 'cluster_id') as cluster_id
   FROM system.access.audit a)

SELECT DISTINCT 
  c.cluster_source,
  a.cluster_id, 
  c.cluster_name,
  a.email
FROM user_cluster a JOIN system.compute.clusters c ON a.cluster_id = c.cluster_id
WHERE a.cluster_id IS NOT NULL
  AND c.cluster_source NOT IN ('JOB')
  --AND a.cluster_id = '< colocar o id do cluster se quiser filtrar >'
ORDER BY 1,2,3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 09. Quais usuários têm o maior tempo de execução dentro de um cluster interativo?

-- COMMAND ----------

SELECT
  user_identity.email,
  request_params.commandLanguage,
  sum(request_params.executionTime)/3600 as minutes
FROM
  system.access.audit
WHERE
  1 = 1
  AND action_name = 'runCommand'
  AND request_params.status NOT IN ('skipped')
GROUP BY ALL
ORDER BY minutes DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 10. Quais os comandos mais demorados dentro dos clusters interativos?

-- COMMAND ----------

SELECT
  event_date,
  user_identity.email,
  request_params.notebookId,
  request_params.clusterId,
  request_params.status,
  request_params.executionTime as seconds,
  request_params.executionTime/60 as minutes,
  request_params.executionTime/60/60 as hour,
  request_params.commandLanguage,
  request_params.commandId,
  request_params.commandText
FROM 
  system.access.audit a
WHERE 1=1 
  AND action_name = 'runCommand' 
  AND request_params.status NOT IN ('skipped') 
  AND TIMESTAMPDIFF(HOUR, event_date, CURRENT_TIMESTAMP()) < 24 * 90 
ORDER BY
  seconds DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 11. Qual é o consumo diário de Notebook Serverless?
-- MAGIC

-- COMMAND ----------

SELECT 
  u.usage_date, 
  u.sku_name, 
  u.billing_origin_product, 
  u.usage_quantity, 
  u.usage_type,
  u.usage_metadata, 
  u.custom_tags, 
  u.product_features
FROM system.billing.usage u
WHERE u.sku_name LIKE '%SERVERLESS%' 
  AND u.product_features.is_serverless -- for serverless only
  AND u.billing_origin_product IN ("NOTEBOOKS","INTERACTIVE")
ORDER BY u.usage_date DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 12. Quanto cada usuário consumiu de Notebook Serverless nos últimos 30 dias?
-- MAGIC

-- COMMAND ----------

SELECT
  usage_metadata.notebook_id,
  identity_metadata.run_as,
  SUM(usage_quantity) as total_dbu
FROM
  system.billing.usage
WHERE
  billing_origin_product in ("NOTEBOOKS","INTERACTIVE")
  and product_features.is_serverless -- SERVERLESS
  and usage_unit = 'DBU'
  and usage_date >= DATEADD(day, -30, current_date)
  --and identity_metadata.run_as = '< colocar o email do usuario se quiser filtrar >'
GROUP BY
  all
ORDER BY
  total_dbu DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 13. Quais são os Jobs no Databricks Workflow que mais consomem no período?

-- COMMAND ----------

SELECT usage_metadata.job_id as `Job ID`, 
       get_json_object(request_params.new_settings, '$.name') as `Job Name`,
       sum(u.usage_quantity) as `DBUs Consumed`,
       sum(u.usage_quantity * lp.pricing.default) as `Cost`
  FROM system.billing.usage u
      LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
        u.sku_name = lp.sku_name AND
        u.usage_start_time >= lp.price_start_time AND
        (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
      LEFT JOIN system.access.audit a ON u.usage_metadata.job_id = a.request_params.job_id
WHERE u.usage_metadata.job_id is not null
  AND a.request_params.new_settings is not null
  AND a.service_name = 'jobs'
GROUP BY ALL
ORDER BY `Cost` DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 14. Qual é o consumo diário de Job Serverless (últimos 60 dias)?

-- COMMAND ----------

SELECT
      us.usage_date,
      sum(us.usage_quantity) as dbus,
      dbus * any_value(lp.pricing.`default`) as cost_at_list_price
  FROM
      system.billing.usage us
      left join system.billing.list_prices lp on lp.sku_name = us.sku_name and lp.price_end_time is null
  WHERE
      us.usage_date >= DATE_SUB(current_date(), 60)
      AND us.sku_name like "%JOBS_SERVERLESS%"
  GROUP BY
      ALL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 15. Qual o valor de cada projeto? (verificando pela tag "Project")

-- COMMAND ----------

SELECT u.custom_tags.project as `Project`,
       sum(u.usage_quantity) as `DBUs Consumed`,
       sum(u.usage_quantity * lp.pricing.default) as `Total Dollar Cost`
  FROM system.billing.usage u
      LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
        u.sku_name = lp.sku_name AND
        u.usage_start_time >= lp.price_start_time AND
        (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
GROUP BY ALL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 16. Qual a quantidade de tokens utilizados nas interações com os foundations models?

-- COMMAND ----------

SELECT 
     u.ingestion_date as `Data`, 
     u.usage_quantity as `DBUs Consumido`,
     u.usage_quantity * lp.pricing.default as `Custo`
 FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
        u.sku_name = lp.sku_name AND
        u.usage_start_time >= lp.price_start_time AND
        (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
WHERE u.usage_type = 'TOKEN'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 17. Quais são as tabelas mais populares (mais acessada)?

-- COMMAND ----------

SELECT access_table, 
       count(access_table) as qtde_acesso
FROM (SELECT IFNULL(request_params.full_name_arg, 'Non-specific') AS access_table
        FROM system.access.audit
       WHERE action_name = 'getTable')
WHERE access_table NOT LIKE '__databricks%'
GROUP BY ALL
ORDER BY qtde_acesso DESC
LIMIT 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 18. Quem mais acessa essas tabelas populares?

-- COMMAND ----------

SELECT user_identity.email, 
       count(*) as qnt_acessos
  FROM system.access.audit
 WHERE request_params.table_full_name = '< colocar o nome da tabela >'
 GROUP BY ALL 
 ORDER BY 2 DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 19. Quais foram as tabelas acessadas por um usuário nas últimas 24 horas?

-- COMMAND ----------

SELECT action_name as `EVENTO`,
       event_time as `QUANDO`,
       IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABELA_ACESSADA`,
       IFNULL(request_params.commandText,'GET table') AS `QUERY_TEXT`
  FROM system.access.audit
 WHERE action_name IN ('createTable', 'commandSubmit','getTable','deleteTable','generateTemporaryTableCredential')
   AND datediff(now(), event_date) < 1
   --AND user_identity.email = '< colocar o email do usuario se quiser filtrar >'
ORDER BY event_date DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 20. Qual a linhagem de uma determinada tabela?

-- COMMAND ----------

SELECT DISTINCT target_table_full_name
  FROM system.access.table_lineage
 WHERE source_table_full_name = '< colocar o nome da tabela >'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 21. Quais as entidades que leem de uma determinada tabela?

-- COMMAND ----------

SELECT DISTINCT entity_type, 
       entity_id, 
       source_table_full_name
  FROM system.access.table_lineage
 WHERE source_table_full_name = '< colocar o nome da tabela >'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 22. Quais tabelas estão com comentários vazios?

-- COMMAND ----------

SELECT
    t.table_catalog as table_catalog
    ,t.table_schema as table_schema
    ,t.table_name as table_name
    ,t.table_catalog || '.' || t.table_schema || '.' || t.table_name    as table_unique_name
    ,t.table_owner as table_owner
    ,t.table_type as table_type
    ,t.last_altered_by as table_last_altered_by
    ,t.last_altered as table_last_altered_at
    ,t.data_source_format as table_data_source_format
    ,t.`comment` as table_comment
    ,case when table_comment is null then true else false end as table_empty_comment
FROM
    system.information_schema.tables t
WHERE
    true
    and t.table_catalog != "system"
    and t.table_catalog != "__databricks_internal"
    and t.table_schema != "information_schema"
    and t.`comment` is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 23. Quais colunas estão com comentários vazios?

-- COMMAND ----------

SELECT 
  c.table_catalog as table_catalog
  ,c.table_schema as table_schema
  ,c.table_name as table_name
  ,c.column_name as column_name
  ,c.table_catalog || '.' || c.table_schema || '.' || c.table_name || '.' || c.column_name  as column_unique_name
  ,c.is_nullable as column_is_nullable
  ,c.full_data_type as column_full_data_type
  ,c.`comment`as columns_comment
FROM
  system.information_schema.columns c
WHERE
  true
  and c.table_catalog != "system"
  and c.table_catalog != "__databricks_internal"
  and c.table_schema != "information_schema"
  and c.`comment` is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 24. Quais as tabelas menos populares (que não foram acessadas nos últimos 90 dias)?
-- MAGIC

-- COMMAND ----------

with used_tables as (
select
  source_table_catalog,
  source_table_schema,
  source_table_name,
  count(distinct created_by) as downstream_users,
  count(*) as downstream_dependents
from
  system.access.table_lineage
where
  true
  and source_table_full_name is not null
  and event_time >= date_add(now(), -90)
group by
  all
)

select
  tabelas.table_catalog
  ,tabelas.table_schema
  ,tabelas.table_name
  ,tabelas.table_type
  ,tabelas.table_owner
  ,tabelas.`comment` as table_comment
  ,tabelas.created as table_created_at
  ,tabelas.created_by as table_created_by
  ,tabelas.last_altered as table_last_update_at
  ,tabelas.last_altered_by as table_last_altered_by
from
  system.information_schema.`tables` as tabelas
  left join used_tables as ut on ut.source_table_catalog = tabelas.table_catalog and ut.source_table_schema = tabelas.table_schema and ut.source_table_name = tabelas.table_name
where
  true
  and ut.downstream_dependents is null
  and tabelas.table_catalog != "system"
  and tabelas.table_catalog != "__databricks_internal"
  and tabelas.table_schema != "information_schema"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 25. Qual foi o comando SQL executado em uma determinada tabela?

-- COMMAND ----------

SELECT 
       l.source_table_full_name,
       l.entity_type,
       q.statement_text,
       q.executed_by,
       q.end_time
  FROM system.access.table_lineage  l
  JOIN system.query.history q
  ON l.entity_run_id = q.statement_id
WHERE source_table_full_name = '< colocar o nome da tabela >
