# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Version Code Control
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 10-JUN-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## SYSTEM TABLES.  (system.access.audit)
# MAGIC
# MAGIC * https://docs.databricks.com/pt/admin/system-tables/index.html

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Quem acessa mais essa tabela?
# MAGIC
# MAGIC SELECT user_identity.email, count(*) as qnt_acessos
# MAGIC
# MAGIC FROM system.access.audit
# MAGIC
# MAGIC -- WHERE request_params.table_full_name = "CATALOG.SCHEMA.TABLE"
# MAGIC WHERE request_params.table_full_name = "tax.silver.nfe_prod"
# MAGIC
# MAGIC AND service_name = "unityCatalog"
# MAGIC
# MAGIC AND action_name = "generateTemporaryTableCredential"
# MAGIC
# MAGIC GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Quem apagou esta tabela?
# MAGIC
# MAGIC SELECT user_identity.email, count(*) as qnt_acessos
# MAGIC
# MAGIC FROM system.access.audit
# MAGIC
# MAGIC -- WHERE request_params.table_full_name = "CATALOG.SCHEMA.TABLE"
# MAGIC WHERE request_params.table_full_name = "tax.silver.nfe_prod"
# MAGIC
# MAGIC
# MAGIC AND service_name = "unityCatalog"
# MAGIC
# MAGIC AND action_name = "deleteTable"
# MAGIC
# MAGIC GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- O que esse usuário acessou nas últimas 24 horas?
# MAGIC
# MAGIC SELECT request_params.table_full_name
# MAGIC
# MAGIC FROM system.access.audit
# MAGIC
# MAGIC WHERE user_identity.email = "luis.assuncao@databricks.com"
# MAGIC
# MAGIC AND service_name = "unityCatalog"
# MAGIC
# MAGIC AND action_name = "generateTemporaryTableCredential"
# MAGIC
# MAGIC AND datediff(now(), event_date) <= 1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Quais tabelas esse usuário acessa com mais frequência?
# MAGIC
# MAGIC SELECT request_params.table_full_name, COUNT(*) as num_acessos
# MAGIC
# MAGIC FROM system.access.audit
# MAGIC
# MAGIC WHERE user_identity.email = "luis.assuncao@databricks.com"
# MAGIC
# MAGIC and request_params.table_full_name is not null
# MAGIC
# MAGIC AND service_name = "unityCatalog"
# MAGIC
# MAGIC AND action_name = "generateTemporaryTableCredential"
# MAGIC
# MAGIC GROUP BY 1 ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verifica os acessos concedidos
# MAGIC
# MAGIC -- SHOW GRANTS ON TABLE  "CATALOG.SCHEMA.TABLE"
# MAGIC
# MAGIC
# MAGIC SHOW GRANTS ON TABLE  tax.silver.nfe_prod
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- Quantidade de Users ATIVOS por Mes
# MAGIC
# MAGIC SELECT year_month,
# MAGIC active_users,
# MAGIC LAG(active_users, 1)
# MAGIC OVER (ORDER BY year_month)
# MAGIC AS active_users_previous_month,
# MAGIC
# MAGIC CASE
# MAGIC WHEN LAG(active_users, 1)
# MAGIC OVER (ORDER BY year_month)
# MAGIC IS null THEN 0 -- o primeiro mês não tem mês anterior
# MAGIC ELSE active_users - LAG(active_users, 1)
# MAGIC OVER (ORDER BY year_month)
# MAGIC END AS growth,
# MAGIC round(((active_users-active_users_previous_month)*100)/active_users_previous_month, 2) as perc_grow
# MAGIC FROM (
# MAGIC SELECT date_format(event_time, 'MM/yy') as year_month,
# MAGIC count(DISTINCT user_identity.email) as active_users
# MAGIC FROM system.access.audit
# MAGIC WHERE year(event_time) = year(current_date()) AND action_name IS NOT NULL
# MAGIC GROUP BY year_month
# MAGIC
# MAGIC ORDER BY year_month)
