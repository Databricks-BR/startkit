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
# MAGIC ## SYSTEM TABLES.  (system.billing.usage)
# MAGIC
# MAGIC * https://docs.databricks.com/en/administration-guide/system-tables/billing.html
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## TAG settings - Usage Detail
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/admin/account-settings/usage-detail-tags

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Criando tags por áreas/projetos em clusters e jobs
# MAGIC
# MAGIC select
# MAGIC     date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC     sum(lp.pricing.default * u.usage_quantity) as list_cost,
# MAGIC     u.custom_tags.Team
# MAGIC
# MAGIC from system.billing.usage u
# MAGIC
# MAGIC inner join system.billing.list_prices lp
# MAGIC
# MAGIC on u.cloud = lp.cloud and
# MAGIC u.sku_name = lp.sku_name and
# MAGIC u.usage_start_time >= lp.price_start_time
# MAGIC
# MAGIC GROUP BY
# MAGIC     custom_tags.Team,
# MAGIC     date_format(usage_date, 'yyyy-MM')
# MAGIC
