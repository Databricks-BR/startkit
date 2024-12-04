-- Databricks notebook source
-- MAGIC
-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### Controle de Versão do Código (Pipeline)
-- MAGIC
-- MAGIC | versão | data | autor | e-mail | alterações |
-- MAGIC | --- | --- | --- | --- | --- |
-- MAGIC | 1.0 | 20-nov-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | Item | Descrição |
-- MAGIC | --- | --- |
-- MAGIC | **Objetivo Pipeline** | Criação da Estrutura dos Catálogos e Schemas |
-- MAGIC | **Camada** | Governança de Dados |
-- MAGIC | **Databricks Run Time** | DBR 15.4 LTS |
-- MAGIC | **Linguagem** | SQL |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criação dos Catálogos

-- COMMAND ----------

create catalog if not exists bronze_prd COMMENT 'Camada Bronze Produção - Dados não tratados';
create catalog if not exists silver_prd COMMENT 'Camada Silver Produção - Dados enriquecidos';
create catalog if not exists gold_prd COMMENT 'Camada Gold Produção - Dados de Negócio';

create catalog if not exists bronze_dev COMMENT 'Camada Bronze Desenvolvimento - Dados não tratados';
create catalog if not exists silver_dev COMMENT 'Camada Silver Desenvolvimento - Dados enriquecidos';
create catalog if not exists gold_dev COMMENT 'Camada Gold Desenvolvimento - Dados de Negócio';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criação dos Schemas / Databases
-- MAGIC #### Camada BRONZE - separado por sistema de origem

-- COMMAND ----------

use catalog bronze_dev;


create schema if not exists salesforce COMMENT 'Dados do sistema SALESFORCE';

create schema if not exists sap COMMENT 'Dados do sistema SAP';

use catalog bronze_prd;


create schema if not exists salesforce COMMENT 'Dados do sistema SALESFORCE';

create schema if not exists sap COMMENT 'Dados do sistema SAP';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criação dos Schemas / Databases
-- MAGIC #### Camada SILVER - separado por sistema de origem

-- COMMAND ----------

use catalog silver_dev;


create schema if not exists salesforce COMMENT 'Dados do sistema SALESFORCE';

create schema if not exists sap COMMENT 'Dados do sistema SAP';

use catalog silver_prd;


create schema if not exists salesforce COMMENT 'Dados do sistema SALESFORCE';

create schema if not exists sap COMMENT 'Dados do sistema SAP';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criação dos Schemas / Databases
-- MAGIC #### Camada GOLD - separado por domínio de dados / departamento

-- COMMAND ----------

use catalog gold_dev;

create SCHEMA if not exists corporativo;
create SCHEMA if not exists diretoria;
create SCHEMA if not exists clinica;
create SCHEMA if not exists operacao;
create SCHEMA if not exists financeiro;
create SCHEMA if not exists comercial;
create SCHEMA if not exists contabil;
create SCHEMA if not exists rh;
create SCHEMA if not exists fiscal;
create SCHEMA if not exists marketing;

use catalog gold_prd;

create SCHEMA if not exists corporativo;
create SCHEMA if not exists diretoria;
create SCHEMA if not exists clinica;
create SCHEMA if not exists operacao;
create SCHEMA if not exists financeiro;
create SCHEMA if not exists comercial;
create SCHEMA if not exists contabil;
create SCHEMA if not exists rh;
create SCHEMA if not exists fiscal;
create SCHEMA if not exists marketing;

