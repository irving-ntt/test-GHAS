# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Genera datos complementarios para No Encontrados
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC Tablas INPUT:
# MAGIC     N/A
# MAGIC Tablas OUTPUT:
# MAGIC     N/A
# MAGIC Tablas INPUT DELTA:
# MAGIC     DELTA_ENCONTRADOS
# MAGIC 	DELTA_FECHA
# MAGIC 	DELTA_NO_ENCONTRADOS_
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     DELTA_VARIASCTAS
# MAGIC 	DELTA_CARGA
# MAGIC Archivos SQL:
# MAGIC     SQL_IDC_500_ENCONTRADOS.sql
# MAGIC 	SQL_IDC_500_NO_ENCONTRADOS.sql
# MAGIC 	SQL_IDC_500_CARGA.sql
# MAGIC 	SQL_IDC_500_VARIAS_CTAS.sql
# MAGIC

# COMMAND ----------

# DBTITLE 1,INICIO - CONFIGURACIONES PRINCIPALES
# MAGIC %run "./startup" 

# COMMAND ----------

# DBTITLE 1,PARAMETROS
# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({

    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_proceso": str,
    "sr_subetapa": str,
    "sr_dt_org_arc": str,
    "sr_origen_arc": str,
    "sr_subproceso": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
})

# Validar widgets
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,VALIDA EXISTENCIA SQL
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(
    queries_df.filter(
        col("Archivo SQL").startswith("SQL_IDC_500")
    )
)

# COMMAND ----------

# DBTITLE 1,APLICA REGLAS SOBRE DELTAS ENCONTRADOS y FECHA
statement_1 = query.get_statement(
    "SQL_IDC_500_ENCONTRADOS.sql",
    DELTA_TABLA_ENCONTRADOS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_ENCONTRADOS_{params.sr_folio}",
    DELTA_TABLA_FECHA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_FECHA_{params.sr_folio}"
)

df_encontrados_500 = db.sql_delta(statement_1)

if conf.debug:
    display(df_encontrados_500)


# COMMAND ----------

# DBTITLE 1,APLICA REGLAS SOBRE DELTAS NO ENCONTRADOS
statement_2 = query.get_statement(
    "SQL_IDC_500_NO_ENCONTRADOS.sql",
    DELTA_TABLA_NO_ENCONTRADOS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_NO_ENCONTRADOS_{params.sr_folio}"
)

df_no_encontrados_500 = db.sql_delta(statement_2)

if conf.debug:
    display(df_no_encontrados_500)

# COMMAND ----------

# DBTITLE 1,UNE RESULTADO DE ENCONTRADOS Y NO ENCONTRADOS
df_union_500 = df_encontrados_500.union(df_no_encontrados_500)

if conf.debug:
    display(df_union_500)

db.write_delta(f"DELTA_500_MARCA_{params.sr_folio}", df_union_500, "overwrite")

# COMMAND ----------

# DBTITLE 1,GENERA DELTA VARIAS CUENTAS
statement_3 = query.get_statement(
    "SQL_IDC_500_VARIAS_CTAS.sql",
    DELTA_500_CARGA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_500_MARCA_{params.sr_folio}"
)

df_variasctas = db.sql_delta(statement_3)

if conf.debug:
    display(df_variasctas)

db.write_delta(f"DELTA_VARIASCTAS_{params.sr_folio}", df_variasctas, "overwrite")

# COMMAND ----------

# DBTITLE 1,GENERA DELTA CARGA
statement_4 = query.get_statement(
    "SQL_IDC_500_CARGA.sql",
    DELTA_500_CARGA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_500_MARCA_{params.sr_folio}"
)

df_carga = db.sql_delta(statement_4)

if conf.debug:
    display(df_carga)

db.write_delta(f"DELTA_CARGA_{params.sr_folio}", df_carga, "overwrite")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
