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
# MAGIC     DELTA_VIGENTES
# MAGIC 	DELTA_NO_VIGENTES
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     DELTA_ENCONTRADOS
# MAGIC Archivos SQL:
# MAGIC     SQL_IDC_400_NO_VIGENTES.sql
# MAGIC 	SQL_IDC_400_VIGENTES.sql
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
if conf.debug:
    display(
        queries_df.filter(
            col("Archivo SQL").startswith("SQL_IDC_400")
        )
    )

# COMMAND ----------

# DBTITLE 1,EXTRAE DATOS DE DELTA CURP
statement_1 = query.get_statement(
    "SQL_IDC_401_CURP.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CURP_{params.sr_folio}"
)

df_curp = db.sql_delta(statement_1)

if conf.debug:
    display(df_curp)

# COMMAND ----------

# DBTITLE 1,UNE RESULTADO CON ENCONTRADOS NSS
statement_curp = query.get_statement(
    "SQL_IDC_402_CURP.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_ENCONTRADOS_NSS_{params.sr_folio}"
)
df_encontrados_nss = db.sql_delta(statement_curp)


df_union = df_curp.union(df_encontrados_nss)

if conf.debug:
    display(df_union)

db.write_delta(f"DELTA_400_ENCONTRADOS_{params.sr_folio}", df_union, "overwrite")



# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
