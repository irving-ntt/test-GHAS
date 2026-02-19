# Databricks notebook source
# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_origen_arc": str,
    "sr_dt_org_arc": str,
    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
    "var_tramite": str,
})
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,Construcción de consulta y dataframe de paso
statement_001 = query.get_statement(
    "IDC_0050_EXT_IMSS_ISSSTE_001.sql",
    SR_FOLIO=params.sr_folio,
)
df = db.read_data("default", statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,Construcción de 2da consulta
statement_002 = query.get_statement(
    "IDC_0050_EXT_IMSS_ISSSTE_002.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Creación de tablas delta
db.write_delta('TEMP_CLIENTES_DISPERSION' + '_' + params.sr_id_archivo, df.select('FTC_FOLIO', 'FTN_NSS_CURP', 'FCN_ID_TIPO_SUBCTA'), "overwrite")

db.write_delta('TEMP_IDEN_CLIENTE' + '_' + params.sr_id_archivo, df.select('FTC_FOLIO','FTN_NSS_CURP','FTN_ID_ARCHIVO', 'FTN_NO_LINEA', 'FTN_NSS','FTC_CURP').distinct(), "overwrite")

db.write_delta('TEMP_CLIENTE_NO_IDEN' + '_' + params.sr_id_archivo, db.read_data("default", statement_002), "overwrite")


if conf.debug:
    display(db.read_delta('TEMP_CLIENTES_DISPERSION' + '_' + params.sr_id_archivo))
    display(db.read_delta('TEMP_IDEN_CLIENTE' + '_' + params.sr_id_archivo))
    display(db.read_delta('TEMP_CLIENTE_NO_IDEN' + '_' + params.sr_id_archivo))

# COMMAND ----------

# DBTITLE 1,Consulta y tabla delta final
statement_003 = query.get_statement(
    "IDC_0050_EXT_IMSS_ISSSTE_003.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

db.write_delta('TEMP_UNION_CLIENTE' + '_' + params.sr_id_archivo, db.sql_delta(statement_003), "overwrite")

if conf.debug:
    display(db.read_delta('TEMP_UNION_CLIENTE' + '_' + params.sr_id_archivo))

# COMMAND ----------

# DBTITLE 1,Liberación de recursos
del df
CleanUpManager.cleanup_notebook(locals())
