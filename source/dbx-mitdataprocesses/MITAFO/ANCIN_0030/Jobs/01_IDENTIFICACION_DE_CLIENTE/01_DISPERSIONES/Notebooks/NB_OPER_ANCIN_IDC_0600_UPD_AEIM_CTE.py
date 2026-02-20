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
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0600_UPD_AEIM")))

# COMMAND ----------

# DBTITLE 1,Creación de consulta
statement_001 = query.get_statement(
    "IDC_0600_UPD_AEIM_CTE_001.sql",
    CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
    SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
    SR_ID_ARCHIVO=params.sr_id_archivo,
)

# COMMAND ----------

# DBTITLE 1,Ejecución de consulta
df=db.sql_delta(statement_001)
if conf.debug:
    display(df)
    display(df.count())

# COMMAND ----------

# DBTITLE 1,Creación tabla de paso delta
db.write_delta(f"TEMP_CTAS_AEIM_CTE_{params.sr_id_archivo}", df, "overwrite")
if conf.debug:
    display(db.read_delta(f"TEMP_CTAS_AEIM_CTE_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE 1,CREACION TABLA DELTA TEMP_CTAS_VIG_CARGA
df1 = df.select('FTC_CLAVE_TMP','FTN_ESTATUS_DIAG','FTN_NUM_CTA_INVDUAL','FTC_ID_DIAGNOSTICO','FTC_ID_SUBP_NO_VIG','FTD_FECHA_CERTIFICACION','FTN_CTE_PENSIONADO','FTC_FOLIO', 'FTN_NSS','FTC_CLAVE_ENT_RECEP','FTN_ID_ARCHIVO','FTC_RFC','FTC_CURP','FTC_NOMBRE_CTE')
temp_view = 'TEMP_CTAS_VIG_CARGA' + '_' + params.sr_id_archivo
db.write_delta(temp_view, df1, "overwrite")

if conf.debug:
    display(df1)


# COMMAND ----------

# DBTITLE 1,Primera versión
#df_temp = df_temp.withColumn('FTC_CLAVE_TMP', regexp_replace(df_temp['FTC_CLAVE_TMP'], '^0+', ''))
#if conf.debug:
#    display(df_temp)
#    display(df_temp.count())
#
#db.write_delta(f"TEMP_DISP_IMSS_ISSSTE_FORMATED_{params.sr_id_archivo}", df_temp, "overwrite")

# COMMAND ----------

# statement_002 = query.get_statement(
#     "IDC_0600_UPD_AEIM_CTE_002.sql",
#     CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
#     SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
#     SR_ID_ARCHIVO=params.sr_id_archivo,
# )

statement_002 = query.get_statement(
    "IDC_0600_UPD_AEIM_CTE_002.sql",
    CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
    SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
    SR_ID_ARCHIVO=params.sr_id_archivo,
)

# COMMAND ----------

df = db.sql_delta(statement_002)
if conf.debug:
    display(df)
    display(df.count())

# COMMAND ----------

df = df.dropDuplicates([
    "FTC_FOLIO",
    "FTC_CLAVE_TMP",
    "FTC_RFC_ARCH",
    "FTC_NOMBRE_COMPLETO_ARCH",
    "FNC_REG_PATRONAL_IMSS",
])

if conf.debug:
    display(df)
    display(df.count())


# COMMAND ----------

df_final = df.select('FTC_FOLIO','FTN_NSS_ARCH','FTC_RFC_ARCH','FTC_NOMBRE_COMPLETO_ARCH','FNC_REG_PATRONAL_IMSS','FTN_NUM_CTA_INVDUAL','FTN_ESTATUS_REG','FTN_MOTIVO', 'FTC_CLAVE_ENT_RECEP','FTC_NOMBRE_BUC','FTC_AP_PATERNO_BUC','FTC_AP_MATERNO_BUC','FTC_RFC_BUC')


if conf.debug:
    display(df_final)
    display(df_final.count())

# COMMAND ----------

# DBTITLE 1,Guardar datos en tabla auxiliar
table_name_aux = "CIERREN_DATAUX.TTSISGRAL_ETL_AEIM_AUX"
db.write_data(df_final, table_name_aux, "default", "append")

# COMMAND ----------

# DBTITLE 1,Armar el statement para el merge
statement_003 = query.get_statement(
    "IDC_0600_UPD_AEIM_CTE_003.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecutar el merge
execution = db.execute_oci_dml(
    statement=statement_003, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Crear statement de borrado de la auxiliar
statement_004 = query.get_statement(
    "IDC_0600_UPD_AEIM_CTE_004.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecutar el borrado de la auxiliar
execution = db.execute_oci_dml(
    statement=statement_004, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Limpiar data usada en el NB
CleanUpManager.cleanup_notebook(locals())