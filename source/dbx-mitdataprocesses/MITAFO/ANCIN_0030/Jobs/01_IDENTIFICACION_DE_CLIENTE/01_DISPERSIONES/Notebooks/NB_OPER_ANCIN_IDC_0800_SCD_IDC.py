# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

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
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0800_SCD_IDC")))

# COMMAND ----------

temp_var = ""
if params.var_tramite == "IMSS":
    temp_var = "FTN_NSS"
elif params.var_tramite == "ISSSTE":
    temp_var = "FTC_CURP"
elif params.var_tramite == "AEIM":
    temp_var = "FTN_NSS"
else:
    raise Exception("No se ha definido el parámetro var_tramite")

# COMMAND ----------

if params.var_tramite == "IMSS" or params.var_tramite == "AEIM":
    statement_001 = query.get_statement(
        "IDC_0800_SCD_IDC_001.sql",
        CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
        SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
        SR_ID_ARCHIVO=params.sr_id_archivo,
    )
elif params.var_tramite == "ISSSTE":
        statement_001 = query.get_statement(
        "IDC_0800_SCD_IDC_006.sql",
        CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
        SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
        SR_ID_ARCHIVO=params.sr_id_archivo,
    )
else:
    raise Exception("No se ha definido el parámetro var_tramite")

# COMMAND ----------

df = db.sql_delta(statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, nvl, to_date

# Filtrar y seleccionar columnas en una sola operación
df_nulos = df.filter(col("FLAG").isNull()).select(
    "FTC_FOLIO",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_NOMBRE_CTE",
    "FTN_NUM_CTA_INVDUAL",
    "FTN_ESTATUS_DIAG",
    nvl(col("FTC_ID_DIAGNOSTICO"), lit('')).alias("FTC_ID_DIAGNOSTICO"),
    nvl(col("FTC_ID_SUBP_NO_VIG"), lit('')).alias("FTC_ID_SUBP_NO_VIG"),
    to_date(col("FTD_FECHA_CERTIFICACION"), 'dd/MM/yyyy').alias("FTD_FECHA_CERTIFICACION"),
    "FTN_CTE_PENSIONADO"
)

if conf.debug:
    display(df_nulos)

# COMMAND ----------

table_name_001 = 'CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE'
db.write_data(df_nulos, table_name_001, "default", "append")

# COMMAND ----------

# DBTITLE 1,FILTRADO DE INFORMACIÓN BANDERA NO NULA

from pyspark.sql.functions import col

if params.var_tramite == "IMSS" or params.var_tramite == "AEIM":
    df_nonulos = df.where(
        (col("FLAG").isNotNull()) &
        (col("FTN_NUM_CTA_INVDUAL") != col("FTN_NUM_CTA_INVDUAL_2")) |
        (col("FTN_ESTATUS_DIAG") != col("FTN_ESTATUS_DIAG_2")) |
        (col("FTC_ID_DIAGNOSTICO") != col("FTC_ID_DIAGNOSTICO_2")) |
        (col("FTC_ID_SUBP_NO_VIG") != col("FTC_ID_SUBP_NO_VIG_2")) |
        (col("FTN_CTE_PENSIONADO") != col("FTN_CTE_PENSIONADO_2")) |
        (col("FTD_FECHA_CERTIFICACION") != col("FTD_FECHA_CERTIFICACION_2"))
    ).select(
        "FTC_FOLIO",
        "FTN_NSS",
        "FTC_CURP",
        "FTN_NUM_CTA_INVDUAL",
        "FTN_ESTATUS_DIAG",
        "FTC_ID_DIAGNOSTICO",
        "FTC_ID_SUBP_NO_VIG",
        "FTN_CTE_PENSIONADO",
        "FTD_FECHA_CERTIFICACION"
    )
    df.unpersist()
    del df
    df_nonulos.cache()

    if conf.debug:
        display(df_nonulos)

elif params.var_tramite == "ISSSTE":
    df_nonulos = df.where(
        (col("FLAG").isNotNull()) &
        (col("FTN_NUM_CTA_INVDUAL") != col("FTN_NUM_CTA_INVDUAL_2")) |
        (col("FTN_ESTATUS_DIAG") != col("FTN_ESTATUS_DIAG_2")) |
        (col("FTC_ID_DIAGNOSTICO") != col("FTC_ID_DIAGNOSTICO_2")) |
        (col("FTC_ID_SUBP_NO_VIG") != col("FTC_ID_SUBP_NO_VIG_2")) |
        (col("FTN_CTE_PENSIONADO") != col("FTN_CTE_PENSIONADO_2")) |
        (col("FTD_FECHA_CERTIFICACION") != col("FTD_FECHA_CERTIFICACION_2"))
    ).select(
        "FTC_FOLIO",
        "FTN_NSS",
        "FTC_CURP",
        "FTN_NUM_CTA_INVDUAL",
        "FTN_ESTATUS_DIAG",
        "FTC_ID_DIAGNOSTICO",
        "FTC_ID_SUBP_NO_VIG",
        "FTD_FECHA_CERTIFICACION",
        "FTN_CTE_PENSIONADO"
    )
    df.unpersist()
    del df
    df_nonulos.cache()

    if conf.debug:
        display(df_nonulos)
else:
    raise Exception("No se ha definido el parámetro var_tramite")

# COMMAND ----------

# DBTITLE 1,CAMBIO DE TIPO DE DATO FTC_ID_SUBP_NO_VIG
from pyspark.sql.functions import col

df_nonulos = df_nonulos.withColumn("FTC_ID_SUBP_NO_VIG", col("FTC_ID_SUBP_NO_VIG").cast("decimal(10,0)"))
  
if conf.debug:
    display(df_nonulos)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cambiamos los campos y propiedas del df_nonulos para poder insertarlo en la tabla auxiliar.

# COMMAND ----------

from pyspark.sql.functions import col

df_nonulos = df_nonulos.select(
    col("FTC_FOLIO").alias("FTC_FOLIO"),
    col("FTN_NSS").alias("FTC_NSS"),
    col("FTC_CURP").alias("FTC_CURP"),
    col("FTN_NUM_CTA_INVDUAL").alias("FTN_NUM_CTA_INVDUAL"),
    col("FTN_ESTATUS_DIAG").alias("FTN_ESTATUS_DIAG"),
    col("FTC_ID_DIAGNOSTICO").alias("FTN_ID_DIAGNOSTICO"),
    col("FTC_ID_SUBP_NO_VIG").alias("FTN_ID_SUBP_NO_VIG"),
    col("FTN_CTE_PENSIONADO").alias("FTN_CTE_PENSIONADO"),
    to_date(col("FTD_FECHA_CERTIFICACION"), 'dd/MM/yyyy').alias("FTD_FECHA_CERTIFICACION")
)

#df_nonulos.count()

# Mostrar el DataFrame con los nuevos nombres de columnas
if conf.debug:
    display(df_nonulos)

# COMMAND ----------

table_name_001 = 'CIERREN_DATAUX.TTSISGRAL_ETL_VAL_IDENT_CTE_AUX'
db.write_data(df_nonulos, table_name_001, "default", "append")

# COMMAND ----------

if params.var_tramite == "IMSS" or params.var_tramite == "AEIM":
    statement_002 = query.get_statement(
        "IDC_0800_SCD_IDC_002.sql",
        SR_FOLIO=params.sr_folio,
    )
elif params.var_tramite == "ISSSTE":
    statement_002 = query.get_statement(
        "IDC_0800_SCD_IDC_003.sql",
        SR_FOLIO=params.sr_folio,
    )
else:
    raise Exception("No se ha definido el parámetro var_tramite")

# COMMAND ----------

execution = db.execute_oci_dml(
    statement=statement_002, async_mode=False
)

# COMMAND ----------

statement_003 = query.get_statement(
    "IDC_0800_SCD_IDC_004.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

execution = db.execute_oci_dml(
    statement=statement_003, async_mode=False
)

# COMMAND ----------

statement_004 = query.get_statement(
    "IDC_0800_SCD_IDC_005.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_final = db.read_data("default", statement_004)
if conf.debug:
    display(df_final)

# COMMAND ----------

validacion = "1" # Si ejecuta el NB 0700
# para que no se ejecute el NB 0700, validacion debe ser cero.
if df_final.count() > 0 and (params.sr_subproceso in ["8", "439", "121"]):
    validacion = "0" # No ejecuta el NB 0700
dbutils.jobs.taskValues.set(key="validacion", value=validacion)
logger.info("validacion: " + validacion)

# COMMAND ----------

# validacion = "0" 
# df_validacion = df_final.filter(col("FTN_ESTATUS_DIAG") == 0)
# val_temp = df_validacion.isEmpty() # --> # True, False
# if val_temp: # Aque pregunta si val_temp es True o False
#     validacion = "1"  # Todos los clientes fueron identificados
# dbutils.jobs.taskValues.set(key="validacion", value=validacion)

# El código realiza lo siguiente paso a paso:
# - Si la tabla TTSISGRAL_ETL_VAL_IDENT_CTE tiene registros con valor Cero (0) en el campo FTN_ESTATUS_DIAG. NO INSERTA EN EL ELT_DISPERSIONES (Se salta el NB 0700)
# - Si la tabla TTSISGRAL_ETL_VAL_IDENT_CTE no tiene registros con valor Cero (0) en el campo FTN_ESTATUS_DIAG. INSERTA EN EL ELT_DISPERSIONES (Se ejecuta el NB 0700)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())