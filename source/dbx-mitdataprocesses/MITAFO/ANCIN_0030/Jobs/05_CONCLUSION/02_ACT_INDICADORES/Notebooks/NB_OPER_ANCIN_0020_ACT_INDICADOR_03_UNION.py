# Databricks notebook source
# DBTITLE 1,Descripción general
'''
Descripcion:
    Es la parte final de la subetapa, uniendo las tablas delta correspondientes para obtener los indicadores a actualizar.
Subetapa:
    ACTUALIZACIÓN DE INDICADORES
Trámite:
      8 - DISPERSION ORDINARIA IMSS
    439 - GUBERNAMENTAL IMSS
    121 - INTERESES EN TRÁNSITO IMSS
    118 - ACLARACIONES ESPECIALES
    120 - DISPERSIÓN ORDINARIA ISSSTE
    210 - GUBERNAMENTAL ISSSTE
    122 - INTERESES EN TRÁNSITO ISSSTE
Tablas INPUT:
    N/A
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    ORACLE_DISPERSIONES
    DELTA_DISPER_ACT_IND_01
Tablas OUTPUT DELTA:
    DELTA_DISPER_ACT_IND_02
Archivos SQL:
    0020_ACT_INDICADOR_002.sql
'''

# COMMAND ----------

# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_folio": str,
        "sr_dt_org_arc": str,
        "sr_origen_arc": str,
        "sr_tipo_mov": str,
        "sr_fecha_acc": str,
        "sr_accion": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
        "sr_tipo_archivo": str,
        "sr_estatus_mov": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("0020_ACT_INDICADOR")))

# COMMAND ----------

# DBTITLE 1,Construcción consulta principal
statement_001 = query.get_statement(
    "0020_ACT_INDICADOR_003.sql",
    SR_FOLIO=params.sr_folio,
    SR_USUARIO=params.sr_usuario,
    SR_SUBPROCESO=params.sr_subproceso,
    CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
    SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
)

# COMMAND ----------

# DBTITLE 1,Unión de indicadores
df_indicadores = db.sql_delta(statement_001)
if conf.debug:
    display(df_indicadores)

# COMMAND ----------

# DBTITLE 1,Eliminación de duplicados
df_final = df_indicadores.dropDuplicates(["FCN_ID_IND_CTA_INDV", "FTN_NUM_CTA_INVDUAL", "FFN_ID_CONFIG_INDI", "FCC_VALOR_IND", "FTC_VIGENCIA"])

# Si está en modo debug True, muestra el DataFrame resultante
if conf.debug:
    display(df_final)

# COMMAND ----------

# DBTITLE 1,Filtro ind cuenta individual nula
from pyspark.sql.functions import col, isnull, isnotnull

# Registros con una cuenta individual nula  (FTN_ID_CTA_INVDUAL)
df_nulos = df_final.filter(isnull(col('FCN_ID_IND_CTA_INDV')))

#Seleccionar los campos para los registros que se van a insertar
df_nulos_insert = df_nulos.select(["FTN_NUM_CTA_INVDUAL", "FFN_ID_CONFIG_INDI", "FCC_VALOR_IND", "FTC_VIGENCIA", "FTD_FEH_REG", "FTC_USU_REG", "FTD_FEH_ACT","FCC_USU_ACT"])

# Si está en modo debug True, muestra el DataFrame resultante
if conf.debug:
    display(df_nulos_insert)


# COMMAND ----------

# DBTITLE 1,Inserta registros en tabla IND_CTA_INV
db.write_data(df_nulos_insert, "CIERREN.TTAFOGRAL_IND_CTA_INDV", "default", "append")

# COMMAND ----------

# DBTITLE 1,Filtro indi cuenta individual no nula
from pyspark.sql.functions import col, isnotnull, lit

# Registros con una cuenta individual NO nula  (FTN_ID_CTA_INVDUAL)
df_no_nulos = df_final.filter(isnotnull(col('FCN_ID_IND_CTA_INDV')))

# Agregar una nueva columna 'SR_FOLIO' al DataFrame df_no_nulos
df_no_nulos_folio = df_no_nulos.withColumn('FTC_FOLIO', lit(params.sr_folio))

# Si está en modo debug True, muestra el DataFrame resultante
if conf.debug:
    display(df_no_nulos_folio)


# COMMAND ----------

# DBTITLE 1,Llenado de tabla auxiliar
table_name_aux = "CIERREN_DATAUX.TTAFOGRAL_ETL_IND_CTA_INDV_AUX"
db.write_data(df_no_nulos_folio, table_name_aux, "default", "append")

# COMMAND ----------

# DBTITLE 1,Construcción de sentencia para merge
statement_002 = query.get_statement(
    "0020_ACT_INDICADOR_004.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecución de merge
execution = db.execute_oci_dml(
    statement=statement_002, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Depuración de dataframes usados
CleanUpManager.cleanup_notebook(locals())

# COMMAND ----------

# DBTITLE 1,Fin
#Notificación de finalizado el flujo
Notify.send_notification("INFO", params) 
