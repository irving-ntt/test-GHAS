# Databricks notebook source
'''
Descripcion:
    Carga para la generación de la linea 9 del archivo de respuesta
Subetapa:
    20 - ARCHIVO RESPUESTA
Trámite:
    118 - DAEIMSS
    120 - DOISSSTE
    122 - DITISSSTE
Tablas INPUT:
    N/A
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_LEE_ARCH_#SR_ID_ARCHIVO#
    DELTA_ACLARACIONES_ESPECIALES_200_#SR_ID_ARCHIVO#
    DELTA_ACLARACIONES_ESPECIALES_300_#SR_ID_ARCHIVO#
    DELTA_ACLARACIONES_ESPECIALES_400_#SR_ID_ARCHIVO#
Tablas OUTPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_ARCH_RESP_#SR_ID_ARCHIVO#
Archivos SQL:
    ACLARACIONES_ESPECIALES_008.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio" : str,
    "sr_id_archivo": str,
    "sr_fec_arc" : str,
    "sr_fec_liq" : str,
    "sr_dt_org_arc" : str,
    "sr_origen_arc" : str,
    "sr_tipo_layout": str,
    "sr_tipo_reporte" : str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_etapa": str,
    "sr_id_snapshot": str
})
# Validar widgets
params.validate()

# COMMAND ----------

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

file_manager = FileManager(err_repo_path=conf.err_repo_path)

# COMMAND ----------

DELTA_TABLE_LEE_ARCH = "DELTA_ACLARACIONES_ESPECIALES_LEE_ARCH_" + params.sr_id_archivo
DELTA_TABLE_200 = "DELTA_ACLARACIONES_ESPECIALES_200_" + params.sr_id_archivo
DELTA_TABLE_300 = "DELTA_ACLARACIONES_ESPECIALES_300_" + params.sr_id_archivo
DELTA_TABLE_400 = "DELTA_ACLARACIONES_ESPECIALES_400_" + params.sr_id_archivo

# COMMAND ----------

# DBTITLE 1,VALIDACION 01
#Ejecuta consulta para el 01
statement_001 = query.get_statement(
    "ACLARACIONES_ESPECIALES_008.sql",
    DELTA_TABLE_LEE_ARCH=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_LEE_ARCH}",
    DELTA_ACL_ESP_200=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_200}",
    DELTA_ACL_ESP_300=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_300}",
    DELTA_ACL_ESP_400=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_400}",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    SR_SUBPROCESO=params.sr_subproceso
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

# COMMAND ----------

df = df.fillna(0)

# COMMAND ----------

if conf.debug:
    display(df)

# COMMAND ----------

df_mod = df

# COMMAND ----------

from pyspark.sql.functions import col, format_number, lpad, format_string

# List of columns to format
columns_to_format = ['FTC_IMPORTE_AAS_48','FTC_IMPORTE_AAS_49','FTC_IMPORTE_ACR_46','FTC_IMPORTE_ALP_47',
'FTC_IMPORTE_AVO_45','FTC_IMPORTE_CSO_44','FTC_IMPORTE_CVP_28','FTC_IMPORTE_CVP_32',
'FTC_IMPORTE_CVT_29','FTC_IMPORTE_RET_31','FTC_IMPORTE_SAR_26','FTC_IMPORTE_VIE_36',
'FTC_IMPORTE_VIE_37','FTC_IMPORTE_VIV_34','FTC_IMPORTE_VIV_35','FTC_IMPORTE_CVT_33',
'FTC_IMPORTE_AST_31','FTC_IMPORTE_ASD_32','FTC_IINTERES_AST_33','FTC_IINTERES_ASD_34',
'FTC_IINTERES_SAR_35','FTC_IINTERES_RET_36','FTC_IINTERES_CVP_37','FTC_IINTERES_CVT_38',
'FTC_IINTERES_SAR_39','FTC_IINTERES_RET_40','FTC_IINTERES_CVP_41','FTC_IINTERES_CVT_42',
'FTC_IINTERES_AST_43','FTC_IINTERES_ASD_44','FTC_IINTERES_AST_45','FTC_IINTERES_ASD_46',
'FTC_IINTERES_TOT_47','FTN_IMPORTE_RCV_10','FTN_INTERES_RCV_11','FTN_IMPORTE_AVOL_12',
'FTN_INTERES_AVOL_13','FTN_IMPORTE_RET_14','FTN_INTERES_RET_15','FTN_IMPORTE_ALP_16',
'FTN_INTERES_ALP_17','FTN_IMPORTE_SUB_18','FTN_INTERES_SUB_19','FTN_IMPORTE_APP_20',
'FTN_INTERES_APP_21','FTN_IMPORTE_GUB_22','FTN_INTERES_GUB_23','FTN_IMPORTE_VIV_27',
'FTN_INTERES_VIV_28','FTN_IMPORTE_REM_31','FTN_IMPORTE_EXT_33','FTN_INTERES_VIV_35',
'FTN_MONTO_PESOS','FTN_TOTAL_CUENTAS_PAGAR','FTC_IMPORTE_SAR_23','FTC_IMPORTE_CV_25',
'FTC_IMPORTE_CV_26','FTC_INTERES_RET_28','FTC_INTERES_CV_29','FTC_INTERES_CV_30',
'FTC_IMPORTE_AT_31','FTC_IMPORTE_AD_32','FTC_INTERES_AT_33','FTC_INTERES_AD_34']
 
for column in columns_to_format:
    df_mod = df_mod.withColumn(
        f'{column}',
        lpad(format_string('%.2f', col(column).cast('double')), 16, '0')
    )

# COMMAND ----------

columns_to_format = ['FTC_IMPORTE_SAR_30','FTC_IMPORTE_RET_27','FTC_IMPORTE_RET_24','FTC_IMPORTE_SAR_27'
]
 
for column in columns_to_format:
    df_mod = df_mod.withColumn(
        f'{column}',
        lpad(format_string('%.2f', col(column).cast('double')), 13, '0')
    )

# COMMAND ----------

columns_to_format = ['FTN_INTERES_VIV_29','FTN_INTERES_VIV_30','FTN_INTERES_REM_32','FTN_INTERES_EXT_34']
 
for column in columns_to_format:
    df_mod = df_mod.withColumn(
        f'{column}',
        lpad(format_string('%.6f', col(column).cast('double')), 19, '0')
    )

# COMMAND ----------

columns_to_format = ['FTN_ID_ARCHIVO']
 
for column in columns_to_format:
    df_mod = df_mod.withColumn(
        f'{column}',
        lpad(format_string('%.0f', col(column).cast('double')), 10, '0')
    )

# COMMAND ----------

columns_to_format = ['FTN_NUM_AP_28']
 
for column in columns_to_format:
    df_mod = df_mod.withColumn(
        f'{column}',
        lpad(format_string('%.0f', col(column).cast('double')), 9, '0')
    )

# COMMAND ----------

columns_to_format = ['FTC_FECHA_LOTE']
 
for column in columns_to_format:
    df_mod = df_mod.withColumn(
        f'{column}',
        lpad(format_string('%.0f', col(column).cast('double')), 8, '0')
    )

# COMMAND ----------

columns_to_format = ['FTC_CONSECUTIVO']
 
for column in columns_to_format:
    df_mod = df_mod.withColumn(
        f'{column}',
        lpad(format_string('%.0f', col(column).cast('double')), 3, '0')
    )

# COMMAND ----------

DELTA_TABLE_001 = "DELTA_ACLARACIONES_ESPECIALES_ARCH_RESP_" + params.sr_id_archivo
db.write_delta(DELTA_TABLE_001, df_mod, "overwrite")

# COMMAND ----------

if conf.debug:
    display(df_mod)
