# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_id_archivo": str,
    "sr_path_arch": str,
    "sr_origen_arc": str,
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

DELTA_TABLE_001 = "DELTA_VALIDA_CONTENIDO_" + params.sr_id_archivo

# COMMAND ----------

# DBTITLE 1,VALIDACION 01
#Ejecuta consulta para el 01
statement_001 = query.get_statement(
    "VAL_CONT_TRANS_INFO_AC_01_001.sql",
    DELTA_TABLE_NAME_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_001}"
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

# COMMAND ----------

import os
#columns_all_null = [col for col in df.columns if df.filter(df[col].isNotNull() & (col != 'FTN_NO_LINEA')).count() == 0]
columns_all_null = [col for col in df.columns if df.filter(df[col].isNotNull()).count() == 0]

# Drop columns where all values are null
df_cleaned = df.drop(*columns_all_null)

# Removing rows where all values are null
df_cleaned = df_cleaned.dropna(how='all', subset=[col for col in df_cleaned.columns if col != 'FTN_NO_LINEA'])

ETAPA='01'
statement_001=''
DELTA_TABLE_CLEANED_001 = "DELTA_VALIDA_CONTENIDO_CIFRAS_CONTROL_ERR_" + ETAPA + "_" + params.sr_id_archivo

#len(df_cleaned.columns) depende de los valores fijos como FTN_NO_LINEA, ID_ARCHIVO
if len(df_cleaned.take(1)) == 1 and len(df_cleaned.columns) > 1:
    full_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + SETTINGS.GENERAL.PATH_RCDI + '/' + SETTINGS.GENERAL.PATH_INF + '/' + os.path.basename(params.sr_path_arch) + f'_{ETAPA}_INCIDENCIAS.csv'

    db.write_delta(DELTA_TABLE_CLEANED_001, df_cleaned, "overwrite")

    statement_001 = query.get_statement(
        "VAL_CONT_GEN_CIFRAS_CONTROL_SI_ERR.sql",
        SR_SUBETAPA=params.sr_subetapa,
        FILTRO_ETAPA=ETAPA,
        USR=SETTINGS.GENERAL.PROCESS_USER,
        SR_ID_ARCHIVO=params.sr_id_archivo,
        DELTA_TABLE_NAME_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_001}",
        DELTA_TABLE_NAME_CLEANED_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_CLEANED_001}"
        )

    # Genera el archivo y decide si calcular MD5 o no
    file_manager.generar_archivo_ctindi(
        df_final=df_cleaned,  # DataFrame que se va a guardar
        full_file_name=full_path,
        header=True,
        calcular_md5=False  # Cambia a False si no quieres calcular el MD5
    )
else:
    statement_001 = query.get_statement(
        "VAL_CONT_GEN_CIFRAS_CONTROL_NO_ERR.sql",
        SR_SUBETAPA=params.sr_subetapa,
        FILTRO_ETAPA=ETAPA,
        USR=SETTINGS.GENERAL.PROCESS_USER,
        SR_ID_ARCHIVO=params.sr_id_archivo,
        DELTA_TABLE_NAME_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_001}"
        )

DELTA_TABLE_NAME_002 = "DELTA_VALIDA_CONTENIDO_CIFRAS_CONTROL_" + params.sr_id_archivo

db.write_delta(DELTA_TABLE_NAME_002, db.sql_delta(statement_001), "append")

db.drop_delta(DELTA_TABLE_CLEANED_001)
