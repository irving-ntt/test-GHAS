# Databricks notebook source
'''
Descripción:
    Realiza la validación de contenido del archivo de FOVISSTE Transferencia de acreditados
Subetapa:
    Validación de contenido
Trámite:
    FOVISSTE Transferencia de acreditados
Tablas Input:
    N/A
Tablas Output:
    N/A
Tablas Delta:
    DELTA_VALIDA_CONTENIDO_#sr_id_archivo#
Archivos SQL:
    "VAL_CONT_FOVISSTE_TRANS_ACRE_02_001.sql"
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
    "sr_id_archivo": str,
    "sr_path_arch": str,
    "sr_origen_arc": str,
    #"sr_folio" :str,
    #"sr_instancia_proceso" :str,
    #"sr_usuario" :str,
    #"sr_etapa" :str,
    #"sr_id_snapshot" :str,
    #"sr_paso" :str,
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

id_proceso = params.sr_id_archivo if params.sr_id_archivo != "1" else params.sr_folio
DELTA_TABLE_001 = "DELTA_VALIDA_CONTENIDO_" + id_proceso

# COMMAND ----------

# DBTITLE 1,VALIDACION 01
#Ejecuta consulta para el 01
statement_001 = query.get_statement(
    "VAL_CONT_FOVISSTE_TRANS_ACRE_02_001_RECHAZO.sql",
    DELTA_TABLE_NAME_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_001}"
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

# COMMAND ----------

#import random
#
##columns_all_null = [col for col in df.columns if df.filter(df[col].isNotNull() & (col != 'FTN_NO_LINEA')).count() == 0]
#columns_all_null = [col for col in df.columns if df.filter(df[col].isNotNull()).count() == 0]
#
## Drop columns where all values are null
#df_cleaned = df.drop(*columns_all_null)
#
## Removing rows where all values are null
#df_cleaned = df_cleaned.dropna(how='all', subset=[col for col in df_cleaned.columns if col != 'FTN_NO_LINEA'])
#
#dbutils.jobs.taskValues.set(key="VAR_VAL_CONT_DOIMSS_01_" + params.sr_id_archivo, value='0')
#
##len(df_cleaned.columns) depende de los valores fijos como FTN_NO_LINEA, ID_ARCHIVO
#if len(df_cleaned.take(1)) == 1 and len(df_cleaned.columns) > 1:
#    full_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + params.sr_path_arch + '_01_INCIDENCIAS.csv'
#    tmp = random.randint(10000, 100000)
#
#    (df_cleaned.coalesce(1)
#        .write
#        .mode("overwrite")
#        .format("csv")
#        .option("header", 'true')
#        .option("lineSep","\r\n")
#        .save(full_path + '_' + str(tmp)))
#
#    list_of_files = dbutils.fs.ls(full_path + '_' + str(tmp))
#    for item in list_of_files:
#        if item.name.startswith("part"):
#            csv_part_file = item.path
#
#    dbutils.fs.mv(csv_part_file, full_path, True)
#    dbutils.fs.rm(full_path + '_' + str(tmp), True)
#    
#    dbutils.jobs.taskValues.set(key="VAR_VAL_CONT_DOIMSS_01_" + params.sr_id_archivo, value='1')

# COMMAND ----------

import os
#columns_all_null = [col for col in df.columns if df.filter(df[col].isNotNull() & (col != 'FTN_NO_LINEA')).count() == 0]
columns_all_null = [col for col in df.columns if df.filter(df[col].isNotNull()).count() == 0]

# Drop columns where all values are null
df_cleaned = df.drop(*columns_all_null)

# Removing rows where all values are null
df_cleaned = df_cleaned.dropna(how='all', subset=[col for col in df_cleaned.columns if col != 'FTN_NO_LINEA'])

ETAPA='02'
statement_001=''
DELTA_TABLE_CLEANED_001 = "DELTA_VALIDA_CONTENIDO_CIFRAS_CONTROL_ERR_" + ETAPA + "_" + id_proceso

subprocesos_rctras = {'354', '364', '365', '368', '348', '349', '363', '3286', '350', '347', '3283', '3832', '3356'}
subprocesos_intgrty = {'354', '364', '365', '368', '348', '349', '363', '3286', '350', '347', '3283', '3832'}

id_proceso = params.sr_id_archivo if params.sr_id_archivo != "1" else params.sr_folio

if params.sr_subproceso in subprocesos_rctras and params.sr_subetapa in ("23","363","3356","832"):
    PATH_OUTPUT = SETTINGS.GENERAL.PATH_RCTRAS
elif params.sr_subproceso in subprocesos_intgrty and params.sr_subetapa == "25":
    PATH_OUTPUT = SETTINGS.GENERAL.PATH_INTEGRITY
else:
    PATH_OUTPUT = SETTINGS.GENERAL.PATH_RCDI

#len(df_cleaned.columns) depende de los valores fijos como FTN_NO_LINEA, ID_ARCHIVO
if len(df_cleaned.take(1)) == 1 and len(df_cleaned.columns) > 1:
    full_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/' + SETTINGS.GENERAL.PATH_INF + '/' + os.path.basename(params.sr_path_arch) + f'_{ETAPA}_INCIDENCIAS.csv'

    db.write_delta(DELTA_TABLE_CLEANED_001, df_cleaned, "overwrite")

    statement_001 = query.get_statement(
        "VAL_CONT_GEN_CIFRAS_CONTROL_SI_ERR.sql",
        SR_SUBETAPA=params.sr_subetapa,
        FILTRO_ETAPA=ETAPA,
        USR=SETTINGS.GENERAL.PROCESS_USER,
        SR_ID_ARCHIVO=params.sr_id_archivo,
        #SR_FOLIO=params.sr_folio,
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
        #SR_FOLIO=params.sr_folio,
        DELTA_TABLE_NAME_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_001}"
        )

DELTA_TABLE_NAME_002 = "DELTA_VALIDA_CONTENIDO_CIFRAS_CONTROL_" + id_proceso

db.write_delta(DELTA_TABLE_NAME_002, db.sql_delta(statement_001), "append")

db.drop_delta(DELTA_TABLE_CLEANED_001)


# COMMAND ----------


