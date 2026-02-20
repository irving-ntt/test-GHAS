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
    "sr_folio" :str,
    "sr_instancia_proceso" :str,
    "sr_usuario" :str,
    "sr_etapa" :str,
    "sr_id_snapshot" :str,
    "sr_paso" :str,
    "sr_id_archivo_siguiente": str,
    "sr_fec_arc": str
})
# Validar widgets
#params.validate()

# COMMAND ----------

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

# COMMAND ----------

import os
import fnmatch
import logging

#Validar si va en RCDI o en TRAS
subprocesos_rctras = {'364', '368', '3286', '363', '354', '365'}
subprocesos_rcdi = {'3832'}

id_proceso = params.sr_id_archivo if params.sr_id_archivo != "1" else params.sr_folio

if params.sr_subproceso in subprocesos_rctras:
    PATH_OUTPUT = SETTINGS.GENERAL.PATH_RCTRAS
else:
    PATH_OUTPUT = SETTINGS.GENERAL.PATH_RCDI

full_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/' + SETTINGS.GENERAL.PATH_INF + '/'

full_path_output = SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/' + SETTINGS.GENERAL.PATH_INF + '/'

full_name = os.path.basename(params.sr_path_arch)

files = dbutils.fs.ls(full_path)

content = ''
arch_error = 0

for i in files:
    if fnmatch.fnmatch(i.path, '*' + full_name + '_*'):
        logger.info("Archivo a procesar: " + i.path)
        etapa = i.path[-18:-16]
        content += "DETALLE: "+ etapa + '\n' + dbutils.fs.head(i.path, 10048576) + '\n'
        dbutils.fs.rm(i.path)
        arch_error = 1

if arch_error == 1:
    dbutils.fs.put(
        full_path_output + '/' + id_proceso + '_Validacion_Respuesta.csv',
        'IDENTIFICADOR: ' + id_proceso + '\n' + '\n' + content,
        overwrite=True
    )
    logger.info('Archivo creado: ' + full_path_output + '/' + id_proceso + '_Validacion_Respuesta.csv')


# COMMAND ----------

DELTA_TABLE_NAME_001 = "DELTA_VALIDA_CONTENIDO_" + id_proceso

DELTA_TABLE_NAME_CIFRAS_CONTROL_001 = "DELTA_VALIDA_CONTENIDO_CIFRAS_CONTROL_" + id_proceso

statement_001 = query.get_statement(
        "VAL_CONT_GEN_CIFRAS_CONTROL.sql",
        DELTA_TABLE_NAME_CIFRAS_CONTROL_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_NAME_CIFRAS_CONTROL_001}"
        )


# COMMAND ----------

if params.sr_id_archivo != "1":
    db.execute_oci_dml(statement="DELETE FROM CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL where FTN_ID_ARCHIVO = " + params.sr_id_archivo, async_mode=False)
else:
    db.execute_oci_dml(statement="DELETE FROM CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL where FTC_FOLIO = '" + params.sr_folio + "'", async_mode=False)

# COMMAND ----------

db.write_data(db.sql_delta(statement_001), "CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL", "default", "append")

# COMMAND ----------

Notify.send_notification("VAL_CONT", params)

# COMMAND ----------

#Escribe delta
db.drop_delta(DELTA_TABLE_NAME_CIFRAS_CONTROL_001)

db.drop_delta(DELTA_TABLE_NAME_001)
