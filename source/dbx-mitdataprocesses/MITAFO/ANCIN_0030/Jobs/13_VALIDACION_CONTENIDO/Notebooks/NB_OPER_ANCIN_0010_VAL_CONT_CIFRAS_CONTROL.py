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
    "sr_paso" :str
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
subprocesos_rctras = {'354', '364', '365', '368', '348', '349', '363', '3286', '350', '347', '3283', '3832','3356'}
subprocesos_intgrty = {'354', '364', '365', '368', '348', '349', '363', '3286', '350', '347', '3283', '3832'}

id_proceso = params.sr_id_archivo if params.sr_id_archivo != "1" else params.sr_folio

if params.sr_subproceso in subprocesos_rctras and params.sr_subetapa in ("23", "3356"):
    PATH_OUTPUT = SETTINGS.GENERAL.PATH_RCTRAS
elif params.sr_subproceso in subprocesos_intgrty and params.sr_subetapa in ("25", "4030"):
    PATH_OUTPUT = SETTINGS.GENERAL.PATH_INTEGRITY
else:
    PATH_OUTPUT = SETTINGS.GENERAL.PATH_RCDI

full_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/' + SETTINGS.GENERAL.PATH_INF

full_path_output = SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/' + SETTINGS.GENERAL.PATH_INF

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
        full_path_output + '/' + id_proceso + '_Carga_Archivo.csv',
        'IDENTIFICADOR: ' + id_proceso + '\n' + '\n' + content,
        overwrite=True
    )
    if params.sr_subproceso in subprocesos_intgrty and params.sr_subetapa in ["25", "4030"]:
        logger.info("Mueve a Error: " + SETTINGS.GENERAL.EXTERNAL_LOCATION + params.sr_path_arch + " -> " + SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/Error')
        dbutils.fs.mkdirs(SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/Error/')
        dbutils.fs.mv(SETTINGS.GENERAL.EXTERNAL_LOCATION + params.sr_path_arch, SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/Error/')
else:
    if params.sr_subproceso in subprocesos_intgrty and params.sr_subetapa in ["25", "4030"]:
        logger.info("Mueve a OK: " + SETTINGS.GENERAL.EXTERNAL_LOCATION + params.sr_path_arch + " -> " + SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/Ok')
        dbutils.fs.mkdirs(SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/OK/')
        dbutils.fs.mv(SETTINGS.GENERAL.EXTERNAL_LOCATION + params.sr_path_arch, SETTINGS.GENERAL.EXTERNAL_LOCATION + PATH_OUTPUT + '/OK/')

# COMMAND ----------

DELTA_TABLE_NAME_001 = "DELTA_VALIDA_CONTENIDO_" + id_proceso

DELTA_TABLE_NAME_CIFRAS_CONTROL_001 = "DELTA_VALIDA_CONTENIDO_CIFRAS_CONTROL_" + id_proceso

statement_001 = query.get_statement(
        "VAL_CONT_GEN_CIFRAS_CONTROL.sql",
        DELTA_TABLE_NAME_CIFRAS_CONTROL_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_NAME_CIFRAS_CONTROL_001}"
        )


# COMMAND ----------

if params.sr_id_archivo == "1":
    db.execute_oci_dml(statement="DELETE FROM CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL where FTC_FOLIO = '" + params.sr_folio + "'", async_mode=False)
else:
    db.execute_oci_dml(statement="DELETE FROM CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL where FTN_ID_ARCHIVO = " + params.sr_id_archivo, async_mode=False)

# COMMAND ----------

db.write_data(db.sql_delta(statement_001), "CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL", "default", "append")

# COMMAND ----------

#Cuando este el endpoint
if params.sr_subproceso in subprocesos_rctras and params.sr_subetapa in ["25", "4030"]:
    Notify.send_notification("VAL_CONT_TRAS", params)
else:
    Notify.send_notification("VAL_CONT", params)
#Notify.send_notification("VAL_CONT", params)

# COMMAND ----------

#Escribe delta
db.drop_delta(DELTA_TABLE_NAME_CIFRAS_CONTROL_001)

db.drop_delta(DELTA_TABLE_NAME_001)
