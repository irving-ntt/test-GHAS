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

# COMMAND ----------

DELTA_TABLE_001 = "DELTA_VALIDA_CONTENIDO_" + params.sr_id_archivo

task_keys = [
    "TSK_NB_OPER_ANCIN_DOIMSS_01",
    "TSK_NB_OPER_ANCIN_DOIMSS_02",
    "TSK_NB_OPER_ANCIN_DOIMSS_08",
    "TSK_NB_OPER_ANCIN_DOIMSS_09"
]

for task_key in task_keys:
    if dbutils.jobs.taskValues.get(
        taskKey=task_key, 
        key="VAR_VAL_CONT_DOIMSS_" + task_key.split('_')[-1] + "_" + params.sr_id_archivo
    ) == 1:
        raise Exception("Datos incorrectos en validación de contenido")

db.drop_delta(DELTA_TABLE_001)

Notify.send_notification("CARG_ARCH", params)