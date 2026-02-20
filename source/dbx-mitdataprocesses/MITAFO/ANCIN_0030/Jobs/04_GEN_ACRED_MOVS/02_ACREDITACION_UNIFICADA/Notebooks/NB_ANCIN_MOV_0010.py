# Databricks notebook source
# MAGIC %md
# MAGIC # NB_ANCIN_MOV_0010
# MAGIC
# MAGIC **Descripción:** Control de flujo para procesamiento de movimientos IMSS
# MAGIC
# MAGIC **Subetapa:** Control de flujo
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:** NA
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:** NA
# MAGIC
# MAGIC **Archivos SQL:** NA
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **NC_100**: Punto de decisión basado en SR_ETAPA
# MAGIC 2. **Flujo Normal**: SR_ETAPA = 1
# MAGIC 3. **Flujo Rechazo**: SR_ETAPA = 2
# MAGIC 4. **Flujo Recalculo**: SR_ETAPA = 3

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
# Solo incluir parámetros que realmente se usan + obligatorios del framework
params = WidgetParams(
    {
        "sr_paso": str,
        "sr_etapa": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_id_snapshot": str,
        "sr_folio": str,
        "sr_fec_liq": (str, "YYYYMMDD"),
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_fec_acc": (str, "YYYYMMDD"),
        "sr_tipo_mov": str,
        "sr_subetapa": str,
        #"sr_id_archivo": str,
        "sr_reproceso": str,
        "sr_etapa_bit": str,
    }
)
params.validate()

# COMMAND ----------

# MAGIC %md
# MAGIC <ol>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316541?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_ANCIN_MOV_0010.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316540?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_ANCIN_MOV_0010_A.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC </ol>
# MAGIC
# MAGIC # Variable Clave: SR_ETAPA
# MAGIC
# MAGIC - SR_ETAPA = 1: Flujo Normal (Nuevo procesamiento)
# MAGIC - SR_ETAPA = 2: Flujo de Rechazo (Eliminación de movimientos problemáticos)
# MAGIC - SR_ETAPA = 3: Flujo de Recalculo (Combinación de normal + rechazo)
# MAGIC
# MAGIC # Stage NC_100 - Primer Punto de Decisión:
# MAGIC ### `Condición: p_SR_ETAPA = 2 OR p_SR_ETAPA = 3`
# MAGIC
# MAGIC - SI: Ir a (Flujo de Rechazo/Recalculo)
# MAGIC - NO: Ir a (Flujo Normal)
# MAGIC

# COMMAND ----------

stage = "FLUJO_NORMAL"

if params.sr_etapa == "3":
    stage = "FLUJO_RECALCULO"
elif params.sr_etapa == "2":
    stage = "FLUJO_RECHAZO"
elif params.sr_etapa == "1":
    stage = "FLUJO_NORMAL"
else:
    raise Exception(f"Etapa {params.sr_etapa} no contemplada")

# Asignar valor al stage para los siguientes notebooks del workflow
dbutils.jobs.taskValues.set(key="stage", value=stage)

# Log
logger.info(f"Stage NC_100: {stage}")

# COMMAND ----------

for key, value in params.to_dict().items():
    dbutils.jobs.taskValues.set(key=key, value=value)

# COMMAND ----------

# # Normalizar el valor a mayúsculas desde el parámetro
# sr_tipo_mov = params.sr_tipo_mov.upper()

# # Evaluar y reasignar el valor según las reglas de negocio
# if sr_tipo_mov == "ABONO" or sr_tipo_mov == "2":
#     sr_tipo_mov = "2"
#     logger.info(f"El valor de sr_tipo_mov ha sido cambiado a {sr_tipo_mov}")
# else:
#     sr_tipo_mov = "1"
#     logger.info(f"El valor de sr_tipo_mov ha sido cambiado a {sr_tipo_mov}")

# # Guardar el valor final en el contexto del job con key en minúsculas
# dbutils.jobs.taskValues.set(key="sr_tipo_mov", value=sr_tipo_mov)
