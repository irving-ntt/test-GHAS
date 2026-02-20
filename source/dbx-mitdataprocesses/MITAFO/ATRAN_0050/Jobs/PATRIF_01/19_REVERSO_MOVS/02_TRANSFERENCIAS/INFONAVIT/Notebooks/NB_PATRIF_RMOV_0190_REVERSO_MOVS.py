# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_RMOV_0190_REVERSO_MOVS
# MAGIC
# MAGIC **Descripción:** Notebook orquestador que determina el flujo de ejecución para el reverso de movimientos según el subproceso recibido.
# MAGIC
# MAGIC **Subetapa:** Reverso de Movimientos
# MAGIC
# MAGIC **Trámite:** Patrimonio - Reverso de Movimientos
# MAGIC
# MAGIC **Tablas Input:** NA (Orquestador)
# MAGIC
# MAGIC **Tablas Output:** NA (Orquestador)
# MAGIC
# MAGIC **Tablas DELTA:** NA (Orquestador)
# MAGIC
# MAGIC **Archivos SQL:** NA
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **NC_010_SUBPROCESOS**: Evaluar parámetro SR_SUBPROCESO para determinar el camino
# MAGIC 2. **Setear variable de salida**: Definir el job hijo que debe ejecutarse
# MAGIC 3. **Opciones de ejecución**:
# MAGIC    - **Subproceso 363**: Ejecutar JQ_PATRIF_RMOV_0100_TRNS_FOV (Transferencias FOVISSSTE)
# MAGIC    - **Subprocesos 364, 365, 368**: Ejecutar JQ_PATRIF_RMOV_0100_TRANSF (Transferencias INFONAVIT)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JQ_PATRIF_RMOV_0190_REVERSO_MOVS_image (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams(
    {
        "sr_paso": str,
        "sr_etapa": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_subetapa": str,
        "sr_id_snapshot": str,
        "sr_folio": str,
        "sr_subproceso": str,
        "sr_fec_liq": str,
        "sr_fec_acc": str,
        "sr_proceso": str,
        "sr_proceso": str,
    }
)
params.validate()

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JQ_PATRIF_RMOV_0190_REVERSO_MOVS_image.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,NC_010_SUBPROCESOS - Determinar camino de ejecución
"""
Lógica de decisión basada en el subproceso:
- Subproceso 363: Transferencias FOVISSSTE
- Subprocesos 364, 365, 368: Transferencias INFONAVIT
"""

# Convertir sr_subproceso a entero para comparación
subproceso = int(params.sr_subproceso)

# Determinar el job hijo a ejecutar
if subproceso == 363:
    job_target = "JQ_PATRIF_RMOV_0100_TRNS_FOV"
    logger.info(f"Subproceso {subproceso}: Se ejecutará Transferencias FOVISSSTE")
elif subproceso in [364, 365, 368]:
    job_target = "JQ_PATRIF_RMOV_0100_TRANSF"
    logger.info(f"Subproceso {subproceso}: Se ejecutará Transferencias INFONAVIT")
else:
    error_msg = (
        f"Subproceso {subproceso} no válido. Valores esperados: 363, 364, 365, 368"
    )
    logger.error(error_msg)
    raise ValueError(error_msg)

logger.info(f"Job target determinado: {job_target}")

# COMMAND ----------

# DBTITLE 1,Setear variable de salida con el job target
"""
Setear la variable dbutils.jobs.taskValues para que el orquestador 
de Databricks Workflows pueda ejecutar el job correspondiente
"""

# Setear el job target como salida del notebook
dbutils.jobs.taskValues.set(key="job_target", value=job_target)
dbutils.jobs.taskValues.set(key="sr_subproceso", value=str(subproceso))

logger.info("Variables adicionales de salida configuradas:")
logger.info(f"  - job_target: {job_target}")

# COMMAND ----------

# DBTITLE 1,Seteo de parametros externos a Job params.
for key, value in params.to_dict().items():
    dbutils.jobs.taskValues.set(key=key, value=value)

# COMMAND ----------

# DBTITLE 1,Logging final
CleanUpManager.cleanup_notebook(locals())
