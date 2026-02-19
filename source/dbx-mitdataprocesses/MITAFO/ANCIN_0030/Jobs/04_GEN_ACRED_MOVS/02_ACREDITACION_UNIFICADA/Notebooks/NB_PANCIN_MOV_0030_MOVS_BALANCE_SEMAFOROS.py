# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0030_MOVS_BALANCE_SEMAFOROS
# MAGIC
# MAGIC **Descripción:** Exportar semáforos rojos a archivo de texto
# MAGIC
# MAGIC **Subetapa:** Exportar semáforos
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:** TEMP_JOIN_MAXIMOS_{sr_folio}
# MAGIC
# MAGIC **Tablas Output:** SEMAFOROS_ROJOS_{sr_folio}
# MAGIC
# MAGIC **Tablas DELTA:** SEMAFOROS_ROJOS_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:** NA
# MAGIC
# MAGIC **Flow:** Exportar semáforos paralelo
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1359597267196340?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_SEMAFOROS.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

from pyspark.sql.functions import col, lit, when

# COMMAND ----------

# DBTITLE 1,DEFINICIÓN DE PARÁMETROS
# Definir parámetros del notebook
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

# Validar parámetros
params.validate()

# Cargar configuración
conf = ConfManager()
db = DBXConnectionManager()

logger.info("=== INICIANDO: NB_PANCIN_MOV_0030_MOVS_BALANCE_SEMAFOROS ===")
logger.info(f"Parámetros: {params}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reglas TF_150 aplicadas (Salida 2 - LK_TF150_TO_SQ500)
# MAGIC
# MAGIC Basado en el transformer TF_150 del XML `JP_PANCIN_MOV_0030_MOVS_BALANCE.xml`:
# MAGIC
# MAGIC - ### Reglas para el calculo:
# MAGIC   - Las varaibles calculadas ya estan en el set de datos (`TEMP_JOIN_MAXIMOS`)
# MAGIC   - Las transformaciones deben ser aplicadas a todos los registros solo si el parametro SEMROJOS = 1 AND SEMAFORO = 191.
# MAGIC
# MAGIC - ### Con las variables de arriba ahora calcular, "TRANSFORMACIONES":
# MAGIC - Solo a los registros donde SEMROJOS = 1 AND SEMAFORO = 191, agregar al set de datos la columna SEM_ROJOS que tendra el valor de la columna SEMROJOS.
# MAGIC - Escritura final: `write_delta(..., SEMAFOROS_ROJOS_{sr_folio}, overwrite)` como nueva tabla (no listada en ds_catalog).

# COMMAND ----------

# DBTITLE 2,SQ_500 - EXPORTAR SEMÁFOROS ROJOS
# Exportar semáforos rojos a archivo de texto
# Equivalente al stage SQ_500

logger.info("Exportando semáforos rojos")

# filter "SEMROJOS" == 1 & "SEMAFORO" == 191
df_filter = db.read_delta(f"TEMP_JOIN_MAXIMOS_{params.sr_folio}")
df_filter = df_filter.filter((col("SEMROJOS") == lit(1)) & (col("SEMAFORO") == lit(191)))

# NUEVA TABLA: SEMAFOROS_ROJOS no existe en ds_catalog.txt
db.write_delta(f"SEMAFOROS_ROJOS_{params.sr_folio}", df_filter, "overwrite")

if conf.debug:
    display(db.read_delta(f"SEMAFOROS_ROJOS_{params.sr_folio}"))

logger.info(f"Semáforos rojos exportados: SEMAFOROS_ROJOS_{params.sr_folio}")

# COMMAND ----------

# DBTITLE 3,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())

logger.info("=== JOB COMPLETADO: NB_PANCIN_MOV_0030_MOVS_BALANCE_SEMAFOROS ===")
