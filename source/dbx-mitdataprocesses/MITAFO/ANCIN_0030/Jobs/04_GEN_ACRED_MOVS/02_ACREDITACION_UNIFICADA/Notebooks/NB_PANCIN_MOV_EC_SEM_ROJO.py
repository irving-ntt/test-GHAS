# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_EC_SEM_ROJO_2
# MAGIC
# MAGIC **Descripción:** Revalidación de semáforos rojos posterior a cifras de control (equivalente a EC_SEM_ROJO_2 en DataStage). Sustituye lectura de archivo por verificación directa en Delta.
# MAGIC
# MAGIC - Entrada: Delta `SEMAFOROS_ROJOS_#SR_FOLIO#` (Unity Catalog)
# MAGIC - Salida: Bandera de Jobs `has_red` con valores "1" (hay rojos) o "0" (sin rojos)
# MAGIC - Reglas:
# MAGIC   - No mezclar OCI con Delta en una misma operación
# MAGIC   - Usar `logger.info()` en lugar de `print()`
# MAGIC   - No SQL embebido; no aplica QueryManager aquí
# MAGIC   - Parametrización mediante `WidgetParams`
# MAGIC
# MAGIC ## Decisión
# MAGIC - `has_red = 1` → Hay semáforos rojos: no ejecutar `NB_PANCIN_MOV_0035_BALANCE`
# MAGIC - `has_red = 0` → No hay semáforos rojos: ejecutar `NB_PANCIN_MOV_0035_BALANCE`

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

from pyspark.sql import SparkSession

# Parámetros / Widgets
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

# Framework
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

# Revalidación de semáforos rojos leyendo Delta (sin SQL)
has_red_value = "0"
count_rows = 0

try:
    df_sem = db.read_delta(f"SEMAFOROS_ROJOS_{params.sr_folio}")
    # Verificación ligera para no escanear toda la tabla
    count_rows = df_sem.limit(1).count()
    has_red_value = "1" if count_rows > 0 else "0"
except Exception as e:
    # Si no existe la Delta, se asume que no hay semáforos rojos
    logger.info(
        f"EC_SEM_ROJO_2: Delta SEMAFOROS_ROJOS_{params.sr_folio} no encontrada. Se asume 0 semáforos rojos."
    )
    has_red_value = "0"

logger.info(
    f"EC_SEM_ROJO_2: registros encontrados={count_rows}, has_red={has_red_value}"
)

# Publicar bandera para el workflow
try:
    dbutils.jobs.taskValues.set(key="has_red", value=has_red_value)
    logger.info(f"EC_SEM_ROJO_2: bandera de Jobs 'has_red' seteada a {has_red_value}")
except Exception as e:
    logger.info(
        "EC_SEM_ROJO_2: no fue posible setear taskValues (ejecución local/interactive)"
    )

# Finalizar notebook con estatus legible para condiciones adicionales
status = "HAS_RED" if has_red_value == "1" else "NO_RED"
logger.info(f"EC_SEM_ROJO_2: status de salida = {status}")
#dbutils.notebook.exit(status)
