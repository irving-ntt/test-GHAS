# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_MOV_DEV_PSJL_02_06_VALIDATE_DATASET
# MAGIC
# MAGIC **Descripción:** Valida que el dataset DELTA_600_GEN_MOV del job anterior exista y tenga datos.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Devolución de Pagos SJL
# MAGIC
# MAGIC **Stage Original:** DS_600_GEN_MOV (Dataset Input)
# MAGIC
# MAGIC **Dependencia:**
# MAGIC - Requiere que JP_PATRIF_MOV_DEV_PSJL_01 haya ejecutado
# MAGIC
# MAGIC **Tabla Input (Delta):**
# MAGIC - DELTA_600_GEN_MOV_{sr_folio}
# MAGIC
# MAGIC **Tabla Output:** NA (solo validación)

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams(
    {
        "sr_actualiza": str,
        "sr_etapa": str,
        "sr_fec_acc": str,
        "sr_fec_liq": str,
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_paso": str,
        "sr_proceso": str,
        "sr_reproceso": str,
        "sr_subetapa": str,
        "sr_subproceso": str,
        "sr_usuario": str,
    }
)
params.validate()

# Cargar configuración de entorno
conf = ConfManager()

# Inicializar managers
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Validar que Dataset DELTA_600_GEN_MOV Exista
# Este job depende del dataset creado por JP_PATRIF_MOV_DEV_PSJL_01
dataset_table = f"DELTA_600_GEN_MOV_{params.sr_folio}"

try:
    # Solo validar que la tabla exista (no hacer count)
    db.read_delta(dataset_table).limit(1).collect()

    if conf.debug:
        display(db.read_delta(dataset_table).limit(10))
        db.read_delta(dataset_table).printSchema()

except Exception as e:
    error_msg = f"""
    ERROR: El dataset {dataset_table} NO existe.
    
    Este job requiere que JP_PATRIF_MOV_DEV_PSJL_01 haya ejecutado primero.
    
    Por favor ejecute JP_PATRIF_MOV_DEV_PSJL_01 antes de continuar con este proceso.
    
    Error técnico: {str(e)}
    """
    raise Exception(error_msg)

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
