# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0040_DEL_MOVS
# MAGIC
# MAGIC **Descripción:** Eliminar registros insertados en TTSISGRAL_ETL_MOVIMIENTOS en la etapa de recálculo
# MAGIC
# MAGIC **Subetapa:** Eliminación de movimientos
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:** NA
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:** NA
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0040_DEL_MOVS_001_DELETE.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100**: Ejecutar DELETE condicional en Oracle (TTSISGRAL_ETL_MOVIMIENTOS)
# MAGIC 2. **PK_200**: Monitoreo y verificación

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

# Validar parámetros
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

logger.info("=== INICIANDO: NB_PANCIN_MOV_0040_DEL_MOVS ===")
logger.info(f"Parámetros: {params}")

# COMMAND ----------

# DBTITLE 1,ELIMINACIÓN CONDICIONAL DE MOVIMIENTOS
# Cargar query para eliminar registros de movimientos
# NOTA: Los parámetros constantes (CX_CRE_ESQUEMA, TL_CRE_MOVIMIENTOS)
# han sido extraídos del logs.txt del job JP_PANCIN_MOV_0040_DEL_MOVS
statement_delete = query.get_statement(
    "NB_PANCIN_MOV_0040_DEL_MOVS_001_DELETE.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,  # Valor del logs.txt: CIERREN_ETL
    TL_CRE_MOVIMIENTOS=conf.TL_CRE_MOVIMIENTOS,  # Valor del logs.txt: TTSISGRAL_ETL_MOVIMIENTOS
    SR_FOLIO=params.sr_folio,  # Valor variable del logs.txt: 202507240929318226, etc.
)

# Ejecutar DELETE condicional en Oracle
# NOTA: Se usa db.execute_oci_dml() porque es una operación DELETE (DML)
execution = db.execute_oci_dml(statement=statement_delete, async_mode=True)

# COMMAND ----------

# DBTITLE 3,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())
logger.info("=== JOB COMPLETADO: NB_PANCIN_MOV_0040_DEL_MOVS ===")
