# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_0800_GMO_CLEANER
# MAGIC
# MAGIC **Propósito:** Limpieza de tablas Delta temporales generadas durante el proceso de generación de movimientos patrimoniales.
# MAGIC
# MAGIC **Descripción:** Este notebook elimina todas las tablas Delta temporales creadas por los notebooks de todos los procesos de generación de movimientos:
# MAGIC
# MAGIC ### 02_TRANSFERENCIAS/INFONAVIT
# MAGIC - NB_PATRIF_MOV_TRANS_INFO_01
# MAGIC - NB_PATRIF_MOV_TRANS_INFO_02
# MAGIC - NB_PATRIF_MOV_TRANS_TA_01
# MAGIC
# MAGIC ### 02_TRANSFERENCIAS/FOVISSSTE
# MAGIC - NB_PATRIF_0800_GMO_TRANS_FOV_01
# MAGIC
# MAGIC ### 03_DEV_SALDO_EXCED/INFONAVIT
# MAGIC - NB_PATRIF_GMO_0100_DEV_SALD_EXC
# MAGIC
# MAGIC ### 03_DEV_SALDO_EXCED/FOVISSSTE
# MAGIC - NB_PATRIF_GMO_0100_DEV_FOV
# MAGIC
# MAGIC ### 05_TRP/INFONAV_FOVISSS
# MAGIC - NB_PATRIF_MOV_TRANS_RP_01
# MAGIC
# MAGIC ### 06_DPSJL
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_01
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02 (7 notebooks modulares)
# MAGIC
# MAGIC **Total de tablas:** 29 tablas Delta temporales
# MAGIC
# MAGIC **Ejecución:** Debe ejecutarse al final del proceso para liberar espacio en el data lake.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cargar framework
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parametros originales
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
conf = ConfManager()

# COMMAND ----------

# DBTITLE 1,Objeto para manejo del datalake
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Borrado de deltas
# Definimos la lista de tablas con sufijo dinámico organizadas por proceso

# ==================================================================================
# 02_TRANSFERENCIAS/INFONAVIT
# ==================================================================================
# Tablas generadas por NB_PATRIF_MOV_TRANS_INFO_01.py
tables_trans_info_01 = [
    f"DELTA_500_SALDO_{params.sr_folio}",
]

# Tablas generadas por NB_PATRIF_MOV_TRANS_INFO_02.py
tables_trans_info_02 = [
    f"NB_PATRIF_MOV_TRANS_INFO_02_001_{params.sr_folio}",
    f"TEMP_VALOR_ACCION_{params.sr_folio}",
    f"NB_PATRIF_MOV_TRANS_INFO_02_002_{params.sr_folio}",
    f"TEMP_TIPO_SUBCTA_{params.sr_folio}",
    f"NB_PATRIF_MOV_TRANS_INFO_02_003_{params.sr_folio}",
    f"TEMP_CONCEPTO_MOV_{params.sr_folio}",
    f"NB_PATRIF_MOV_TRANS_INFO_02_004_{params.sr_folio}",
    f"TEMP_MATRIZ_CONV_{params.sr_folio}",
    f"NB_PATRIF_MOV_TRANS_INFO_02_005_{params.sr_folio}",
]

# Tablas generadas por NB_PATRIF_MOV_TRANS_TA_01.py
tables_trans_ta_01 = [
    f"DELTA_TMP_MOV_TRANS_SALDOS_{params.sr_folio}",
    f"DELTA_TMP_MOV_TRANS_CON_INTERESES_{params.sr_folio}",
]

# ==================================================================================
# 02_TRANSFERENCIAS/FOVISSSTE
# ==================================================================================
# Tablas generadas por NB_PATRIF_0800_GMO_TRANS_FOV_01.py
tables_trans_fov_01 = [
    f"DELTA_500_SALDO_{params.sr_folio}",
]

# ==================================================================================
# 03_DEV_SALDO_EXCED/INFONAVIT
# ==================================================================================
# Tablas generadas por NB_PATRIF_GMO_0100_DEV_SALD_EXC.py
tables_dev_sald_exc = [
    f"TEMP_DEV_SALDOS_{params.sr_folio}",
    f"TEMP_VALOR_ACCION_{params.sr_folio}",
    f"DELTA_500_SALDO_{params.sr_folio}",
]

# ==================================================================================
# 03_DEV_SALDO_EXCED/FOVISSSTE
# ==================================================================================
# Tablas generadas por NB_PATRIF_GMO_0100_DEV_FOV.py
tables_dev_fov = [
    f"TEMP_DEV_SALDOS_FOV_{params.sr_folio}",
    f"TEMP_VALOR_ACCION_{params.sr_folio}",
    f"DELTA_500_SALDO_{params.sr_folio}",
]

# ==================================================================================
# 05_TRP/INFONAV_FOVISSS
# ==================================================================================
# Tablas generadas por NB_PATRIF_MOV_TRANS_RP_01.py
tables_trp_01 = [
    f"DELTA_500_SALDO_{params.sr_folio}",
]

# ==================================================================================
# 06_DPSJL
# ==================================================================================
# Tablas generadas por NB_PATRIF_MOV_DEV_PSJL_01.py
tables_dpsjl_01 = [
    f"TEMP_DEV_PAG_SJL_{params.sr_folio}",
    f"DELTA_600_GEN_MOV_{params.sr_folio}",
]

# Tablas generadas por NB_PATRIF_MOV_DEV_PSJL_02 (7 notebooks)
tables_dpsjl_02 = [
    "TEMP_VALOR_ACCION",
    "TEMP_TIPO_SUBCTA",
    f"TEMP_MOV_SUBCTA_{params.sr_subproceso}",
    f"TEMP_MATRIZ_CONV_{params.sr_folio}",
    f"TEMP_VAL_SALDOS_{params.sr_folio}",
]

# ==================================================================================
# CONSOLIDAR TODAS LAS TABLAS
# ==================================================================================
all_tables = (
    tables_trans_info_01
    + tables_trans_info_02
    + tables_trans_ta_01
    + tables_trans_fov_01
    + tables_dev_sald_exc
    + tables_dev_fov
    + tables_trp_01
    + tables_dpsjl_01
    + tables_dpsjl_02
)

# Recorremos la lista y aplicamos drop
for table in all_tables:
    db.drop_delta(table)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
