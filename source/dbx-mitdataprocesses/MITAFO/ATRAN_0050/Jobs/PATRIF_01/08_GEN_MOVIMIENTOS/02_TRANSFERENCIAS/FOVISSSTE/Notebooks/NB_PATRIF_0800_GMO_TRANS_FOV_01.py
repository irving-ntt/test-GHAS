# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

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

params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columna 'Archivo SQL' contenga el nombre del notebook
display(queries_df.filter(col("Archivo SQL").contains("GMO_TRANS_FOV_01")))

# COMMAND ----------

# MAGIC %md
# MAGIC <ol>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316518?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/FOVISSSTE/Notebooks/assets/NB_PATRIF_0800_GMO_TRANS_FOV_01.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316516?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/FOVISSSTE/Notebooks/assets/NB_PATRIF_0800_GMO_TRANS_FOV_01_A.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316517?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/FOVISSSTE/Notebooks/assets/NB_PATRIF_0800_GMO_TRANS_FOV_01_B.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,Proceso unificado: Filtros 08, 92, UNION y manejo de nulos
delta_table = f"{conf.delta_700_saldos_proc}_{params.sr_folio}"
logger.info(f"Procesando query unificado desde tabla Delta: {delta_table}")

# Query unificado que combina todos los pasos
statement_unified = query.get_statement(
    "NB_PATRIF_0800_GMO_TRANS_FOV_01_UNIFIED.sql",
    DELTA_TABLE=delta_table,
    SR_FOLIO=params.sr_folio,
    TIPO_SUBCTA_VIV08_FOV=conf.TIPO_SUBCTA_VIV08_FOV,
    TIPO_SUBCTA_VIV92_FOV=conf.TIPO_SUBCTA_VIV92_FOV,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# Ejecutar query unificado y guardar resultado final
db.write_delta(
    f"DELTA_500_SALDO_{params.sr_folio}",
    db.sql_delta(statement_unified),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"DELTA_500_SALDO_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Resumen del proceso optimizado
CleanUpManager.cleanup_notebook(locals())
