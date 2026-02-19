# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_MOV_DEV_PSJL_02_07_PROCESS_FINAL
# MAGIC
# MAGIC **Descripción:** Notebook FINAL de procesamiento para JP_PATRIF_MOV_DEV_PSJL_02. Ejecuta todos los joins, lookups, aplica reglas de negocio del transformer e inserta en Oracle.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Devolución de Pagos SJL - PROCESS
# MAGIC
# MAGIC **Trámite:** Devolución de Pagos SJL (DPSJL)
# MAGIC
# MAGIC **Dependencias (6 notebooks previos):**
# MAGIC - JP_PATRIF_MOV_DEV_PSJL_01 (crea DELTA_600_GEN_MOV)
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_01_EXTRACT_VALOR_ACCION
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_02_EXTRACT_TIPO_SUBCTA
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_03_EXTRACT_MOV_SUBCTA
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_04_EXTRACT_MATRIZ_CONV
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_05_EXTRACT_VAL_SALDOS
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_06_VALIDATE_DATASET
# MAGIC
# MAGIC **Tablas Input:** NA (usa tablas TEMP de notebooks previos)
# MAGIC
# MAGIC **Tablas Output (Oracle):**
# MAGIC - CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS (INSERT con DELETE previo)
# MAGIC
# MAGIC **Tablas DELTA (Lectura - 6 tablas):**
# MAGIC - DELTA_600_GEN_MOV_{sr_folio} (del job anterior)
# MAGIC - TEMP_VAL_SALDOS_{sr_folio} (notebook 05)
# MAGIC - TEMP_VALOR_ACCION (notebook 01)
# MAGIC - TEMP_TIPO_SUBCTA (notebook 02)
# MAGIC - TEMP_MOV_SUBCTA_{sr_subproceso} (notebook 03)
# MAGIC - TEMP_MATRIZ_CONV_{sr_folio} (notebook 04)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_006_DELTA_PROCESS.sql
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_007_OCI_DELETE_PRE_MOVS.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Notebook:
# MAGIC 1. **Validar Tablas Temporales**: Verificar que todas las 6 tablas TEMP existan
# MAGIC 2. **Procesamiento Consolidado**: SQL con 6 CTEs (joins, lookups, transformer)
# MAGIC 3. **DELETE Previo**: Eliminar registros anteriores del mismo folio/subproceso
# MAGIC 4. **INSERT a Oracle**: Cargar pre-movimientos con hint APPEND
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332252?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

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
query = QueryManager()
db = DBXConnectionManager()

# Mostrar lista de archivos SQL disponibles
display(query.get_sql_list())

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332260?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_07_PROCESS_FINALimage.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332259?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_07_PROCESS_FINALimage (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332258?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_07_PROCESS_FINALimage (2).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332262?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_07_PROCESS_FINALimage (3).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332261?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_07_PROCESS_FINALimage (4).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332263?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_07_PROCESS_FINALimage (5).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,PASO 1: Procesamiento Completo - Joins, Lookups y Transformer (6 CTEs)
statement_process = query.get_statement(
    "NB_PATRIF_MOV_DEV_PSJL_02_006_DELTA_PROCESS.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    FL_ID_TIPO_MOV=conf.FL_ID_TIPO_MOV,
    CON_MOV_VIV97=conf.CON_MOV_VIV97,
    TABLA_NCI_VIV=conf.TABLA_NCI_VIV,
    TABLA_NCI_RCV=conf.TABLA_NCI_RCV,
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO,
)

# Ejecutar procesamiento consolidado (6 CTEs: joins + lookups + transformer)
df_pre_movimientos = db.sql_delta(statement_process)

if conf.debug:
    display(df_pre_movimientos.limit(20))
    df_pre_movimientos.printSchema()

# COMMAND ----------

# DBTITLE 1,PASO 2: DELETE Previo - Limpiar Registros Anteriores (BeforeSQL)
statement_delete = query.get_statement(
    "NB_PATRIF_MOV_DEV_PSJL_02_007_OCI_DELETE_PRE_MOVS.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

# Ejecutar DELETE
db.execute_oci_dml(statement=statement_delete, async_mode=False)

# COMMAND ----------

# DBTITLE 1,PASO 3: INSERT a Oracle - Cargar Pre-Movimientos
# Insertar registros procesados en tabla de Oracle
logger.info("Insertando datos transformados en OCI")
table_name_target = "CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS"
db.write_data(df_pre_movimientos, table_name_target, "default", "append")

# COMMAND ----------

Notify.send_notification("PATRIF_0800_GMO", params)

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
