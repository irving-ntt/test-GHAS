# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0010_EXT_PREMOVS_CONCEP_MOV
# MAGIC
# MAGIC **Descripción:** Extracción de datos de referencia desde Oracle para premovimientos
# MAGIC
# MAGIC **Subetapa:** Extracción de datos de referencia
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_CONFIG_CONCEP_MOV
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_CONCEPTO_MOV_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0010_EXT_PREMOVS_OCI_003.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 3. **DB_600**: Extracción de concepto movimiento desde Oracle
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316581?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0010_EXT_PREMOVS.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

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

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316584?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0010_EXT_PREMOVS_CONCEP_MOV.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 3,EXTRAER DATOS DE CONCEPTO MOV (DB_600)
# Cargar query para extraer datos de concepto de movimiento desde OCI
statement_003 = query.get_statement(
    "NB_PANCIN_MOV_0010_EXT_PREMOVS_OCI_003.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRN_CONFIG_CONCEP_MOV=conf.TL_CRN_CONFIG_CONCEP_MOV,
)

# Extraer datos de concepto de movimiento y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_CONCEPTO_MOV_{params.sr_folio}", db.read_data("default", statement_003), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_CONCEPTO_MOV_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 5,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())
