# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0020_SUF_SALDO
# MAGIC
# MAGIC **Descripción:** Preparación de información para cálculo de suficiencia de saldos
# MAGIC
# MAGIC **Subetapa:** Preparación de datos para suficiencia
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - CX_CRE_ESQUEMA.TL_CRE_PRE_MOVIMIENTOS
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_BALANCE_MOVS
# MAGIC - RESULTADO_SUF_SALDO_{sr_folio} (Delta)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_BALANCE_MOVS_{sr_folio}
# MAGIC - RESULTADO_SUF_SALDO_FINAL_0020_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0020_SUF_SALDO_001.sql
# MAGIC - NB_PANCIN_MOV_0020_SUF_SALDO_002.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DS_100**: Lee datos de dataset (sufijo 06)
# MAGIC 2. **DB_200_TTAFOGRAL_BALANCE_MOVS**: Extrae datos de Oracle con consulta compleja
# MAGIC 3. **JO_110**: Inner Join entre DS_100 y DB_200
# MAGIC 4. **DS_120**: Guarda resultado en dataset (sufijo 03)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332285?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0020_SUF_SALDO_image.png" style="max-width: 100%; height: auto;"/>
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
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332283?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0020_SUF_SALDO_image (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 2,DB_200_TTAFOGRAL_BALANCE_MOVS - EXTRACCIÓN DE ORACLE
# Extraer datos de Oracle con consulta compleja
# Equivalente al stage DB_200_TTAFOGRAL_BALANCE_MOVS

logger.info("Ejecutando consulta Oracle para balance de movimientos")

# Cargar query para extraer datos de balance de movimientos desde Oracle
statement_001 = query.get_statement(
    "NB_PANCIN_MOV_0020_SUF_SALDO_001.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRE_PRE_MOVIMIENTOS=conf.TL_CRE_PRE_MOVIMIENTOS,
    TL_CRN_BALANCE_MOVS=conf.TL_CRN_BALANCE_MOVS,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 3,GUARDAR DATOS ORACLE EN TABLA DELTA TEMPORAL
# Leer datos de Oracle y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_BALANCE_MOVS_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite",
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_BALANCE_MOVS_{params.sr_folio}"))
    logger.info(
        f"Registros extraídos de Oracle: {db.read_delta(f'TEMP_BALANCE_MOVS_{params.sr_folio}').count()}"
    )

logger.info("Datos de balance extraídos correctamente de Oracle")

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332283?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0020_SUF_SALDO_image (2).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 4,JO_110 - INNER JOIN ENTRE DATASET Y ORACLE
# Realizar Inner Join entre los datos del dataset y los datos de Oracle
# Equivalente al stage JO_110

logger.info("Realizando Inner Join entre dataset y datos de Oracle")

# Cargar query para realizar join entre tabla Delta existente y tabla temporal Oracle
statement_002 = query.get_statement(
    "NB_PANCIN_MOV_0020_SUF_SALDO_002.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 5,GUARDAR RESULTADO FINAL
# Procesar datos y guardar resultado final en tabla Delta
db.write_delta(
    f"RESULTADO_SUF_SALDO_FINAL_0020_{params.sr_folio}",
    db.sql_delta(statement_002),
    "overwrite",
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"RESULTADO_SUF_SALDO_FINAL_0020_{params.sr_folio}"))
    logger.info(
        f"Registros procesados: {db.read_delta(f'RESULTADO_SUF_SALDO_FINAL_0020_{params.sr_folio}').count()}"
    )

logger.info("Inner Join completado correctamente")

# COMMAND ----------

# DBTITLE 6,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())
