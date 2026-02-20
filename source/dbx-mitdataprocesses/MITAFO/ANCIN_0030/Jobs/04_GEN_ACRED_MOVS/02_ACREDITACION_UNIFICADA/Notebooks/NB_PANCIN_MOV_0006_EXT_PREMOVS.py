# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0006_EXT_PREMOVS
# MAGIC
# MAGIC **Descripción:** Procesamiento de premovimientos con múltiples flujos y joins
# MAGIC
# MAGIC **Subetapa:** Procesamiento de premovimientos
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - CX_CRE_ESQUEMA.TL_CRE_PRE_MOVIMIENTOS
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_TIPO_SUBCTA
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_MOV_SUBCTA
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_CONFIG_SUBPROCESO
# MAGIC - CX_BNF_ESQUEMA.TL_BNF_TRAMITE
# MAGIC - CX_BNF_ESQUEMA.TL_BNF_TRAMITE_SUBSECUENTE
# MAGIC - RESULTADO_MATRIZ_CONVIVENCIA_{sr_folio} (Delta)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_PREMOVIMIENTOS_{sr_folio}
# MAGIC - TEMP_FLUJO_1_{sr_folio}
# MAGIC - TEMP_FLUJO_2_{sr_folio}
# MAGIC - RESULTADO_PREMOVIMIENTOS_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0006_EXT_PREMOVS_001.sql
# MAGIC - NB_PANCIN_MOV_0006_EXT_PREMOVS_003.sql
# MAGIC - NB_PANCIN_MOV_0006_EXT_PREMOVS_004.sql
# MAGIC - NB_PANCIN_MOV_0006_EXT_PREMOVS_005.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100**: Extracción de premovimientos desde Oracle
# MAGIC 2. **DS_600**: Acceso a tabla Delta (matriz convivencia)
# MAGIC 3. **TF_200**: Transformer (división en flujos)
# MAGIC 4. **JO_300**: Join flujo 1 con datos de marca
# MAGIC 5. **JO_400**: Join flujo 2 con datos de marca
# MAGIC 6. **FU_500**: Funnel (unión de flujos)
# MAGIC 7. **DS_700**: Guardado en dataset (sufijo 06)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316552?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0006_EXT_PREMOVS.png" style="max-width: 100%; height: auto;"/>
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
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316553?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0006_EXT_PREMOVS_A.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE DATOS DE ORACLE - PREMOVIMIENTOS
# Cargar query para extraer datos de premovimientos desde Oracle
statement_001 = query.get_statement(
    "NB_PANCIN_MOV_0006_EXT_PREMOVS_001.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    CX_BNF_ESQUEMA=conf.CX_BNF_ESQUEMA,
    TL_CRE_PRE_MOVIMIENTOS=conf.TL_CRE_PRE_MOVIMIENTOS,
    TL_CRN_TIPO_SUBCTA=conf.TL_CRN_TIPO_SUBCTA,
    TL_CRN_MOV_SUBCTA=conf.TL_CRN_MOV_SUBCTA,
    TL_CRN_CONFIG_SUBPROCESO=conf.TL_CRN_CONFIG_SUBPROCESO,
    TL_BNF_TRAMITE=conf.TL_BNF_TRAMITE,
    TL_BNF_TRAMITE_SUBSECUENTE=conf.TL_BNF_TRAMITE_SUBSECUENTE,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 2,GUARDAR DATOS EN TABLA DELTA TEMPORAL
# Leer datos de Oracle y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_PREMOVIMIENTOS_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite",
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_PREMOVIMIENTOS_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # FILTRO TRANSFORMER
# MAGIC ```
# MAGIC FCN_ID_TIPO_MOV = 180
# MAGIC OR (
# MAGIC     FCN_ID_TIPO_MOV = 181
# MAGIC     AND (
# MAGIC         FCN_ID_SUBPROCESO = 130
# MAGIC         OR FCN_ID_SUBPROCESO = 6398
# MAGIC         OR FCN_ID_SUBPROCESO = 3281
# MAGIC         OR FCN_ID_SUBPROCESO = 10555
# MAGIC     )
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC # Regla VFOLIO
# MAGIC ```
# MAGIC IF FCN_ID_TIPO_MOV = 181
# MAGIC    AND (
# MAGIC         FCN_ID_SUBPROCESO = 130
# MAGIC         OR FCN_ID_SUBPROCESO = 6398
# MAGIC         OR FCN_ID_SUBPROCESO = 3281
# MAGIC    )
# MAGIC THEN FTC_FOLIO
# MAGIC ELSE IF FCN_ID_TIPO_MOV = 180
# MAGIC         AND ISNULL(FOLIO)
# MAGIC      THEN FTC_FOLIO
# MAGIC ELSE IF FCN_ID_TIPO_MOV = 180
# MAGIC         AND ISNOTNULL(FOLIO)
# MAGIC      THEN FOLIO
# MAGIC ELSE '0'
# MAGIC ```
# MAGIC
# MAGIC <ol>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316577?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0006_EXT_PREMOVS_B.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316578?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0006_EXT_PREMOVS_C.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 3,ACCEDER A TABLA DELTA TEMPORAL (DS_600)
# Los archivos .ds de DataStage ya son tablas Delta en Databricks
# No necesitamos leer el archivo, ya existe como tabla Delta
# RESULTADO_MATRIZ_CONVIVENCIA_{params.sr_folio} ya existe como tabla Delta

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"RESULTADO_MATRIZ_CONVIVENCIA_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 4,PROCESAR FLUJO 1 - JOIN CON DATOS DE MARCA
# Cargar query para procesar flujo 1 (JO_300)
statement_003 = query.get_statement(
    "NB_PANCIN_MOV_0006_EXT_PREMOVS_003.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Procesar flujo 1 y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_FLUJO_1_{params.sr_folio}", db.sql_delta(statement_003), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_FLUJO_1_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # FILTRO TRANSFORMER
# MAGIC ```
# MAGIC FCN_ID_TIPO_MOV = 181
# MAGIC AND FCN_ID_SUBPROCESO <> 130
# MAGIC AND FCN_ID_SUBPROCESO <> 6398
# MAGIC AND FCN_ID_SUBPROCESO <> 3281
# MAGIC AND FCN_ID_SUBPROCESO <> 10555
# MAGIC ```
# MAGIC
# MAGIC <ol>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316580?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0006_EXT_PREMOVS_D.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316579?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0006_EXT_PREMOVS_E.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 5,PROCESAR FLUJO 2 - JOIN CON DATOS DE MARCA
# Cargar query para procesar flujo 2 (JO_400)
statement_004 = query.get_statement(
    "NB_PANCIN_MOV_0006_EXT_PREMOVS_004.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Procesar flujo 2 y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_FLUJO_2_{params.sr_folio}", db.sql_delta(statement_004), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_FLUJO_2_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 6,COMBINAR FLUJOS Y GUARDAR RESULTADO FINAL
# Cargar query para combinar ambos flujos (FU_800)
statement_005 = query.get_statement(
    "NB_PANCIN_MOV_0006_EXT_PREMOVS_005.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    CLV_UNID_NEG=conf.CLV_UNID_NEG,
    CLV_SUFIJO_06=conf.CLV_SUFIJO_06,
    EXT_ARC_DS=conf.EXT_ARC_DS,
)

# Combinar flujos y guardar resultado final
db.write_delta(
    f"RESULTADO_PREMOVIMIENTOS_{params.sr_folio}",
    db.sql_delta(statement_005),
    "overwrite",
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"RESULTADO_PREMOVIMIENTOS_{params.sr_folio}"))
    logger.info(
        f"Registros procesados: {db.read_delta(f'RESULTADO_PREMOVIMIENTOS_{params.sr_folio}').count()}"
    )

# COMMAND ----------

# DBTITLE 7,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())
