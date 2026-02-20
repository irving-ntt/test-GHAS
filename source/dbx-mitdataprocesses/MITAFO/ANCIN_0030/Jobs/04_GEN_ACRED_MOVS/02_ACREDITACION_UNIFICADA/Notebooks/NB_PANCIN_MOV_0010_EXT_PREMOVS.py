# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0010_EXT_PREMOVS
# MAGIC
# MAGIC **Descripción:** Procesamiento de premovimientos con joins a tablas de referencia
# MAGIC
# MAGIC **Subetapa:** Procesamiento de premovimientos
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - RESULTADO_PREMOVIMIENTOS_{sr_folio} (Delta)
# MAGIC - TEMP_TIPO_SUBCTA_{sr_folio} (Delta temporal)
# MAGIC - TEMP_MOV_SUBCTA_{sr_folio} (Delta temporal)
# MAGIC - TEMP_CONCEPTO_MOV_{sr_folio} (Delta temporal)
# MAGIC - TEMP_VALOR_ACCION_{sr_folio} (Delta temporal)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_DATOS_CON_MARCA_{sr_folio}
# MAGIC - DATOS_SIN_MARCA_{sr_folio}
# MAGIC - TEMP_JOIN_TIPO_SUBCTA_{sr_folio}
# MAGIC - TEMP_JOIN_MOV_SUBCTA_{sr_folio}
# MAGIC - TEMP_JOIN_CONCEP_MOV_{sr_folio}
# MAGIC - TEMP_JOIN_VALOR_ACCION_{sr_folio}
# MAGIC - TEMP_JOIN_FINAL_{sr_folio}
# MAGIC - RESULTADO_MOVIMIENTOS_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0010_EXT_PREMOVS_001.sql
# MAGIC - NB_PANCIN_MOV_0010_EXT_PREMOVS_002.sql
# MAGIC - NB_PANCIN_MOV_0010_EXT_PREMOVS_003.sql
# MAGIC - NB_PANCIN_MOV_0010_EXT_PREMOVS_004.sql
# MAGIC - NB_PANCIN_MOV_0010_EXT_PREMOVS_005.sql
# MAGIC - NB_PANCIN_MOV_0010_EXT_PREMOVS_006.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DS_0100**: Acceso a tabla Delta (premovimientos)
# MAGIC 2. **FI_110**: Filtro de datos con marca
# MAGIC 3. **DS_200_SINMARCA**: Guardado de datos sin marca
# MAGIC 4. **JO_120**: Join con tipo subcuenta
# MAGIC 5. **JO_130**: Join con movimientos subcuenta
# MAGIC 6. **JO_140**: Join con concepto movimiento
# MAGIC 7. **JO_150**: Join con valor acción
# MAGIC 8. **DS_300**: Guardado en dataset (sufijo 02)
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

# DBTITLE 1,ACCEDER A TABLA DELTA TEMPORAL (DS_0100)
# Los archivos .ds de DataStage ya son tablas Delta en Databricks
# No necesitamos leer el archivo, ya existe como tabla Delta
# RESULTADO_PREMOVIMIENTOS_{params.sr_folio} ya existe como tabla Delta

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"RESULTADO_PREMOVIMIENTOS_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316587?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0010_EXT_PREMOVS_B.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 2,FILTRAR DATOS CON MARCA (FI_110)
# Cargar query para filtrar datos con marca
statement_001 = query.get_statement(
    "NB_PANCIN_MOV_0010_EXT_PREMOVS_001.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Filtrar datos con marca y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_DATOS_CON_MARCA_{params.sr_folio}", db.sql_delta(statement_001), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_DATOS_CON_MARCA_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316586?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0010_EXT_PREMOVS_C.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 3,GUARDAR DATOS SIN MARCA (DS_200_SINMARCA)
# Cargar query para obtener datos sin marca
statement_002 = query.get_statement(
    "NB_PANCIN_MOV_0010_EXT_PREMOVS_002.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    CLV_UNID_NEG=conf.CLV_UNID_NEG,
    CLV_SUFIJO_01=conf.CLV_SUFIJO_01,
    EXT_ARC_DS=conf.EXT_ARC_DS,
)

# Guardar datos sin marca
db.write_delta(
    f"DATOS_SIN_MARCA_{params.sr_folio}", db.sql_delta(statement_002), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"DATOS_SIN_MARCA_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316588?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0010_EXT_PREMOVS_D.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 4,JOIN CON TIPO SUBCTA (JO_120)
# Cargar query para join con tipo de subcuenta usando tabla temporal OCI
statement_003 = query.get_statement(
    "NB_PANCIN_MOV_0010_EXT_PREMOVS_003.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Procesar join con tipo de subcuenta usando tabla temporal OCI
db.write_delta(
    f"TEMP_JOIN_TIPO_SUBCTA_{params.sr_folio}", db.sql_delta(statement_003), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_JOIN_TIPO_SUBCTA_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316590?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0010_EXT_PREMOVS_E.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 5,JOIN CON MOV SUBCTA (JO_140)
# Cargar query para join con movimientos de subcuenta usando tabla temporal OCI
statement_004 = query.get_statement(
    "NB_PANCIN_MOV_0010_EXT_PREMOVS_004.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Procesar join con movimientos de subcuenta usando tabla temporal OCI
db.write_delta(
    f"TEMP_JOIN_MOV_SUBCTA_{params.sr_folio}", db.sql_delta(statement_004), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_JOIN_MOV_SUBCTA_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316591?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0010_EXT_PREMOVS_F.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 6,JOIN CON CONCEPTO MOV (JO_150)
# Cargar query para join con concepto de movimiento usando tabla temporal OCI
statement_005 = query.get_statement(
    "NB_PANCIN_MOV_0010_EXT_PREMOVS_005.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Procesar join con concepto de movimiento usando tabla temporal OCI
db.write_delta(
    f"TEMP_JOIN_CONCEPTO_MOV_{params.sr_folio}",
    db.sql_delta(statement_005),
    "overwrite",
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_JOIN_CONCEPTO_MOV_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316589?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0010_EXT_PREMOVS_G.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 7,JOIN CON VALOR ACCION (JO_160)
# Cargar query para join con valor de acción usando tabla temporal OCI
statement_006 = query.get_statement(
    "NB_PANCIN_MOV_0010_EXT_PREMOVS_006.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    CLV_UNID_NEG=conf.CLV_UNID_NEG,
    CLV_SUFIJO_02=conf.CLV_SUFIJO_02,
    EXT_ARC_DS=conf.EXT_ARC_DS,
)

# Procesar join con valor de acción usando tabla temporal OCI y guardar resultado final
db.write_delta(
    f"RESULTADO_MOVIMIENTOS_{params.sr_folio}", db.sql_delta(statement_006), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"RESULTADO_MOVIMIENTOS_{params.sr_folio}"))
    logger.info(
        f"Registros procesados: {db.read_delta(f'RESULTADO_MOVIMIENTOS_{params.sr_folio}').count()}"
    )

# COMMAND ----------

# DBTITLE 8,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())
