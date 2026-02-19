# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0016_SUF_SALDO
# MAGIC
# MAGIC **Descripción:** Cálculo de suficiencia de saldos con agregaciones complejas
# MAGIC
# MAGIC **Subetapa:** Cálculo de suficiencia de saldos
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - CX_CRE_ESQUEMA.TL_CRE_PRE_MOVIMIENTOS
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_TIPO_SUBCTA
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_MOV_SUBCTA
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_CONFIG_CONCEP_MOV
# MAGIC - CX_BNF_ESQUEMA.TL_BNF_TRAMITE
# MAGIC - CX_BNF_ESQUEMA.TL_BNF_TRAMITE_SUBSECUENTE
# MAGIC - RESULTADO_MATRIZ_CONVIVENCIA_{sr_folio} (Delta)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_PREMOVIMIENTOS_{sr_folio}
# MAGIC - TEMP_TRANSFORMACION_{sr_folio}
# MAGIC - TEMP_DATOS_CON_MARCA_{sr_folio}
# MAGIC - TEMP_DATOS_SIN_MARCA_{sr_folio}
# MAGIC - TEMP_JOIN_TIPO_SUBCTA_{sr_folio}
# MAGIC - TEMP_JOIN_MOV_SUBCTA_{sr_folio}
# MAGIC - TEMP_JOIN_CONCEP_MOV_{sr_folio}
# MAGIC - TEMP_JOIN_VALOR_ACCION_{sr_folio}
# MAGIC - TEMP_JOIN_FINAL_{sr_folio}
# MAGIC - RESULTADO_SUF_SALDO_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0016_SUF_SALDO_001.sql
# MAGIC - NB_PANCIN_MOV_0016_SUF_SALDO_002.sql
# MAGIC - NB_PANCIN_MOV_0016_SUF_SALDO_003.sql
# MAGIC - NB_PANCIN_MOV_0016_SUF_SALDO_004.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100**: Extracción de premovimientos desde Oracle
# MAGIC 2. **TF_200**: Transformación de datos
# MAGIC 3. **DS_600**: Acceso a tabla Delta (matriz convivencia)
# MAGIC 4. **FT_400**: Filtro de datos con marca
# MAGIC 5. **JO_500**: Joins múltiples con tablas de referencia
# MAGIC 6. **AG_500**: Agregación con reglas de decimales
# MAGIC 7. **DS_700**: Guardado en dataset (sufijo 06)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316592?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0016_SUF_SALDO.png" style="max-width: 100%; height: auto;"/>
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
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316593?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0016_SUF_SALDO_A.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,EXTRAER DATOS DE PREMOVIMIENTOS (DB_100_ETL_PRE_MOVIMIENTOS)
# Cargar query para extraer datos de premovimientos desde OCI
statement_001 = query.get_statement(
    "NB_PANCIN_MOV_0016_SUF_SALDO_001.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    CX_BNF_ESQUEMA=conf.CX_BNF_ESQUEMA,
    TL_CRE_PRE_MOVIMIENTOS=conf.TL_CRE_PRE_MOVIMIENTOS,
    TL_CRN_TIPO_SUBCTA=conf.TL_CRN_TIPO_SUBCTA,
    TL_CRN_MOV_SUBCTA=conf.TL_CRN_MOV_SUBCTA,
    TL_CRN_CONFIG_CONCEP_MOV=conf.TL_CRN_CONFIG_CONCEP_MOV,
    TL_BNF_TRAMITE=conf.TL_BNF_TRAMITE,
    TL_BNF_TRAMITE_SUBSECUENTE=conf.TL_BNF_TRAMITE_SUBSECUENTE,
    SR_FOLIO=params.sr_folio,
    SR_TIPO_MOV=params.sr_tipo_mov,
)

# Extraer datos de premovimientos y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_PREMOVIMIENTOS_{params.sr_folio}", db.read_data("default", statement_001), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_PREMOVIMIENTOS_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316594?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0016_SUF_SALDO_B.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 2,PROCESAR TRANSFORMACIÓN (TF_200)
# Cargar query para procesar la transformación de datos
statement_002 = query.get_statement(
    "NB_PANCIN_MOV_0016_SUF_SALDO_002.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Procesar transformación y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_TRANSFORMACION_{params.sr_folio}", db.sql_delta(statement_002), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_TRANSFORMACION_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316595?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0016_SUF_SALDO_C.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 3,ACCEDER A TABLA DELTA TEMPORAL (DS_600)
# Los archivos .ds de DataStage ya son tablas Delta en Databricks
# No necesitamos leer el archivo, ya existe como tabla Delta
# RESULTADO_MATRIZ_CONVIVENCIA_{params.sr_folio} ya existe como tabla Delta

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"RESULTADO_MATRIZ_CONVIVENCIA_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 4,FILTRAR DATOS CON MARCA (FT_400)
# Cargar query para filtrar datos con marca
statement_003 = query.get_statement(
    "NB_PANCIN_MOV_0016_SUF_SALDO_003.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Filtrar datos con marca y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_FILTRADO_MARCA_{params.sr_folio}", db.sql_delta(statement_003), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_FILTRADO_MARCA_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316596?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0016_SUF_SALDO_D.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 5,JOIN CON DATOS FILTRADOS (JO_300)
# Cargar query para realizar join con datos filtrados
statement_004 = query.get_statement(
    "NB_PANCIN_MOV_0016_SUF_SALDO_004.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Procesar join con datos filtrados
db.write_delta(
    f"TEMP_JOIN_DATOS_{params.sr_folio}", db.sql_delta(statement_004), "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_JOIN_DATOS_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316597?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0016_SUF_SALDO_E.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 6,AGREGAR DATOS (AG_500) - PROCESAMIENTO PYSPARK COMPLETO
# Obtener datos del join anterior para procesamiento con PySpark
df_join_data = db.read_delta(f"TEMP_JOIN_DATOS_{params.sr_folio}")

# Optimización de rendimiento: Cache del dataframe para operaciones múltiples
df_join_data.cache()
logger.info(f"Dataframe cacheado para optimización. Registros: {df_join_data.count()}")

# Agrupar y sumar con las precisiones decimales específicas del AG_500
# Aplicando configuraciones del AG_500: method=sort, nul_res=False, selection=reduce
# Configuración equivalente al AG_500:
# - method = sort (ordenamiento antes de agrupar)
# - nul_res = False (no permitir nulos en resultado)
# - selection = reduce (solo campos de agrupación + agregaciones)

# Ordenar primero (equivalente a method=sort)
df_sorted = df_join_data.orderBy(
    col("FTN_NUM_CTA_INVDUAL").asc_nulls_first(),
    col("FCN_ID_TIPO_SUBCTA").asc_nulls_first(),
    col("FCN_ID_SIEFORE").asc(),
    col("FTN_DEDUCIBLE").asc_nulls_first(),
    col("FCN_ID_PLAZO").asc_nulls_first(),
)

# Agrupar y agregar (equivalente a selection=reduce)
df_aggregated = (
    df_sorted.groupBy(
        "FTN_NUM_CTA_INVDUAL",
        "FCN_ID_TIPO_SUBCTA",
        "FCN_ID_SIEFORE",
        "FTN_DEDUCIBLE",
        "FCN_ID_PLAZO",
    )
    .agg(
        # FTF_MONTO_PESOS_SUM - decimal(18,2)
        spark_sum(
            when(col("FTF_MONTO_PESOS").isNotNull(), col("FTF_MONTO_PESOS")).otherwise(
                0
            )
        )
        .cast(DecimalType(18, 2))
        .alias("FTF_MONTO_PESOS_SUM"),
        # FTF_MONTO_ACCIONES_SUM - decimal(18,6)
        spark_sum(
            when(
                col("FTF_MONTO_ACCIONES").isNotNull(), col("FTF_MONTO_ACCIONES")
            ).otherwise(0)
        )
        .cast(DecimalType(18, 6))
        .alias("FTF_MONTO_ACCIONES_SUM"),
        # FTF_MONTO_PESOS_SOL - decimal(18,2)
        spark_sum(
            when(
                col("FTF_MONTO_PESOS_SOL").isNotNull(), col("FTF_MONTO_PESOS_SOL")
            ).otherwise(0)
        )
        .cast(DecimalType(18, 2))
        .alias("FTF_MONTO_PESOS_SOL"),
        # FTF_MONTO_ACCIONES_SOL - decimal(18,6)
        spark_sum(
            when(
                col("FTF_MONTO_ACCIONES_SOL").isNotNull(), col("FTF_MONTO_ACCIONES_SOL")
            ).otherwise(0)
        )
        .cast(DecimalType(18, 6))
        .alias("FTF_MONTO_ACCIONES_SOL"),
    )
    .orderBy(
        "FTN_NUM_CTA_INVDUAL",
        "FCN_ID_TIPO_SUBCTA",
        "FCN_ID_SIEFORE",
        "FTN_DEDUCIBLE",
        "FCN_ID_PLAZO",
    )
)

# Guardar resultado final con precisiones decimales aplicadas
# Reemplaza el dataset p_CLV_SUFIJO_06 = RESULTADO_PREMOVIMIENTOS
db.write_delta(
    f"RESULTADO_PREMOVIMIENTOS_{params.sr_folio}", df_aggregated, "overwrite"
)

# Mostrar resultados en modo debug
if conf.debug:
    display(df_aggregated)
    logger.info(f"Registros procesados: {df_aggregated.count()}")
    logger.info("Configuraciones AG_500 aplicadas:")
    logger.info("- method=sort: Ordenamiento antes de agrupar")
    logger.info("- nul_res=False: No permitir nulos en resultado (when().otherwise(0))")
    logger.info("- selection=reduce: Solo campos de agrupación + agregaciones")
    logger.info("Precisiones decimales aplicadas:")
    logger.info("- FTF_MONTO_PESOS_SUM: decimal(18,2)")
    logger.info("- FTF_MONTO_ACCIONES_SUM: decimal(18,6)")
    logger.info("- FTF_MONTO_PESOS_SOL: decimal(18,2)")
    logger.info("- FTF_MONTO_ACCIONES_SOL: decimal(18,6)")

# Limpiar cache para liberar memoria
df_join_data.unpersist()
logger.info("Cache liberado para optimizar uso de memoria")

# COMMAND ----------

# DBTITLE 7,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())
