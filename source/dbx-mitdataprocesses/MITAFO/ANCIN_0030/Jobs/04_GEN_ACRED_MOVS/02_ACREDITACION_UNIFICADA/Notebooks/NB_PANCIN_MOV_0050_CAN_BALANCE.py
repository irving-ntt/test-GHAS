# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0050_CAN_BALANCE
# MAGIC
# MAGIC **Descripción:** Job parallel que inserta información de la tabla ETL_MOVIMIENTOS a la tabla TTAFOGRAL_BALANCE_MOVS para calcular el balance de movimientos.
# MAGIC
# MAGIC **Subetapa:** Cálculo de balance de movimientos
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - CX_CRE_ESQUEMA.TL_CRE_MOVIMIENTOS (Oracle - staging)
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_TIPO_SUBCTA (Oracle - configuración)
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_CONFIG_CONCEP_MOV (Oracle - configuración)
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_BALANCE_MOVS (Oracle - balance existente)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_BALANCE_MOVS (Oracle - resultado final)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_MOVIMIENTOS_ETL_{sr_folio}
# MAGIC - TEMP_MAX_ID_BALANCE_{sr_folio}
# MAGIC - TEMP_JOIN_COMPLETO_{sr_folio}
# MAGIC - NB_PANCIN_MOV_0050_CAN_BALANCE_001_BALANCE_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0050_CAN_BALANCE_001_MOVIMIENTOS.sql
# MAGIC - NB_PANCIN_MOV_0050_CAN_BALANCE_002_MAX_ID.sql
# MAGIC - NB_PANCIN_MOV_0050_CAN_BALANCE_003_JOIN_COMPLETO.sql
# MAGIC - NB_PANCIN_MOV_0050_CAN_BALANCE_004_TRANSFORM.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100**: Extraer movimientos ETL desde Oracle a Delta
# MAGIC 2. **DB_300**: Extraer máximo ID de balance desde Oracle a Delta
# MAGIC 3. **CG_110 + JO_120**: Aplicar lógica de join con SQL desde Delta
# MAGIC 4. **TF_130**: Aplicar transformaciones de balance con SQL desde Delta
# MAGIC 5. **DB_200**: Insertar resultado final en Oracle (TTAFOGRAL_BALANCE_MOVS)
# MAGIC 6. **Notebook finaliza**: Balance de movimientos calculado e insertado en Oracle
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0050_CAN_BALANCE_image.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
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
params.validate()

conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

display(query.get_sql_list())

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0050_CAN_BALANCE_image (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,DB_100: EXTRACCIÓN DE MOVIMIENTOS ETL DESDE ORACLE
# Cargar query para leer movimientos de la tabla ETL_MOVIMIENTOS
# NOTA: Los parámetros constantes han sido extraídos del logs.txt del job JP_PANCIN_MOV_0050_CAN_BALANCE
statement_movimientos = query.get_statement(
    "NB_PANCIN_MOV_0050_CAN_BALANCE_001_MOVIMIENTOS.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,  # Valor del logs.txt: CIERREN_ETL
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,  # Valor del logs.txt: CIERREN
    TL_CRE_MOVIMIENTOS=conf.TL_CRE_MOVIMIENTOS,  # Valor del logs.txt: TTSISGRAL_ETL_MOVIMIENTOS
    TL_CRN_TIPO_SUBCTA=conf.TL_CRN_TIPO_SUBCTA,  # Valor del logs.txt: TCCRXGRAL_TIPO_SUBCTA
    TL_CRN_CONFIG_CONCEP_MOV=conf.TL_CRN_CONFIG_CONCEP_MOV,  # Valor del logs.txt: TFAFOGRAL_CONFIG_CONCEP_MOV
    SR_FOLIO=params.sr_folio,  # Valor variable del logs.txt
)

# Extraer movimientos ETL desde Oracle y escribir en Delta
# NOTA: No crear DataFrame intermedio - operación directa
db.write_delta(
    f"TEMP_MOVIMIENTOS_ETL_{params.sr_folio}",
    db.read_data("default", statement_movimientos),
    "overwrite",
)

logger.info("Movimientos ETL extraídos desde Oracle y guardados en Delta")

# Debug: mostrar datos si está habilitado
if conf.debug:
    display(db.read_delta(f"TEMP_MOVIMIENTOS_ETL_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0050_CAN_BALANCE_image (3).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 2,DB_300: EXTRACCIÓN DE MÁXIMO ID DE BALANCE DESDE ORACLE
# Cargar query para obtener el máximo ID de balance existente
statement_max_id = query.get_statement(
    "NB_PANCIN_MOV_0050_CAN_BALANCE_002_MAX_ID.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,  # Valor del logs.txt: CIERREN
    TL_CRN_BALANCE_MOVS=conf.TL_CRN_BALANCE_MOVS,  # Valor del logs.txt: TTAFOGRAL_BALANCE_MOVS
)

# Extraer máximo ID desde Oracle y escribir en Delta
# NOTA: No crear DataFrame intermedio - operación directa
db.write_delta(
    f"TEMP_MAX_ID_BALANCE_{params.sr_folio}",
    db.read_data("default", statement_max_id),
    "overwrite",
)

logger.info("Máximo ID de balance extraído desde Oracle y guardado en Delta")

# Debug: mostrar datos si está habilitado
if conf.debug:
    display(db.read_delta(f"TEMP_MAX_ID_BALANCE_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0050_CAN_BALANCE_image (2).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0050_CAN_BALANCE_image (4).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 TRANSFORMER CG_110 + JO_120: GENERACIÓN DE LLAVE Y JOIN OPTIMIZADO
# MAGIC
# MAGIC **Propósito:** Combinar la lógica de Column Generator (CG_110) y Join (JO_120) en una sola operación
# MAGIC
# MAGIC **Reglas de Negocio:**
# MAGIC - **CG_110 (Column Generator)**:
# MAGIC   - Agregar campo `LLAVE = 1` a todos los registros de movimientos
# MAGIC   - Campo `LLAVE` sirve como clave artificial para el join
# MAGIC - **JO_120 (Join)**:
# MAGIC   - Unir movimientos con el máximo ID de balance usando `LLAVE` como clave
# MAGIC   - Join se hace sobre `1 = 1` ya que todos los registros tienen `LLAVE = 1`
# MAGIC
# MAGIC **Leyenda de Campos:**
# MAGIC - `LLAVE`: Campo artificial agregado para facilitar el join (siempre valor 1)
# MAGIC - `MAXIMO1`: Valor máximo del ID de balance existente en Oracle
# MAGIC - Todos los campos de movimientos se mantienen sin modificación
# MAGIC
# MAGIC **OPTIMIZACIÓN DE PERFORMANCE - Broadcast Join:**
# MAGIC - **Problema identificado**: Join entre tabla grande (movimientos) y pequeña (máximo ID)
# MAGIC - **Solución implementada**: Broadcast join automático de Databricks
# MAGIC - **Cómo funciona**: La tabla pequeña se replica en todos los nodos para join eficiente
# MAGIC - **Beneficio**: Performance optimizada sin cambiar la lógica de negocio
# MAGIC
# MAGIC **Casos Especiales:**
# MAGIC - El join es un CROSS JOIN efectivo ya que todos los registros tienen `LLAVE = 1`
# MAGIC - Se combinan dos operaciones DataStage en una sola query SQL
# MAGIC - La lógica se ejecuta completamente sobre tablas Delta (no se regresa a Oracle)
# MAGIC - **PERFORMANCE**: Broadcast join optimiza automáticamente el join con tabla pequeña

# COMMAND ----------

# DBTITLE 3,CG_110 + JO_120: APLICAR LÓGICA DE JOIN CON SQL DESDE DELTA
# Aplicar lógica de Column Generator (LLAVE) y Join usando SQL desde Delta
# NOTA: Ahora usamos las tablas Delta creadas, no volvemos a Oracle
statement_join_completo = query.get_statement(
    "NB_PANCIN_MOV_0050_CAN_BALANCE_003_JOIN_COMPLETO.sql",
    TABLA_MOVIMIENTOS_ETL=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_MOVIMIENTOS_ETL_{params.sr_folio}",
    TABLA_MAX_ID_BALANCE=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_MAX_ID_BALANCE_{params.sr_folio}",
)

# Aplicar join completo y guardar en Delta
# NOTA: No crear DataFrame intermedio - operación directa
db.write_delta(
    f"TEMP_JOIN_COMPLETO_{params.sr_folio}",
    db.sql_delta(statement_join_completo),
    "overwrite",
)

logger.info("Join completo aplicado y guardado en Delta")

# Debug: mostrar datos si está habilitado
if conf.debug:
    display(db.read_delta(f"TEMP_JOIN_COMPLETO_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables del transformer
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0050_CAN_BALANCE_image (5).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC
# MAGIC ### Transformaciones
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0050_CAN_BALANCE_image (6).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 TRANSFORMER TF_130: CÁLCULO DE BALANCES
# MAGIC
# MAGIC **Propósito:** Aplicar lógica de negocio para calcular balances según tipo de movimiento
# MAGIC
# MAGIC **Reglas de Negocio:**
# MAGIC - **Tipo 180 (Disponible/Aportaciones)**:
# MAGIC   - Montos se asignan a saldos disponibles (`FTN_DISP_PESOS`, `FTN_DISP_ACCIONES`)
# MAGIC   - Montos se asignan a saldos comprometidos con signo negativo (`FTN_COMP_PESOS`, `FTN_COMP_ACCIONES`)
# MAGIC - **Tipo 181 (Pendiente/Retiros)**:
# MAGIC   - Montos se asignan a saldos pendientes con signo negativo (`FTN_PDTE_PESOS`, `FTN_PDTE_ACCIONES`)
# MAGIC - **Otros tipos**: No generan saldos (valores en 0)
# MAGIC
# MAGIC **Leyenda de Campos Calculados:**
# MAGIC - `FTN_DISP_PESOS`: Saldos disponibles en pesos (Tipo 180, valor positivo)
# MAGIC - `FTN_DISP_ACCIONES`: Saldos disponibles en acciones (Tipo 180, valor positivo)
# MAGIC - `FTN_PDTE_PESOS`: Saldos pendientes en pesos (Tipo 181, valor negativo)
# MAGIC - `FTN_PDTE_ACCIONES`: Saldos pendientes en acciones (Tipo 181, valor negativo)
# MAGIC - `FTN_COMP_PESOS`: Saldos comprometidos en pesos (Tipo 180, valor negativo)
# MAGIC - `FTN_COMP_ACCIONES`: Saldos comprometidos en acciones (Tipo 180, valor negativo)
# MAGIC - `FTN_DIA_PESOS`: Saldos del día en pesos (siempre 0)
# MAGIC - `FTN_DIA_ACCIONES`: Saldos del día en acciones (siempre 0)
# MAGIC
# MAGIC **Casos Especiales:**
# MAGIC - Montos de acciones siguen exactamente la misma lógica que pesos
# MAGIC - Campos de día (`FTN_DIA_*`) siempre son 0 por diseño
# MAGIC - Origen de aportación (`FTN_ORIGEN_APORTACION`) siempre es NULL
# MAGIC - Fechas de creación y actualización usan zona horaria de México
# MAGIC - Usuario de creación y actualización viene de `conf.CX_CRE_USUARIO`

# COMMAND ----------

# DBTITLE 4,TF_130: APLICAR TRANSFORMACIONES DE BALANCE CON SQL DESDE DELTA
# Aplicar lógica de negocio del transformer TF_130 usando SQL desde Delta
# NOTA: Usamos la tabla Delta del join, no volvemos a Oracle
statement_transform = query.get_statement(
    "NB_PANCIN_MOV_0050_CAN_BALANCE_004_TRANSFORM.sql",
    TABLA_JOIN_COMPLETO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_JOIN_COMPLETO_{params.sr_folio}",
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO,  # Valor del logs.txt: CIERREN_ETL_APP
)

# Aplicar transformaciones TF_130 y escribir resultado final en Oracle
# NOTA: Usamos write_data para INSERT en Oracle, no write_delta
# NOTA: Schema completo obligatorio: CX_CRN_ESQUEMA.TL_CRN_BALANCE_MOVS

db.write_data(
    db.sql_delta(statement_transform),
    f"{conf.CX_CRN_ESQUEMA}.{conf.TL_CRN_BALANCE_MOVS}",  # Schema completo: CIERREN.TTAFOGRAL_BALANCE_MOVS
    "default",
    "append",
)

# df_balance_transform = db.sql_delta(statement_transform)
# logger.info(df_balance_transform.printSchema())

logger.info("Transformaciones TF_130 aplicadas y balance final insertado en Oracle")

# Debug: mostrar resultado final si está habilitado (antes de insertar en Oracle)
if conf.debug:
    display(db.sql_delta(statement_transform))

# COMMAND ----------

# DBTITLE 5,LIMPIEZA Y FINALIZACIÓN
CleanUpManager.cleanup_notebook(locals())
logger.info("=== JOB COMPLETADO: NB_PANCIN_MOV_0050_CAN_BALANCE ===")
