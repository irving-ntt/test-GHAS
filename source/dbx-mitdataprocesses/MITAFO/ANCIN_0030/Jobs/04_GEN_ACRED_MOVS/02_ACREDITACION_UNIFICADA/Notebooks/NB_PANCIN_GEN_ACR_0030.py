# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_GEN_ACR_0030
# MAGIC
# MAGIC **Descripción:** Job de Secuencia Maestra que valida y decide si ejecutar el workflow JQ_MOV_0010 en el sistema PANCIN
# MAGIC
# MAGIC **Subetapa:** Validación y control de ejecución del workflow PANCIN
# MAGIC
# MAGIC **Trámite:** Generación de movimientos y acreditación
# MAGIC
# MAGIC **Tablas Input:** NA
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - CONTROL_ARCHIVOS_PANCIN_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_GEN_ACR_0030_001_CREAR_CONTROL.sql
# MAGIC - NB_PANCIN_GEN_ACR_0030_002_VERIFICAR_CONTROL.sql
# MAGIC - NB_PANCIN_GEN_ACR_0030_003_ELIMINAR_CONTROL.sql
# MAGIC - NB_PANCIN_GEN_ACR_0030_004_INSERTAR_CONTROL.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job de Secuencia JQ_PANCIN_GEN_ACR_0030:
# MAGIC
# MAGIC ### **Stage NC_100 - Punto de Decisión Principal:**
# MAGIC - **$SR_SUBETAPA = 27** → Flujo de Acreditación (JQ_ACRE_0020)
# MAGIC - **$SR_SUBETAPA = 273** → Flujo de Validación y Generación (JQ_MOV_0010)
# MAGIC
# MAGIC ### **Lógica de Reproceso:**
# MAGIC - **$SR_REPROCESO = 1**: Crear archivo de control (prevenir duplicados)
# MAGIC - **$SR_REPROCESO = 2**: Eliminar archivo de control (permitir reproceso)
# MAGIC - **$SR_REPROCESO = 3**: Ejecutar sin archivo de control
# MAGIC
# MAGIC ### **Stages ANTES de JQ_MOV_0010 (SR_SUBETAPA = 273):**
# MAGIC 1. **EC_010_VALIDA**: Verificar archivo de control en tabla Delta
# MAGIC 2. **NC_020_EJECUCION**: Decisión de ejecución basada en control
# MAGIC 3. **SQ_200_EJENORMAL**: Secuenciador de ejecución normal
# MAGIC 4. **JQ_MOV_0010**: Job de generación de movimientos (se ejecuta o no según validación)
# MAGIC
# MAGIC **NOTA**: Los stages posteriores a JQ_MOV_0010 se implementan en notebooks independientes

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
queries_df = query.get_sql_list()
display(queries_df.filter(col("Archivo SQL").contains("GEN_ACR_0030")))

# COMMAND ----------

# MAGIC %md
# MAGIC <ol>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316535?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_GEN_ACR_0030.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316539?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_GEN_ACR_0030_A.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,STAGE NC_100 - PUNTO DE DECISIÓN PRINCIPAL
# Equivalente al Stage NC_100 del XML
# Determinar qué flujo ejecutar basado en SR_SUBETAPA

logger.info("=== STAGE NC_100 - PUNTO DE DECISIÓN PRINCIPAL ===")
logger.info(f"SR_SUBETAPA: {params.sr_subetapa}")
logger.info(f"SR_REPROCESO: {params.sr_reproceso}")

# Lógica de decisión basada en SR_SUBETAPA
if params.sr_subetapa == "27":
    # Flujo de Acreditación
    # Por ahora solo registramos la decisión
    flujo_ejecutado = "ACREDITACION"
    workflow_ejecutar = "JQ_ACRE_0020"
    logger.info("🚀 Se setea JQ_ACRE_0020 para ejecutarse")
    dbutils.jobs.taskValues.set(
        key="workflow_ejecutar", value="JQ_ACRE_0020"
    )

elif params.sr_subetapa == "273":
    # Flujo de Validación y Generación de Movimientos
    # Por ahora solo registramos la decisión
    flujo_ejecutado = "GENERACION DE MOVIMIENTOS"
    workflow_ejecutar = "JQ_MOV_0010"

else:
    # Subetapa no reconocida
    logger.warning(f"⚠️ SR_SUBETAPA no reconocido: {params.sr_subetapa}")
    flujo_ejecutado = "NO_RECONOCIDO"
    workflow_ejecutar = "NONE"

logger.info(f"Flujo seleccionado: {flujo_ejecutado}")
logger.info(f"Workflow a ejecutar: {workflow_ejecutar}")

# COMMAND ----------

# MAGIC %md
# MAGIC <ol>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316538?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_GEN_ACR_0030_B.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC </ol>
# MAGIC # cd #$RT_PATH_TEMP#;  if [ -f "3281_#$SR_FOLIO##$EXT_ARC_TXT#" ] ;  then echo "1" ; else echo "2" ; fi
# MAGIC Valida si existe un archivo con folio, si existe el resultado 1 si no existe el resultado es 2.
# MAGIC

# COMMAND ----------

# DBTITLE 1,EC_010_VALIDA
# DBTITLE 2,VALIDACIONES ANTES DE JQ_MOV_0010 (SR_SUBETAPA = 273)
# Implementar validaciones ANTES de ejecutar JQ_MOV_0010


resultado_verificacion = None #se agrega linea 20251219

# STAGE EC_010_VALIDA
if params.sr_subetapa == "273":
    logger.info("ℹ️ Validando si tabla de control existe...")
    if spark.catalog.tableExists(f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.control_archivos_pancin"):
        logger.info("✅ La tabla de control 'control_archivos_pancin' existe.")
        # consultar control_archivos_pancin y buscar coincidencias por SR_FOLIO, SR_SUBPROCESO, SR_SUBETAPA
        # si existen coincidencias, entonces resultado_verificacion = "1"
        # si no existen coincidencias, entonces resultado_verificacion = "2"
        control_value = spark.sql(
            f"""
            SELECT *
            FROM {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.control_archivos_pancin
            WHERE sr_folio = '{params.sr_folio}'
            AND sr_subproceso = '{params.sr_subproceso}'
            AND sr_subetapa = '{params.sr_subetapa}'
            AND estado = 'True'
            """
        ).collect() 
        logger.info(f"Resultado de verificación: {control_value}")
        if len(control_value) > 0:
            logger.info("✅ Se encontraron coincidencias en la tabla de control.")
            logger.info(f"ℹ️ SR_REPROCESO: {params.sr_reproceso}")
            resultado_verificacion = "1"
        else:
            logger.info("❌ No se encontraron coincidencias en la tabla de control.")
            logger.info(f"ℹ️ SR_REPROCESO: {params.sr_reproceso}")
            resultado_verificacion = "2"
    else:
        logger.warning("⚠️ La tabla de control 'control_archivos_pancin' no existe.")
        # 1. Crear tabla de control si no existe
        statement_crear_tabla = query.get_statement(
            "NB_PANCIN_GEN_ACR_0030_001_CREAR_CONTROL.sql",
            CATALOG=SETTINGS.GENERAL.CATALOG,
            SCHEMA=SETTINGS.GENERAL.SCHEMA,
        )
        # Ejecutar creación de tabla si no existe
        db.sql_delta(statement_crear_tabla)
        logger.info("✅ Tabla de control verificada/creada exitosamente")
        resultado_verificacion = "2"

logger.info(f"ℹ️ Resultado de verificación: {resultado_verificacion}")
        

# COMMAND ----------

# MAGIC %md
# MAGIC # **Flujos del Proceso**
# MAGIC
# MAGIC ## **1. Flujo Normal**  
# MAGIC **Condiciones:** `SR_ETAPA = 1` | `SR_REPROCESO = 1`  
# MAGIC **Descripción:**  
# MAGIC Genera los movimientos por primera vez y crea el archivo de control correspondiente.  
# MAGIC Este archivo impide que los movimientos se vuelvan a calcular nuevamente.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **2. Flujo de Rechazo**  
# MAGIC **Condiciones:** `SR_ETAPA = 2` | `SR_REPROCESO = 2`  
# MAGIC **Descripción:**  
# MAGIC Elimina los movimientos previamente generados y borra el registro de control, desbloqueando el folio.  
# MAGIC De esta manera, el sistema puede recalcular los movimientos como si fuera la primera ejecución.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **3. Flujo de Recalculo**  
# MAGIC **Condiciones:** `SR_ETAPA = 3` | `SR_REPROCESO = 3`  
# MAGIC **Descripción:**  
# MAGIC Permite recalcular los movimientos manteniendo el registro de control activo.  
# MAGIC Este registro bloquea el cálculo inicial (como si fuera la primera vez), pero permite realizar recalculos tantas veces como sea necesario.
# MAGIC
# MAGIC # Condiciones de ejecucion:
# MAGIC - `resultado_verificacion = 1` (**SI** existe registro del folio en la tabla de control) y `SR_REPROCESO es 2 o 3`
# MAGIC - `resultado_verificacion = 2` (**NO** existe registro del folio en la tabla de control) y `SR_REPROCESO = 1`

# COMMAND ----------

# MAGIC %md
# MAGIC <ol>
# MAGIC   <li>
# MAGIC     <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316537?o=3494755435873649" target="_blank">
# MAGIC       <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_GEN_ACR_0030_C.png" style="max-width: 100%; height: auto;"/>
# MAGIC     </a>
# MAGIC   </li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,NC_020_EJECUCION (Evitar duplicacion de procesos)
if params.sr_subetapa == "273":
    # Lógica de decisión basada en archivo de control y SR_REPROCESO
    if (resultado_verificacion == "1" and params.sr_reproceso in ["2", "3"]) or (
        resultado_verificacion == "2" and params.sr_reproceso == "1"
    ):
        logger.info("✅ Condición de ejecución cumplida - Se procede con JQ_MOV_0010")
        ejecutar_movimientos = True
    else:
        logger.info("❌ Condición de ejecución NO cumplida - NO se ejecuta JQ_MOV_0010")
        ejecutar_movimientos = False

# COMMAND ----------

# DBTITLE 1,SQ_200_EJENORMAL
if params.sr_subetapa == "273":
    # STAGE SQ_200_EJENORMAL - Secuenciador de ejecución normal
    logger.info("--- STAGE SQ_200_EJENORMAL: Secuenciador de ejecución normal ---")

    if ejecutar_movimientos:
        logger.info("🚀 Se setea JQ_MOV_0010 para ejecutarse")
        dbutils.jobs.taskValues.set(
            key="workflow_ejecutar", value="JQ_MOV_0010"
        )
    else:
        logger.info("⏸️ No se ejecuta JQ_MOV_0010 - Condiciones no cumplidas")

# COMMAND ----------

# DBTITLE 3,RESUMEN DE EJECUCIÓN

logger.info("=== RESUMEN DE EJECUCIÓN ===")
logger.info(f"Folio procesado: {params.sr_folio}")
logger.info(f"Subproceso: {params.sr_subproceso}")
logger.info(f"Subetapa: {params.sr_subetapa}")
logger.info(f"Reproceso: {params.sr_reproceso}")
logger.info(f"Flujo a ejecutar: {flujo_ejecutado}")
logger.info(f"Workflow principal: {workflow_ejecutar}")

if params.sr_subetapa == "273":
    logger.info(f"Se ejecuta generacion se movimientos: {ejecutar_movimientos}")
    if not ejecutar_movimientos:
        raise ValueError("Condición de ejecución NO cumplida - NO se ejecuta JQ_MOV_0010")

logger.info("✅ Job de secuencia completado exitosamente")

# COMMAND ----------

# DBTITLE 1,SETEO DE PARAMETROS RECIBIDOS
for key, value in params.to_dict().items():
    #if key not in ("sr_folio_rel", "sr_tipo_mov", "sr_fecha_acc"):
    dbutils.jobs.taskValues.set(key=key, value=value)

# COMMAND ----------

# Limpieza del notebook
CleanUpManager.cleanup_notebook(locals())
