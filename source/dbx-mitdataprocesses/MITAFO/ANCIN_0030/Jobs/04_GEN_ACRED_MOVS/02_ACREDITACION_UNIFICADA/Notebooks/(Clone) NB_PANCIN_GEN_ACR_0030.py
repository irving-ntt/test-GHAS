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
        # Parámetros obligatorios del framework
        "var_tramite": str,
        "sr_etapa": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_id_snapshot": str,
        # Parámetros dinámicos del job de secuencia
        "sr_folio": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_reproceso": str,
        "rt_path_temp": str,
        "ext_arc_txt": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
display(queries_df.filter(col("Archivo SQL").contains("GEN_ACR_0030")))

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
    logger.info("🚀 EJECUTANDO FLUJO DE ACREDITACIÓN (SR_SUBETAPA = 27)")
    logger.info("📋 Workflow a ejecutar: JQ_ACRE_0020")

    # Aquí se ejecutaría JQ_ACRE_0020
    # Por ahora solo registramos la decisión
    flujo_ejecutado = "ACREDITACION"
    workflow_ejecutar = "JQ_ACRE_0020"

elif params.sr_subetapa == "273":
    # Flujo de Validación y Generación de Movimientos
    logger.info(
        "🚀 EJECUTANDO FLUJO DE VALIDACIÓN Y GENERACIÓN DE MOVIMIENTOS (SR_SUBETAPA = 273)"
    )
    logger.info("📋 Workflow a ejecutar: JQ_MOV_0010")

    # Aquí se ejecutaría JQ_MOV_0010
    # Por ahora solo registramos la decisión
    flujo_ejecutado = "VALIDACION_GENERACION"
    workflow_ejecutar = "JQ_MOV_0010"

else:
    # Subetapa no reconocida
    logger.warning(f"⚠️ SR_SUBETAPA no reconocido: {params.sr_subetapa}")
    flujo_ejecutado = "NO_RECONOCIDO"
    workflow_ejecutar = "NONE"

logger.info(f"Flujo seleccionado: {flujo_ejecutado}")
logger.info(f"Workflow a ejecutar: {workflow_ejecutar}")

# COMMAND ----------

# DBTITLE 2,VALIDACIONES ANTES DE JQ_MOV_0010 (SR_SUBETAPA = 273)
# Implementar validaciones ANTES de ejecutar JQ_MOV_0010

# STAGE EC_010_VALIDA y NC_020_EJECUCION
if params.sr_subetapa == "273":
    logger.info("=== VALIDACIONES ANTES DE JQ_MOV_0010 ===")

    # STAGE EC_010_VALIDA - Verificar archivo de control
    logger.info("--- STAGE EC_010_VALIDA: Verificar archivo de control ---")

    # 1. Crear tabla de control si no existe
    statement_crear_tabla = query.get_statement(
        "NB_PANCIN_GEN_ACR_0030_001_CREAR_CONTROL.sql",
        CATALOG=SETTINGS.GENERAL.CATALOG,
        SCHEMA=SETTINGS.GENERAL.SCHEMA,
    )

    # Ejecutar creación de tabla si no existe
    db.sql_delta(statement_crear_tabla)
    logger.info("✅ Tabla de control verificada/creada exitosamente")

    # 2. Insertar registro de control si SR_REPROCESO = 1
    if params.sr_reproceso == "1":
        logger.info("📝 SR_REPROCESO = 1: Insertando registro de control")
        statement_insertar = query.get_statement(
            "NB_PANCIN_GEN_ACR_0030_004_INSERTAR_CONTROL.sql",
            CATALOG=SETTINGS.GENERAL.CATALOG,
            SCHEMA=SETTINGS.GENERAL.SCHEMA,
            SR_FOLIO=params.sr_folio,
            SR_SUBPROCESO=params.sr_subproceso,
            SR_SUBETAPA=params.sr_subetapa,
        )

        # Ejecutar inserción
        db.sql_delta(statement_insertar)
        logger.info("✅ Registro de control insertado exitosamente")

    # 3. Verificar si existe el registro de control
    statement_verificar = query.get_statement(
        "NB_PANCIN_GEN_ACR_0030_002_VERIFICAR_CONTROL.sql",
        CATALOG=SETTINGS.GENERAL.CATALOG,
        SCHEMA=SETTINGS.GENERAL.SCHEMA,
        SR_FOLIO=params.sr_folio,
    )

    # Leer resultado de verificación
    resultado_verificacion = db.sql_delta(statement_verificar)

    # Obtener el valor de control (1 = existe, 2 = no existe)
    if conf.debug:
        display(resultado_verificacion)

    control_value = resultado_verificacion.collect()[0]["CONTROL_VALUE"]
    logger.info(f"Valor de control obtenido: {control_value}")

    # STAGE NC_020_EJECUCION - Decisión de ejecución
    logger.info("--- STAGE NC_020_EJECUCION: Decisión de ejecución ---")

    # Lógica de decisión basada en archivo de control y SR_REPROCESO
    if (control_value == "1" and params.sr_reproceso in ["2", "3"]) or (
        control_value == "2" and params.sr_reproceso == "1"
    ):
        logger.info("✅ Condición de ejecución cumplida - Procediendo con JQ_MOV_0010")
        ejecutar_movimientos = True
    else:
        logger.info("❌ Condición de ejecución NO cumplida - NO se ejecuta JQ_MOV_0010")
        ejecutar_movimientos = False

    # STAGE SQ_200_EJENORMAL - Secuenciador de ejecución normal
    logger.info("--- STAGE SQ_200_EJENORMAL: Secuenciador de ejecución normal ---")

    if ejecutar_movimientos:
        logger.info("🚀 Ejecutando JQ_MOV_0010 - Job de generación de movimientos")
        # Aquí se ejecutaría JQ_MOV_0010
        # Por ahora solo registramos la ejecución
        estado_movimientos = "EJECUTADO_OK"

        # IMPORTANTE: Después de ejecutar JQ_MOV_0010, se deben ejecutar los notebooks independientes
        logger.info(
            "📋 NOTA: Después de JQ_MOV_0010, ejecutar notebooks independientes para:"
        )
        logger.info("   - NC_0180_GEN_ARC: Decisión de archivo")
        logger.info("   - EC_030_ARCHIVO: Crear archivo de control")
        logger.info("   - EC_200_DELETE_ARC: Eliminar archivo de control")

    else:
        logger.info("⏸️ JQ_MOV_0010 NO ejecutado - Condiciones no cumplidas")
        estado_movimientos = "NO_EJECUTADO"

    # Setear job param para que sea evaluado en bloque if/else de Databricks
    dbutils.jobs.taskValues.set(
        key="ejecutar_movimientos", value=str(ejecutar_movimientos)
    )
    logger.info(f"📋 Job param seteado: ejecutar_movimientos = {ejecutar_movimientos}")

    logger.info("✅ Validaciones ANTES de JQ_MOV_0010 completadas")

# Renombar task param a workflow_ejecutar

# COMMAND ----------

# DBTITLE 3,RESUMEN DE EJECUCIÓN

logger.info("=== RESUMEN DE EJECUCIÓN ===")
logger.info(f"Folio procesado: {params.sr_folio}")
logger.info(f"Subproceso: {params.sr_subproceso}")
logger.info(f"Subetapa: {params.sr_subetapa}")
logger.info(f"Reproceso: {params.sr_reproceso}")
logger.info(f"Flujo ejecutado: {flujo_ejecutado}")
logger.info(f"Workflow principal: {workflow_ejecutar}")

if params.sr_subetapa == "273":
    logger.info(f"Estado de movimientos: {estado_movimientos}")
    logger.info(
        "📋 NOTA: Los stages posteriores a JQ_MOV_0010 se implementan en notebooks independientes"
    )

logger.info("✅ Job de secuencia completado exitosamente")

# COMMAND ----------

# Limpieza del notebook
CleanUpManager.cleanup_notebook(locals())
