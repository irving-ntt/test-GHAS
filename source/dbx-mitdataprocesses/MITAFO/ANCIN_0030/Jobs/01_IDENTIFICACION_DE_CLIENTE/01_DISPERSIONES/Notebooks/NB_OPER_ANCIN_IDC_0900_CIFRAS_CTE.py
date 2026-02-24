# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_IDC_0900_CIFRAS_CLIENTE
# MAGIC
# MAGIC **Descripción:** Job parallel que valida cifras de control de identificación de clientes para el proceso de identificación de clientes (IDC). Este job procesa los registros de identificación de clientes y genera estadísticas de validación que se almacenan en las tablas de control de cifras tanto en el esquema ETL como en el esquema principal de CIERREN.
# MAGIC
# MAGIC **Subetapa:** 24 - Identificación de Cliente
# MAGIC
# MAGIC **Trámite:** 7 - Proceso de Identificación de Clientes
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE (Oracle - validación de identificación)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_VAL_IDENT_CTE_{sr_id_archivo}
# MAGIC - {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_CIFRAS_CONTROL_{sr_id_archivo}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_001_OCI_VAL_IDENT.sql
# MAGIC - NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_002_OPTIMIZED_DELTA_CIFRAS.sql
# MAGIC - NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_006_OCI_DELETE_ETL.sql
# MAGIC - NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_006B_DELTA_INSERT_ETL.sql
# MAGIC - NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_007_OCI_DELETE_CIERREN.sql
# MAGIC - NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_007B_DELTA_INSERT_CIERREN.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **BD_100**: Extraer validación de identificación desde Oracle a Delta
# MAGIC 2. **OPTIMIZADO**: Calcular cifras de control en un solo query (combina FL_200, AGT_600, AGT_500, JN_700, TF_800)
# MAGIC 3. **BD_300**: Insertar estadísticas en tabla ETL (DELETE + INSERT)
# MAGIC 4. **BD_200**: Insertar estadísticas en tabla principal CIERREN (DELETE + INSERT)
# MAGIC 5. **Notebook finaliza**: Cifras de control calculadas e insertadas

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams(
    {
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_subetapa": str,
        "sr_subproceso": str,
        "sr_proceso": str,
        "sr_usuario": str,
        "sr_instancia_proceso": str,
        "sr_id_snapshot": str,
        "sr_etapa": str,
        "sr_paso": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_tipo_layout": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("NB_PANCIN_IDC_0900")))

# COMMAND ----------

# DBTITLE: 1. Extraer validación de identificación desde Oracle
logger.info("Paso 1: Extrayendo validación de identificación desde Oracle")

# Cargar query de extracción
statement_001 = query.get_statement(
    "NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_001_OCI_VAL_IDENT.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    TL_VAL_IDENT_CTE=conf.TL_VAL_IDENT_CTE,
    SR_FOLIO=params.sr_folio,

)

# Extraer datos de Oracle y escribir en Delta
db.write_delta(
    f"TEMP_VAL_IDENT_CTE_{params.sr_id_archivo}",
    db.read_data("default", statement_001),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_VAL_IDENT_CTE_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 2. Calcular cifras de control en un solo query optimizado
logger.info("Paso 2: Calculando cifras de control en un solo query optimizado")

# Cargar query optimizado que combina todos los cálculos
statement_002_opt = query.get_statement(
    "NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_002_OPTIMIZED_DELTA_CIFRAS.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    SR_SUBETAPA=params.sr_subetapa,
    SR_USUARIO="DATABRICKS",
)

# Ejecutar query optimizado y escribir tabla temporal
db.write_delta(
    f"TEMP_CIFRAS_CONTROL_{params.sr_id_archivo}",
    db.sql_delta(statement_002_opt),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_CIFRAS_CONTROL_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 3. Insertar estadísticas en tabla ETL
logger.info("Paso 3: Insertando estadísticas en tabla ETL")

# Primero eliminar registros existentes en ETL
statement_006_delete = query.get_statement(
    "NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_006_OCI_DELETE_ETL.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    TL_CRE_VAL_CIFRAS_CTRL=conf.TL_CRE_VAL_CIFRAS_CTRL,
    SR_FOLIO=params.sr_folio,
    SR_SUBETAPA=params.sr_subetapa,
)

# Primero eliminar registros existentes en CIERREN
statement_007_delete = query.get_statement(
    "NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_007_OCI_DELETE_CIERREN.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRN_VAL_CIFRAS_CONTROL=conf.TL_CRN_VAL_CIFRAS_CONTROL,
    SR_FOLIO=params.sr_folio,
    SR_SUBETAPA=params.sr_subetapa,
)

statements = [
    statement_006_delete,
    statement_007_delete
]

# Ejecutar todos los jobs en paralelo usando la nueva función
result = db.execute_multiple_statements_parallel(
   statements=statements,
   check_interval=30,  # Verificar cada 30 segundos
   max_wait_time=7200,  # Timeout de 2 horas
)

# COMMAND ----------

# Luego preparar datos para inserción
statement_006_insert = query.get_statement(
    "NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_006B_DELTA_INSERT_ETL.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_ID_ARCHIVO=params.sr_id_archivo,
)

# Insertar datos en ETL usando write_data
db.write_data(
    db.sql_delta(statement_006_insert),
    f"{conf.CX_CRE_ESQUEMA}.{conf.TL_CRE_VAL_CIFRAS_CTRL}",
    "default",
    "append",
)

# COMMAND ----------

# Luego preparar datos para inserción
statement_007_insert = query.get_statement(
    "NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_007B_DELTA_INSERT_CIERREN.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_ID_ARCHIVO=params.sr_id_archivo,
)

# Insertar datos en CIERREN usando write_data
db.write_data(
    db.sql_delta(statement_007_insert),
    f"{conf.CX_CRN_ESQUEMA}.{conf.TL_CRN_VAL_CIFRAS_CONTROL}",
    "default",
    "append",
)


# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
