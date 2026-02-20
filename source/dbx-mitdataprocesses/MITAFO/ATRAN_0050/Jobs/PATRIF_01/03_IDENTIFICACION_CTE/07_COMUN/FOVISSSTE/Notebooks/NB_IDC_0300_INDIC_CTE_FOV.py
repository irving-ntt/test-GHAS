# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Genera datos complementarios a partir de la consulta inicial
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC Tablas INPUT:
# MAGIC     CIERREN_ETL.TTAFOTRAS_ETL_MARCA_DESMARCA
# MAGIC 	CIERREN_ETL.TLSISGRAL_ETL_BUC
# MAGIC Tablas OUTPUT:
# MAGIC     N/A
# MAGIC Tablas INPUT DELTA:
# MAGIC     DELTA_TRAMITE
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     DELTA_FECHA
# MAGIC 	DELTA_VIVIENDA
# MAGIC 	DELTA_VIGENTES
# MAGIC 	DELTA_NO_VIGENTES
# MAGIC 	DELTA_NO_ENCONTRADOS
# MAGIC Archivos SQL:
# MAGIC     SQL_IDC_300_NCI_INDI.sql
# MAGIC

# COMMAND ----------

# DBTITLE 1,INICIO - CONFIGURACIONES PRINCIPALES
# MAGIC %run "./startup" 

# COMMAND ----------

# DBTITLE 1,PARAMETROS
# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({

    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_proceso": str,
    "sr_subetapa": str,
    "sr_dt_org_arc": str,
    "sr_origen_arc": str,
    "sr_subproceso": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
})

# Validar widgets
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()


# COMMAND ----------

# DBTITLE 1,VALIDA EXISTENCIA SQL
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
if conf.debug:
    display(
        queries_df.filter(
            col("Archivo SQL").startswith("SQL_IDC_300")
        )
    )

# COMMAND ----------

# DBTITLE 1,EJECUTA QUERY PARA GENERACION DE DELTA NCI
SUBPROCESO = dbutils.widgets.get("sr_subproceso")

if SUBPROCESO == '363':
    Query = "SQL_IDC_300_NCI_INDI.sql"
elif SUBPROCESO == '350':
    Query = "SQL_IDC_300_NCI_DSEF.sql"
elif SUBPROCESO == '3283':
    Query = "SQL_IDC_300_NCI_SDDF.sql"


statement_001 = query.get_statement(
Query,
#"SQL_IDC_300_NCI_INDI.sql",
SR_FOLIO=params.sr_folio,
SR_ID_ARCHIVO=params.sr_id_archivo,
hints="/*+ PARALLEL(4) */"
)

db.write_delta(f"DELTA_NCI_{params.sr_folio}", db.read_data("default", statement_001), "overwrite")




# COMMAND ----------

# DBTITLE 1,GENERA DELTA FECHA
statement_2 = query.get_statement(
    "SQL_DELTA_NCI_001.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_NCI_{params.sr_folio}"
)

df_fecha = db.sql_delta(statement_2)

if conf.debug:
    display(df_fecha)

db.write_delta(f"DELTA_FECHA_{params.sr_folio}", df_fecha, "overwrite")


# COMMAND ----------

# DBTITLE 1,GENERA DELTA VIVIENDA
statement_3 = query.get_statement(
    "SQL_DELTA_NCI_002.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_NCI_{params.sr_folio}"
)

df_vivienda = db.sql_delta(statement_3)

if conf.debug:
    display(df_vivienda)

db.write_delta(f"DELTA_VIVIENDA_{params.sr_folio}", df_vivienda, "overwrite")


# COMMAND ----------

# DBTITLE 1,GENERA DELTA DERECHA
statement_4 = query.get_statement(
    "SQL_DELTA_NCI_003.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_NCI_{params.sr_folio}"
)

df_derecha = db.sql_delta(statement_4)

if conf.debug:
    display(df_derecha)

db.write_delta(f"DELTA_DERECHA_{params.sr_folio}", df_derecha, "overwrite")


# COMMAND ----------

# DBTITLE 1,GENERA DELTA IZQUIERDA
statement_5 = query.get_statement(
    "SQL_DELTA_111_TRAMITE.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_111_TRAMITE_{params.sr_folio}"
)

df_izquierda = db.sql_delta(statement_5)

if conf.debug:
    display(df_izquierda)

db.write_delta(f"DELTA_IZQUIERDA_{params.sr_folio}", df_izquierda, "overwrite")


# COMMAND ----------

# DBTITLE 0,APLICA EL JOIN ENTRE IZQUIERDA Y DERECHA
statement_6 = query.get_statement(
    "SQL_JOIN_300_DER_IZQ.sql",
    SR_FOLIO=params.sr_folio,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    DELTA_IZQUIERDA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_IZQUIERDA_{params.sr_folio}",
    DELTA_DERECHA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_DERECHA_{params.sr_folio}"
)

df_join_der_izq = db.sql_delta(statement_6)

if conf.debug:
    display(df_join_der_izq)

db.write_delta(f"DELTA_JOIN_300_DER_IZQ_{params.sr_folio}", df_join_der_izq, "overwrite")

# COMMAND ----------

# DBTITLE 1,GENERA DELTA VIGENTES
statement_7 = query.get_statement(
    "SQL_DELTA_VIGENTES.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_JOIN_300_DER_IZQ_{params.sr_folio}"
)

df_vigentes = db.sql_delta(statement_7)

if conf.debug:
    display(df_vigentes)

db.write_delta(f"DELTA_VIGENTES_{params.sr_folio}", df_vigentes, "overwrite")

# COMMAND ----------

# DBTITLE 1,GENERA DELTA NO VIGENTES
statement_8 = query.get_statement(
    "SQL_DELTA_NO_VIGENTES.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_JOIN_300_DER_IZQ_{params.sr_folio}"
)

df_no_vigentes = db.sql_delta(statement_8)

if conf.debug:
    display(df_no_vigentes)

db.write_delta(f"DELTA_NO_VIGENTES_{params.sr_folio}", df_no_vigentes, "overwrite")

# COMMAND ----------

# DBTITLE 1,GENERA DELTA RECHAZADOS VIGENCIA
statement_9 = query.get_statement(
    "SQL_DELTA_VIG_REJ.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_JOIN_300_DER_IZQ_{params.sr_folio}"
)

df_vig_rej = db.sql_delta(statement_9)

if conf.debug:
    display(df_vig_rej)


# COMMAND ----------

# DBTITLE 1,GENERA DELTA RECHAZADOS TRAMITE
statement_10 = query.get_statement(
    "SQL_DELTA_111_TRAMITE_2.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_111_TRAMITE_{params.sr_folio}"
)

df_tramite_rej = db.sql_delta(statement_10)

if conf.debug:
    display(df_tramite_rej)


# COMMAND ----------

# DBTITLE 1,GENERA DELTA NO_ENCONTRADOS
df_rechazados = df_vig_rej.union(df_tramite_rej)

if conf.debug:
    display(df_rechazados)

db.write_delta(f"DELTA_NO_ENCONTRADOS_{params.sr_folio}", df_rechazados, "overwrite")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
