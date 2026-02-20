# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     BUC Cliente
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC Querys:
# MAGIC     SQL_IDC_200_BUC_354.sql
# MAGIC     SQL_JOIN_200_354.sql
# MAGIC Deltas:
# MAGIC     DELTA_200_BUC_354
# MAGIC     DELTA_111_TRAMITE
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
            col("Archivo SQL").startswith("SQL_IDC_200")
        )
    )

# COMMAND ----------

# DBTITLE 1,VERIFICA SUBPROCESO ACTIVO
SUBPROCESO = dbutils.widgets.get("sr_subproceso")

if SUBPROCESO == '354':
    QueryIni = "SQL_IDC_200_BUC_354.sql"
    DeltaQry = "DELTA_200_BUC_354"
    DeltaIni = "DELTA_102_MARCA_43BIS"
    QJoin = "SQL_JOIN_200_354.sql"
elif SUBPROCESO == '364' or SUBPROCESO == '365' or SUBPROCESO == '368':
    QueryIni = "SQL_IDC_200_BUC_TRA.sql"
    DeltaQry = "DELTA_200_BUC_TRA"
    DeltaIni = "DELTA_300_TRANSFERENCIA"
    QJoin = "SQL_JOIN_200_TRA.sql"
elif SUBPROCESO == '3282':
    QueryIni = "SQL_IDC_200_BUC_DPSJL.sql"
    DeltaQry = "DELTA_200_DPSJL"
    DeltaIni = "DELTA_300_DPSJL"
    QJoin = "SQL_JOIN_200_DPSJL.sql"

if conf.debug:
    display("QI: " + QueryIni + " DQ: " + DeltaQry + " DI: " + DeltaIni + " QJ: " + QJoin)

# COMMAND ----------

# DBTITLE 1,CONSTRUCCION DE QUERY
statement_002 = query.get_statement(
    #QueryIni DEPENDE DEL SUBPROCESO,
    QueryIni,
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_002)

if conf.debug:
    display(df)
    

# COMMAND ----------

# DBTITLE 1,LLENADO DE TABLA DELTA TEMP
#Delta  DEPENDE DEL SUBPROCESO,
temp_view = DeltaQry + '_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_002), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# DBTITLE 1,EJECUTA JOIN ENTRE TABLAS DELTA
statement_003 = query.get_statement(
    #QJoin  DEPENDE DEL SUBPROCESO,
    QJoin,
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}"
)

df_union = db.sql_delta(statement_003)

if conf.debug:
    display(df_union)


# COMMAND ----------

# DBTITLE 1,LLENADO DE TABLA DELTA  TRAMITE 354
#db.write_delta(f"DELTA_TRAMITE_354_{params.sr_folio}", df_union, "overwrite")
if SUBPROCESO == '354':
    db.write_delta(f"DELTA_111_TRAMITE_{params.sr_folio}", df_union, "overwrite")
    df_from_delta_001 = db.read_delta(f"DELTA_111_TRAMITE_{params.sr_folio}")
elif SUBPROCESO == '364':
    db.write_delta(f"DELTA_600_TRAMITE_{params.sr_folio}", df_union, "overwrite")
    df_from_delta_001 = db.read_delta(f"DELTA_600_TRAMITE_{params.sr_folio}")
elif SUBPROCESO == '365':
    db.write_delta(f"DELTA_600_TRAMITE_{params.sr_folio}", df_union, "overwrite")
    df_from_delta_001 = db.read_delta(f"DELTA_600_TRAMITE_{params.sr_folio}")  
elif SUBPROCESO == '368':
    db.write_delta(f"DELTA_600_TRAMITE_{params.sr_folio}", df_union, "overwrite")
    df_from_delta_001 = db.read_delta(f"DELTA_600_TRAMITE_{params.sr_folio}")
elif SUBPROCESO == '3282':
    db.write_delta(f"DELTA_600_TRAMITE_{params.sr_folio}", df_union, "overwrite")
    df_from_delta_001 = db.read_delta(f"DELTA_600_TRAMITE_{params.sr_folio}")


if conf.debug:
    display(df_from_delta_001)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
