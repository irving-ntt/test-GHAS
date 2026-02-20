# Databricks notebook source
# DBTITLE 1,Inicio - Configuraciones Principales
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_folio": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_subetapa": str,
        "sr_sec_lote": str,
        "sr_fecha_lote": str,
        "sr_fecha_acc": str,
        "sr_tipo_archivo": str,
        "sr_estatus_mov": str,
        "sr_tipo_mov": str,
        "sr_accion": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
    }
)
#params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("GAR_0030_GEN_ARCH_CTINDI")))

# COMMAND ----------

# DBTITLE 1,Construcción SQL Sua
statement_001 = query.get_statement(
    "GAR_0030_GEN_ARCH_CTINDI_001.sql",
    SR_FOLIO=params.sr_folio,
    SR_USUARIO=params.sr_usuario,
)

# COMMAND ----------

# DBTITLE 1,Construcción y Llenado Tabla Delta Sua
temp_view = 'DELTA_TLSISGRAL_ETL_CLIENTE_SUA' + '_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# DBTITLE 1,Construcción SQL Movimientos
statement_002 = query.get_statement(
    "GAR_0030_GEN_ARCH_CTINDI_002.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Construcción y Llenado Tabla Delta Mov
temp_view = 'DELTA_TTSISGRAL_ETL_MOVIMIENTOS' + '_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_002), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# DBTITLE 1,Construcción Inner Join SQL Sua y Mov
statement_003 = query.get_statement(
    "GAR_0030_GEN_ARCH_CTINDI_003.sql",
    SR_FOLIO=params.sr_folio,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# COMMAND ----------

# DBTITLE 1,Ejecución de Consulta Sua y Mov
df_final = db.sql_delta(statement_003)
if conf.debug:
    display(df_final)
    display(str(df_final.count()) + " Registros Totales")

# COMMAND ----------

# DBTITLE 1,Inserción de datos en tabla OCI
table_name = "CIERREN.TNAFORECA_SUA"

# borrado = query.get_statement(
#     "DELETE.sql",
#     SR_FOLIO=params.sr_folio,
#     table_name=table_name,
#     hints="/*+ PARALLEL(8) */",
# )

# execution = db.execute_oci_dml(
#     statement=borrado, async_mode=False
# )

db.write_data(df_final, table_name, "default", "append")


# COMMAND ----------

# DBTITLE 1,Fin - Depuración de Dataframes usados
CleanUpManager.cleanup_notebook(locals())
