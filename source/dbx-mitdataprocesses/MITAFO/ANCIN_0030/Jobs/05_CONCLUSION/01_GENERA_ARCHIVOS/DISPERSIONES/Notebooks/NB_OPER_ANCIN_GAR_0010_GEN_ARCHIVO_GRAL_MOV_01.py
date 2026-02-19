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
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("GAR_0010_GEN_ARCH")))

# COMMAND ----------

# DBTITLE 1,Construcción de Consulta de Movimientos
statement_001 = query.get_statement(
    "GAR_0010_GEN_ARCH_CTINDI_MOVIMIENTOS_001.sql",
    SR_FOLIO=params.sr_folio,
    SR_FECHA_LOTE=params.sr_fecha_lote,
    SR_SEC_LOTE=params.sr_sec_lote,
)

# COMMAND ----------

# DBTITLE 1,Extracción de información de Mov
df = db.read_data("default", statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,Construcción y llenado de tabla delta
temp_view = 'DELTA_DOIMSS_CTI_01_MOVIMIENTOS' + '_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# DBTITLE 1,Fin - Limpieza
CleanUpManager.cleanup_notebook(locals())