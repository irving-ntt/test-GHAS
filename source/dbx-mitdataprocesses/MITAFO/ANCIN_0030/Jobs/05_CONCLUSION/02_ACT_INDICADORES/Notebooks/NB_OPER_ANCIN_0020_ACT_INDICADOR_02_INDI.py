# Databricks notebook source
# DBTITLE 1,Descripción General
'''
Descripcion:
    Une la información de dos tablas delta, la tabla que se genera en la subetapa de Movimientos y la tabla generada en el paso anterior. 
Subetapa:
    ACTUALIZACIÓN DE INDICADORES
Trámite:
      8 - DISPERSION ORDINARIA IMSS
    439 - GUBERNAMENTAL IMSS
    121 - INTERESES EN TRÁNSITO IMSS
    118 - ACLARACIONES ESPECIALES
    120 - DISPERSIÓN ORDINARIA ISSSTE
    210 - GUBERNAMENTAL ISSSTE
    122 - INTERESES EN TRÁNSITO ISSSTE
Tablas INPUT:
    N/A
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    ORACLE_DISPERSIONES
    DELTA_DISPER_ACT_IND_01
Tablas OUTPUT DELTA:
    DELTA_DISPER_ACT_IND_02
Archivos SQL:
    0020_ACT_INDICADOR_002.sql
'''

# COMMAND ----------

# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Párametros
params = WidgetParams(
    {
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_folio": str,
        "sr_dt_org_arc": str,
        "sr_origen_arc": str,
        "sr_tipo_mov": str,
        "sr_fecha_acc": str,
        "sr_accion": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
        "sr_tipo_archivo": str,
        "sr_estatus_mov": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("0020_ACT_INDICADOR")))

# COMMAND ----------

# DBTITLE 1,Construcción consulta principal
statement_001 = query.get_statement(
    "0020_ACT_INDICADOR_002.sql",
    SR_FOLIO=params.sr_folio,
    CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
    SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
)

# COMMAND ----------

# DBTITLE 1,Extracción de información
df_resultado = db.sql_delta(statement_001)
display(df_resultado)

# COMMAND ----------

# DBTITLE 1,Creación de tabla Delta
temp_delta = 'DELTA_DISPER_ACT_IND_02' + '_' + params.sr_folio
db.write_delta(temp_delta, df_resultado, "overwrite")
if conf.debug:
    display(db.read_delta(temp_delta))

# COMMAND ----------

# DBTITLE 1,Fin
#Depuración de dataframes usados en el notebook
CleanUpManager.cleanup_notebook(locals())
