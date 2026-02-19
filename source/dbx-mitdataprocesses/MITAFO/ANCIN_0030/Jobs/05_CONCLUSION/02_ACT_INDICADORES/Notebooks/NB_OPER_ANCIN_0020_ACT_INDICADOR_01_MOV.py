# Databricks notebook source
# DBTITLE 1,Descripción General
'''
Descripcion:
    Realiza la extracción de los movimientos y los valores para FFN_ID_CONFIG_INDI = 6, 13 y 16. 
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
    CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS
    CIERREN.TTAFOGRAL_IND_CTA_INDV
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    DELTA_DISPER_ACT_IND_01
Archivos SQL:
    0020_ACT_INDICADOR_001.sql
'''

# COMMAND ----------

# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
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

# DBTITLE 1,Construcción de consulta principal
statement_001 = query.get_statement(
    "0020_ACT_INDICADOR_001.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Extracción de información
#df = db.read_data("default", statement_001)
#df.cache()
#if conf.debug:
#    display(df)

# COMMAND ----------

# DBTITLE 1,Creación de tabla delta
temp_delta = 'DELTA_DISPER_ACT_IND_01' + '_' + params.sr_folio
db.write_delta(temp_delta, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_delta))

# COMMAND ----------

# DBTITLE 1,Fin
#Depuración de dataframes usados en el notebook
CleanUpManager.cleanup_notebook(locals())
