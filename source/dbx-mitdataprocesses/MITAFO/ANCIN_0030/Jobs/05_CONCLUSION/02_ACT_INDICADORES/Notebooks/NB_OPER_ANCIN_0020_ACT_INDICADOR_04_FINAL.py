# Databricks notebook source
# DBTITLE 1,Descripción General
'''
Descripcion:
    Depuración de tablas delta y auxiliares utilizadas durante el flujo. 
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
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    N/A
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

# DBTITLE 1,Construcción de sentencia delete Aux
statement_003 = query.get_statement(
    "0020_ACT_INDICADOR_005.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecución de delete Aux
#La ejecucion se envia en segundo plano con async_mode=True
execution = db.execute_oci_dml(
    statement=statement_003, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Fin - Depuración de tablas delta usadas
from pyspark.sql.functions import col

Tablas_Persistentes = [f"oracle_dispersiones_{params.sr_folio}"]

# List delta tables with the specified folio
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("delta_disper")) \
    .filter(col("tableName").endswith(params.sr_folio)) \
    .filter(~col("tableName").isin(Tablas_Persistentes))

display(tables)

# Drop the tables with the specified folio
for row in tables.collect():
    table_name = row.tableName
    try: 
        spark.sql(f"DROP TABLE IF EXISTS {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{table_name}")
        print(f"tabla {table_name} borrada")
    except:
        print(f"Error al borrar: {table_name} - Permisos insuficientes") 


# Forzar la recolección de basura
import gc
gc.collect()

print("Proceso de eliminación de tablas y liberación de memoria completado.")
