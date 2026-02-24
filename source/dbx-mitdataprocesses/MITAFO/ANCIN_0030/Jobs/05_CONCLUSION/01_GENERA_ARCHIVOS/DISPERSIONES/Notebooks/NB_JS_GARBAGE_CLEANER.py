# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

conf = ConfManager()

# COMMAND ----------

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
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str, # es obligatorio para servicios
    }
)

# Validar widgets
params.validate()

# COMMAND ----------

from pyspark.sql.functions import col

Tablas_Persistentes = [f"oracle_dispersiones_{params.sr_folio.lower()}"]

# List delta tables with the specified folio
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("delta")) \
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
