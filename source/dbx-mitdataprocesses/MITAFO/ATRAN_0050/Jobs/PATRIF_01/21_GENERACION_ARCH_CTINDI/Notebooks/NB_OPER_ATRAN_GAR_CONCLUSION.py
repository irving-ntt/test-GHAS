# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    #"sr_mask_rec_trp": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

conf = ConfManager()
Notify.send_notification("INFO", params)

# COMMAND ----------

# DBTITLE 1,Elimina Tablas DELTA - INCO
from pyspark.sql.functions import col

Tablas_Persistentes = []

# List delta tables with the specified folio
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("delta_ctindi")) \
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
