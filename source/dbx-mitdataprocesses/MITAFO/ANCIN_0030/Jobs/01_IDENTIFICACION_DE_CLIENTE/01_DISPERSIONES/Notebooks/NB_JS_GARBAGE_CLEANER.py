# Databricks notebook source
# DBTITLE 1,Inicio
# MAGIC %run "./startup" 

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_origen_arc": str,
    "sr_dt_org_arc": str,
    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
    "var_tramite": str,
})
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Listado de tablas delta
sr_id_archivo = params.sr_id_archivo
query_str = f"""
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE table_name LIKE '%{sr_id_archivo}%'
"""
tablas_delta = spark.sql(query_str)
display(tablas_delta)

# COMMAND ----------

# DBTITLE 1,Eliminación de tablas delta
# Itera sobre las filas del DataFrame que contiene las tablas encontradas
for row in tablas_delta.collect():
    table_name = row['table_name']
    try:
        # Elimina la tabla Delta usando el método personalizado
        db.drop_delta(table_name)
    except Exception as e:
        # Captura y muestra cualquier error ocurrido durante la eliminación
        print(f"Error al eliminar la tabla {table_name}: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Fin
CleanUpManager.cleanup_notebook(locals())
