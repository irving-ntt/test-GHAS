# Databricks notebook source
"""
Descripcion:
    
Subetapa: 
    26 - Cifras Control
Trámite:
    354 - IMSS Solicitud de marca de cuentas por 43 bis
Tablas input:

Tablas output:
    
Tablas Delta:
        
Archivos SQL:
    
"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso":str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_etapa":str,
    "sr_id_archivo": str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_id_snapshot":str,
    "sr_recalculo":str,
    "sr_tipo_archivo":str,
})
# Validar widgets
params.validate()

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "100_DEVO_15_348_3.sql",
    SR_USUARIO=params.sr_usuario,
    SR_SUBPROCESO=params.sr_subproceso,
    SR_FOLIO=params.sr_folio,
)

table_800 = "CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.read_data("default",statement), f"{table_800}", "default", "append")

# COMMAND ----------

table_002 = "THAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.read_data("default",statement), f"{table_002}", "default", "append")
