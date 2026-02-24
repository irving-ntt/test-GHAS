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

delete_statement = f"DELETE FROM CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.THAFOGRAL_VAL_CIFRAS_CTRL_RESP WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement_001 = query.get_statement(
    "100_DEV_EXC_700_FOVISSTE.sql",
    SR_FOLIO=params.sr_folio,
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
    USUARIO=params.sr_usuario,
)

if conf.debug:
    df = db.read_data("default",statement_001)
    display(df)

table_001 = "TLAFOGRAL_VAL_CIFRAS_CTRL_RESP"
db.write_data(db.read_data("default",statement_001), f"{table_001}", "default", "append")

# COMMAND ----------

table_002 = "THAFOGRAL_VAL_CIFRAS_CTRL_RESP"
db.write_data(db.read_data("default",statement_001), f"{table_002}", "default", "append")
