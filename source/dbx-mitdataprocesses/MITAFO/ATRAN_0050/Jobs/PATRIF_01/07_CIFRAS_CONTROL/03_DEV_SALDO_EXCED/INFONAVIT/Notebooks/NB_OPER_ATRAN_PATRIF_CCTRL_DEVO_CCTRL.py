# Databricks notebook source
"""
Descripcion:
    Extrae la información de Devolución de Saldos Excedentes para la inserción en Cifras Control
Subetapa: 
    26 - Cifras Control
Trámite:
    348 - IMSS Devolución de Excedentes por 43 BIS
Tablas input:

Tablas output:
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP
Tablas Delta:
    N/A
Archivos SQL:
    100_DEVO_15_348_2.sql
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

statement = query.get_statement(
    "100_DEVO_15_348_2.sql",
    SR_USUARIO=params.sr_usuario,
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
    SR_FOLIO=params.sr_folio,
)

table_800 = "CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP"
df = db.read_data("default",statement)
if conf.debug:
    display(df)

# COMMAND ----------

db.write_data(df, f"{table_800}", "default", "append")

# COMMAND ----------

table_002 = "THAFOGRAL_VAL_CIFRAS_CTRL_RESP"
db.write_data(df, f"{table_002}", "default", "append")
