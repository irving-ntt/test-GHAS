# Databricks notebook source
"""
Descripcion:
    Realiza los calculos para insertar en la tabla final de CTRL RESP.
Subetapa: 
    26 - Cifras Control
Trámite:
    3832 - Devolución de pago sin Justificación Legal
Tablas input:
    N/A
Tablas output:
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP
Tablas Delta:
    
Archivos SQL:
    400_TL_PRO_DEV_PAG_SJL.sql
    200_TOT_DPSJL_JOIN_CONTEOS.sql
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

# Se elimina información por folio de la tabla histórica
delete_statement = f"DELETE FROM CIERREN.THAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
)

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "200_TOT_DPSJL_SUMARCH.sql",
    DELTA_TRANSFORMADA=f"DELTA_CONTEOS_{params.sr_folio}",
    SR_USUARIO=params.sr_usuario,
)

# COMMAND ----------

table_001 = "TTAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.sql_delta(statement), f"{table_001}", "default", "append")

# COMMAND ----------

table_002 = "THAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.sql_delta(statement), f"{table_002}", "default", "append")

# COMMAND ----------

if conf.debug:
    display(db.sql_delta(statement))
