# Databricks notebook source
"""
Descripcion:
    Job paralelo que realiza las transformaciones para de insertar a Sumario Archivo
Subetapa: 
    26 - Cifras Control
Trámite:
    3286 - Transferencia de Recursos por portabilidad
Tablas input:
    N/A
Tablas output:
    CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS
Tablas Delta:
    DELTA_JO_400_{params.sr_folio}
Archivos SQL:
    TF_600_SUMARC.sql
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
    "sr_tipo_layout":str,
})
# Validar widgets
# params.validate()

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

statement = query.get_statement(
    "TF_600_SUMARC.sql",
    SR_FOLIO=params.sr_folio,
    USUARIO= params.sr_usuario,
    DELTA_JO_400 = f"DELTA_JO_400_{params.sr_folio}"
)

# COMMAND ----------

if conf.debug:
    display(db.sql_delta(query=statement))

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

table_001 = "TTAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.sql_delta(query=statement), f"{table_001}", "default", "append")

# COMMAND ----------

# Se inserta información en tabla de histórica
table_002 = "THAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.sql_delta(query=statement), f"{table_002}", "default", "append")
