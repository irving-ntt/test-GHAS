# Databricks notebook source
'''
Descripcion:
    Proceso que realiza el proceso de inserción de las tabla TTAFOTRAS_SUM_ARCHIVO_TRANS.
Subetapa:
    26 - Cifras Control
Trámite:
    364 - Transferencias de Acreditados Infonavit
    365 - Transferencias por Anualidad Garantizada
    368 - Uso de Garantía por 43 BIS
Tablas input:
    N/A
Tablas output:
    TTAFOTRAS_SUM_ARCHIVO_TRANS
Tablas delta:
    N/A
Archivos SQL:
    TRANS_200_TI_TRANS_SUMARCH.sql
'''

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

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRANS_200_TI_TRANS_SUMARCH.sql",
    SR_FOLIO = params.sr_folio,
    SR_USER = params.sr_usuario,
    DELTA_200_TI_JOIN=f"DELTA_TRANS_200_TI_JOIN_{params.sr_id_archivo}"
)

# COMMAND ----------

if conf.debug:
    display(db.sql_delta(statement))

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

# Se elimina información por folio de la tabla histórica
delete_statement = f"DELETE FROM CIERREN.THAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

table_001 = "TTAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.sql_delta(statement), f"{table_001}", "default", "append")

# COMMAND ----------

table_002 = "THAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.sql_delta(statement), f"{table_002}", "default", "append")
