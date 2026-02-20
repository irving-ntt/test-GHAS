# Databricks notebook source
'''
Descripcion:
    Proceso que realiza el proceso de inserción de las tabla TLAFOGRAL_VAL_CIFRAS_CTRL_RESP.
Subetapa:
    26 - Cifras Control
Trámite:
    364 - Transferencias de Acreditados Infonavit
    365 - Transferencias por Anualidad Garantizada
    368 - Uso de Garantía por 43 BIS
Tablas input:
    N/A
Tablas output:
    TLAFOGRAL_VAL_CIFRAS_CTRL_RESP
Tablas delta:
    N/A
Archivos SQL:
    TRANS_200_TI_CCTRL.sql
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
    "TRANS_200_TI_CCTRL.sql",
    DELTA_200_TI_JOIN=f"DELTA_TRANS_200_TI_JOIN_{params.sr_id_archivo}",
    SR_FOLIO=params.sr_folio,
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
    SR_USER=params.sr_usuario
)

# COMMAND ----------

if conf.debug:
    display(db.sql_delta(statement))

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

# Se elimina información por folio de la tabla histórica
delete_statement = f"DELETE FROM CIERREN.THAFOGRAL_VAL_CIFRAS_CTRL_RESP WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

table_001 = "TLAFOGRAL_VAL_CIFRAS_CTRL_RESP"
db.write_data(db.sql_delta(statement), f"{table_001}", "default", "append")

# COMMAND ----------

# Se inserta información en tabla de histórica
table_002 = "THAFOGRAL_VAL_CIFRAS_CTRL_RESP"
db.write_data(db.sql_delta(statement), f"{table_002}", "default", "append")

