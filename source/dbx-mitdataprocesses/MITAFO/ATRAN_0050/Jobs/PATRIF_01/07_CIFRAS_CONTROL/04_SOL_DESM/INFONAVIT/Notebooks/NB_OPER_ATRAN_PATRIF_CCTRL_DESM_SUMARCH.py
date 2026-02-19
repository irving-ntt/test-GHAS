# Databricks notebook source
"""
Descripcion:
    Extrae la información de Solicitud de Desmarca para obtención de Cifras Control e inserción a Sumario Archivo
Subetapa: 
    26 - Cifras Control
Trámite:
    347 - Desmarca de crédito de vivienda por 43 BIS
Tablas input:
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
Tablas output:
    TTAFOTRAS_SUM_ARCHIVO_TRANS
Tablas Delta:
    N/A
Archivos SQL:
    100_DESM_SUMARCH.sql
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

# Se elimina información por folio de la tabla histórica
delete_statement = f"DELETE FROM CIERREN.THAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

# Se elimina información por folio de la tabla SUM_ARCHIVO_TRANS
delete_statement = f"DELETE FROM CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

statement_001 = query.get_statement(
    "100_DESM_SUMARCH.sql",
    SR_FOLIO=params.sr_folio,
    SR_PROCESO = params.sr_proceso,
    SR_USUARIO = params.sr_usuario,
    SR_SUBPROCESO = params.sr_subproceso
)

table_001 = "TTAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.read_data("default",statement_001), f"{table_001}", "default", "append")

# COMMAND ----------

# Se inserta información en tabla de histórica
table_002 = "THAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db.read_data("default",statement_001), f"{table_002}", "default", "append")
