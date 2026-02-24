# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta, finalmente inserta en OCI el resultado
Subetapa: 
    25 - Matriz de Convivencia
Trámite:
   348 -Devolución de Excedentes por 43 BIS
   349 -Devolución de Saldos Excedentes por T.A.A.G.
Tablas input:
    PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO
    PROCESOS.TTSISGRAL_SUF_SALDOS
    CIERREN.TTAFOGRAL_BALANCE_MOVS
    CIERREN.TCCRXGRAL_TIPO_SUBCTA SUBCTA
    CIERREN.TCAFOGRAL_VALOR_ACCION
Tablas output:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
Tablas Delta:
    DELTA_TRA_MCV_SOL_DM_01_{params.sr_folio}
    
Archivos SQL:
    TRA_MCV_TRANSFERENCIAS_0200_DEL_PRE_MATRIZ.sql
  

"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_reproceso": int,
    "sr_etapa": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_MCV_TRP_0100_EXT_INFO.sql",
    sr_folio=params.sr_folio,
    sr_subproceso=params.sr_subproceso,
    sr_proceso=params.sr_proceso,
)

df = db.read_data("default", statement)

if conf.debug:
    display(df)

db.write_delta(f"DELTA_TRA_MCV_TRP_{params.sr_folio}", db.read_data("default", statement), "overwrite")

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN PRE MATRIZ
statement = query.get_statement(
    "TRA_MCV_TRP_0200_DEL_PRE_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
   sr_subproceso=params.sr_subproceso,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

table_name = "CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ"

db.write_data(db.read_delta(f"DELTA_TRA_MCV_TRP_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

db.drop_delta(f"DELTA_TRA_MCV_TRP_{params.sr_folio}")

# COMMAND ----------

Notify.send_notification("INFO", params)
