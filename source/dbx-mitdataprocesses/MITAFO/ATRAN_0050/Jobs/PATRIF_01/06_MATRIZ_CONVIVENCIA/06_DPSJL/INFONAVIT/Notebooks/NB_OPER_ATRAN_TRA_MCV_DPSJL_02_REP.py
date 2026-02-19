# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta, finalmente inserta en OCI el resultado
Subetapa: 
    25 - Matriz de Convivencia
Trámite:
    347 -Desmarca de crédito de vivienda por 43 BIS
Tablas input:
    PROCESOS.TTCRXGRAL_DEV_PAG_SJL
   PROCESOS.TTCRXGRAL_VAL_SDOS_DPSJL
   CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV
Tablas output:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
Tablas Delta:
    DELTA_TRA_MCV_SOL_DM_01_{params.sr_folio}
    
Archivos SQL:
    TRA_MCV_SOL_DM_0200_DEL_PRE_MATRIZ.sql
  

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
  "sr_tipo_ejecucion": int,
  "sr_etapa": str,
  "sr_instancia_proceso": str,
  "sr_id_snapshot":str,
  "sr_tipo_mov": str,
  "sr_paso":str,
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
    "TRA_MCV_DPSJL_0400_EXT_INFO_REP_02.sql",
    sr_folio=params.sr_folio,
    sr_subproceso=params.sr_subproceso,
    sr_proceso=params.sr_proceso,
)

db.write_delta(f"DELTA_TRA_MCV_DPSJL_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_MCV_DPSJL_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN PRE MATRIZ
statement = query.get_statement(
    "TRA_MCV_DPSJL_0200_DEL_PRE_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
   sr_subproceso=params.sr_subproceso,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

table_name = "CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ"

db.write_data(db.read_delta(f"DELTA_TRA_MCV_DPSJL_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

db.drop_delta(f"DELTA_TRA_MCV_DPSJL_{params.sr_folio}")

# COMMAND ----------

Notify.send_notification("INFO", params)

# COMMAND ----------


