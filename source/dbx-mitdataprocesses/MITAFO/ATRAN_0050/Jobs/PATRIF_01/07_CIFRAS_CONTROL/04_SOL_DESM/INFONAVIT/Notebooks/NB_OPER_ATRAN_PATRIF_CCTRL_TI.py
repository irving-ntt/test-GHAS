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
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_recalculo": str,
    "p_tipo_arch": str,
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
    "TRA_MCV_SOL_MARCA_0100_EXT_INFO.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_MCV_SOL_MCA_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_MCV_SOL_MCA_01_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN PRE MATRIZ
statement = query.get_statement(
    "TRA_MCV_SOL_MARCA_0200_DEL_PRE_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

#INSERTA EN PRE MATRIZ

table_name = "CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ"

db.write_data(db.read_delta(f"DELTA_TRA_MCV_SOL_MCA_01_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

db.drop_delta(f"DELTA_TRA_MCV_SOL_MCA_01_{params.sr_folio}")

# COMMAND ----------


