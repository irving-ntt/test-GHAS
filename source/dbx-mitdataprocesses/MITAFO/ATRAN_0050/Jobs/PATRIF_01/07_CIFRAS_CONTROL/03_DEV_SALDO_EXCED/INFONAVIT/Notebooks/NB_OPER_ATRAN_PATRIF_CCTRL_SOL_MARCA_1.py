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
    "100_MARCA_DESMARCA_INFO_RECH.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    TIPO_ARCH= params.p_tipo_arch,
)

df = db.read_data("default",statement)

# DELTA_TABLE_001 = f"DELTA_100_MARCA_DESMARCA_RECH_{params.sr_folio}"
# db.write_delta(DELTA_TABLE_001,df , "overwrite")

# COMMAND ----------

if conf.debug:
    display(df.columns)
    display(df)

# COMMAND ----------

from pyspark.sql.functions import lit, col

# Agregar campos nuevos
delta_700 = df.withColumn("FTC_USU_ACT", lit(params.sr_usuario))
if conf.debug:
    display(delta_700)

# COMMAND ----------

table_name = "CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS"

db.write_data(delta_700, table_name, "default", "append")

# COMMAND ----------

df_conteo = df.select(["FTC_FOLIO", "FTN_NUM_REG_RECH_PRO"])

DELTA_TABLE_001 = f"DELTA_MARCA_DESMARCA_RECH_CONTEO_{params.sr_folio}"
db.write_delta(DELTA_TABLE_001, df_conteo, "overwrite")
