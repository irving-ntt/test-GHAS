# Databricks notebook source
# MAGIC %run "./startup" 

# COMMAND ----------

params = WidgetParams({
    "sr_folio": str,    
    "sr_subproceso": str,
    "ind_vigencia": str,
    "config_indi": str
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

#Query Extrae informacion 

statement = query.get_statement(
    "PATRIF_AIN_0100_ACT_IND_TRANF_SELECT_001.sql",
    sr_folio=params.sr_folio,
    sr_subproceso=params.sr_subproceso,
    ind_vigencia=params.ind_vigencia,
    config_indi=params.config_indi
)

db.write_delta(f"DELTA_TRA_SELECT_001_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_SELECT_001_{params.sr_folio}"))

# COMMAND ----------

# Cargar tabla Delta en un DataFrame
df = spark.read.format("delta").table(f"DELTA_TRA_SELECT_001_{params.sr_folio}")

# Mostrar los primeros 100 registros
df.show(100)

# COMMAND ----------

#INSERTA EN LA TABLA 

table_name = "CIERREN_DATAUX.TTAFOGRAL_ETL_IND_CTA_INDV_AUX"

db.write_data(db.read_delta(f"DELTA_TRA_SELECT_001_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

#Se ejecuta el merge


statement = query.get_statement(
    "PATRIF_AIN_0100_ACT_IND_TRANF_MERGE.sql",
    sr_folio=params.sr_folio,
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

#elimiar los datos de la tabla auxiliar 
statement = query.get_statement(
    "PATRIF_AIN_0100_ACT_IND_TRANF_DELETE_AUX.sql",
    sr_folio=params.sr_folio,
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC Inicia el segundo paso 

# COMMAND ----------

# DBTITLE 1,INICIA EL SEGUNDO PASO
#Query Extrae informacion 

statement = query.get_statement(
    "PATRIF_AIN_0100_ACT_IND_TRANF_SELECT_002.sql",
    sr_folio=params.sr_folio,
    sr_subproceso=params.sr_subproceso,
    ind_vigencia=params.ind_vigencia,
    config_indi=params.config_indi
)

db.write_delta(f"DELTA_TRA_SELECT_002_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_SELECT_002_{params.sr_folio}"))

# COMMAND ----------

#INSERTA EN LA TABLA 

table_name = "CIERREN.TTAFOGRAL_IND_CTA_INDV"

db.write_data(db.read_delta(f"DELTA_TRA_SELECT_002_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS DELTA_TRA_SELECT_001_{params.sr_folio}")
spark.sql(f"DROP TABLE IF EXISTS DELTA_TRA_SELECT_002_{params.sr_folio}")
