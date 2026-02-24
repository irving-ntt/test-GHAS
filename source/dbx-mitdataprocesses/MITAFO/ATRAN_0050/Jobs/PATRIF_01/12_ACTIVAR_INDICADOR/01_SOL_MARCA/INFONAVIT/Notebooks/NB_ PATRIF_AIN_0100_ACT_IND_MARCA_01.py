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

# MAGIC %md
# MAGIC Inicia el paso 1 TO INSERT

# COMMAND ----------

# DBTITLE 1,Genera la data de los registros que seran insertados con vigencias prendidas
#Query Extrae informacion 

statement = query.get_statement(
    "PATRIF_AIN_0100_ACT_IND_MARCA_SELECT_001.sql",
    sr_folio=params.sr_folio,
    sr_subproceso=params.sr_subproceso,
    ind_vigencia=params.ind_vigencia,
    config_indi=params.config_indi
)

db.write_delta(f"DELTA_TRA_SELECT_001_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_SELECT_001_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Revisa data generada en el paso anterior
# Cargar tabla Delta en un DataFrame
df = spark.read.format("delta").table(f"DELTA_TRA_SELECT_001_{params.sr_folio}")

# Mostrar los primeros 100 registros
df.show(100)

# COMMAND ----------

# DBTITLE 1,Inserta registros a la tabla de indicadores de cuenta Individual
#INSERTA EN LA TABLA 

table_name = "CIERREN.TTAFOGRAL_IND_CTA_INDV"

db.write_data(db.read_delta(f"DELTA_TRA_SELECT_001_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC Inicia el segundo paso Que genera la data para actualizar la vigencia de los Indicadores que no procedan

# COMMAND ----------

# DBTITLE 1,iNICIA EL SEGUNDO PASO
#Query Extrae informacion 

statement = query.get_statement(
    "PATRIF_AIN_0100_ACT_IND_MARCA_UPDATE_001.sql",
    sr_folio=params.sr_folio,
    sr_subproceso=params.sr_subproceso,
    ind_vigencia=params.ind_vigencia,
    config_indi=params.config_indi
)

db.write_delta(f"DELTA_TRA_MARCA_UPDATE_001_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_MARCA_UPDATE_001_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Inserta datos de vigencias apagadas a tabla auxiliar
#INSERTA EN LA TABLA AUXILIAR

table_name = "CIERREN_DATAUX.TTAFOGRAL_IND_CTA_INDV_AUX"

db.write_data(db.read_delta(f"DELTA_TRA_MARCA_UPDATE_001_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# DBTITLE 1,Actualiza los Indicadores que se apagara vigencia
#Se ejecuta el merge


statement = query.get_statement(
    "PATRIF_AIN_0100_ACT_IND_MARCA_MERGE.sql",
    sr_folio=params.sr_folio,
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Borra contenido tabla Auxiliar
#elimiar los datos de la tabla auxiliar 
statement = query.get_statement(
    "PATRIF_AIN_0100_ACT_IND_MARCA_DELETE_AUX.sql",
    sr_folio=params.sr_folio,
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Borra Tanlas Delta
#Se elimina la Delta para liberar espacio
spark.sql(f"DROP TABLE IF EXISTS DELTA_TRA_SELECT_001_{params.sr_folio}")
spark.sql(f"DROP TABLE IF EXISTS DELTA_TRA_MARCA_UPDATE_001_{params.sr_folio}")


# COMMAND ----------

# DBTITLE 1,Depuración de Dataframes usados
CleanUpManager.cleanup_notebook(locals())
