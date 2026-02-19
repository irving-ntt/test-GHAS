# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_origen_arc": str,
    "sr_dt_org_arc": str,
    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
    "var_tramite": str,
})
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0600_UPD_CTE_PRE")))

# COMMAND ----------

if params.var_tramite == "IMSS" or params.var_tramite == "AEIM":
    p_ind = "3"
elif params.var_tramite == "ISSSTE":
    p_ind = "2"
else:
    raise Exception("Invalid value for var_tramite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primera parte

# COMMAND ----------

statement_001 = query.get_statement(
    "IDC_0600_UPD_CTE_PRE_001.sql",
    p_ind=p_ind,
    sr_folio=params.sr_folio,
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

temp_view = f"temp_df_a_{params.sr_id_archivo}"
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segunda parte

# COMMAND ----------

statement_002 = query.get_statement(
    "IDC_0600_UPD_CTE_PRE_002.sql",
    p_ind=p_ind,
    sr_folio=params.sr_folio,
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

temp_view = f"temp_df_b_{params.sr_id_archivo}"
db.write_delta(temp_view, db.read_data("default", statement_002), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tercera parte

# COMMAND ----------

statement_003 = query.get_statement(
    "IDC_0600_UPD_CTE_PRE_003.sql",
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

temp_view = f"temp_df_c_{params.sr_id_archivo}"
db.write_delta(temp_view, db.read_data("default", statement_003), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parte cuatro

# COMMAND ----------

statement_004 = query.get_statement(
    "IDC_0600_UPD_CTE_PRE_004.sql",
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

temp_view = f"temp_df_d1_{params.sr_id_archivo}"
db.write_delta(temp_view, db.read_data("default", statement_004), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

statement_005 = query.get_statement(
    "IDC_0600_UPD_CTE_PRE_005.sql",
    p_ind=p_ind,
    sr_folio=params.sr_folio,
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

temp_view = f"temp_df_d2_{params.sr_id_archivo}"
db.write_delta(temp_view, db.read_data("default", statement_005), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join entre df_D1 y df_D2

# COMMAND ----------

statement_006 = query.get_statement(
    "IDC_0600_UPD_CTE_PRE_006.sql",
    catalog_name=SETTINGS.GENERAL.CATALOG,
    schema_name=SETTINGS.GENERAL.SCHEMA,
    sr_id_archivo=params.sr_id_archivo
)

# COMMAND ----------

temp_view = f"temp_df_d_{params.sr_id_archivo}"
db.write_delta(temp_view, db.sql_delta(statement_006), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Parte final 

# COMMAND ----------

statement_007 = query.get_statement(
    "IDC_0600_UPD_CTE_PRE_007.sql",
    catalog_name=SETTINGS.GENERAL.CATALOG,
    schema_name=SETTINGS.GENERAL.SCHEMA,
    sr_id_archivo=params.sr_id_archivo
)

# COMMAND ----------

df = db.sql_delta(statement_007)
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,Genera vista TEMP_Siefore01
from pyspark.sql.functions import when, col

#Agrega columna MARCA con la siguiente condición
#If  IsNull(DSLink221.FCN_ID_SIEFORE)  Then 1 Else  0

# Aplicar la lógica para asignar el valor de la columna 'MARCA'
df = df.withColumn("MARCA", when(col("FCN_ID_SIEFORE").isNull(), 1).otherwise(0))

temp_view = 'TEMP_Siefore01' + '_' + params.sr_id_archivo
db.write_delta(temp_view, df, "overwrite")
 
if conf.debug:
    display(df)    

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())