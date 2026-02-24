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
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0300_NCI_CTE")))

# COMMAND ----------

if params.var_tramite == "IMSS" or params.var_tramite == "AEIM":
    p_ind = "3"
elif params.var_tramite == "ISSSTE":
    p_ind = "2"
else:
    raise Exception("Invalid tramite")

statement_001 = query.get_statement(
    "IDC_0300_NCI_CTE_001.sql",
    p_ind=p_ind,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(4) */",
)

df = db.read_data("default", statement_001)
df.cache()

if conf.debug:
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indicador 11 es de cliente pensionado.
# MAGIC ### Indicador 5 es la fecha de apertura de la cuenta individual.
# MAGIC ### Indicador 2 es si la cuenta esta vigente.

# COMMAND ----------

# DBTITLE 1,Genera vista intermedia TEMP_IND_CTA_VIG
df1 = df.filter(col("FFN_ID_CONFIG_INDI") == 2).orderBy(
    "FTN_NUM_CTA_INVDUAL",
    ascending=True,
)
df1 = df1.withColumn("FCC_VALOR_IND", col("FCC_VALOR_IND").cast("int"))

temp_view = "TEMP_IND_CTA_VIG_02" + "_" + params.sr_id_archivo
db.write_delta(temp_view, df1, "overwrite")

if conf.debug:
    display(df1)

del df1

# COMMAND ----------

# DBTITLE 1,Genera vista TEMP_FechaApCtaInd_05
df2 = df.select(
    col("FTN_NUM_CTA_INVDUAL"),
    to_date(col("FCC_VALOR_IND"), "dd/MM/yyyy").alias("FCC_VALOR_IND"),
).filter("FFN_ID_CONFIG_INDI = 5")

temp_view = "TEMP_FechaApCtaInd_05" + "_" + params.sr_id_archivo
db.write_delta(temp_view, df2, "overwrite")

if conf.debug:
    display(df2)

del df2

# COMMAND ----------

# DBTITLE 1,Genera vista TEMP_Pensionado_08
df3 = df.select("FTN_NUM_CTA_INVDUAL", "FCC_VALOR_IND").filter(
    "FFN_ID_CONFIG_INDI = 11"
)

temp_view = "TEMP_Pensionado_11" + "_" + params.sr_id_archivo
db.write_delta(temp_view, df3, "overwrite")

if conf.debug:
    display(df3)

df.unpersist()
del df3, df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Empezamos la version la nueva

# COMMAND ----------

statement_002 = query.get_statement(
    "IDC_0300_NCI_CTE_002.sql",
    CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
    SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
    SR_ID_ARCHIVO=params.sr_id_archivo,
)
DF_TEMP_TRAMITE = db.sql_delta(statement_002)
if conf.debug:
    display(DF_TEMP_TRAMITE)

# COMMAND ----------

DF_TEMP_TRAMITE_ORI = DF_TEMP_TRAMITE
DF_TEMP_TRAMITE = DF_TEMP_TRAMITE.filter(DF_TEMP_TRAMITE.FTC_BANDERA.isNotNull()).orderBy("FTN_NUM_CTA_INVDUAL", ascending=True)
if conf.debug:
    display(DF_TEMP_TRAMITE)

# Creamos una temp view a partir de DF_TEMP_TRAMITE
temp_view = f"temp_view_tramite_no_null_{params.sr_id_archivo}"
DF_TEMP_TRAMITE.createOrReplaceTempView(temp_view)

# COMMAND ----------

statement_003 = query.get_statement(
    "IDC_0300_NCI_CTE_003.sql",
    CATALOG_NAME=SETTINGS.GENERAL.CATALOG,
    SCHEMA_NAME=SETTINGS.GENERAL.SCHEMA,
    SR_ID_ARCHIVO=params.sr_id_archivo,
)
df = db.sql_delta(statement_003)
if conf.debug:
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Aplicamos el filtro donde:
# MAGIC   - FTN_NUM_CTA_INVDUAL isnotnull And FCC_VALOR_IND = 1
# MAGIC   - El resultado del filtro anterior lo guardamos en una tabla delta llamada temp_vigentes_{sr_id_archivo}
# MAGIC
# MAGIC - Aplicamos el filtro donde:
# MAGIC   - FTN_NUM_CTA_INVDUAL isnotnull And FCC_VALOR_IND = 0
# MAGIC   - El resultado del filtro anterior lo guardamos en una tabla delta llamada temp_novigentes_{sr_id_archivo}
# MAGIC
# MAGIC - Aplicamos el filtro que sea todo lo contrario a los anteriores filtros:
# MAGIC   - El resultado del filtro anterior lo guardamos en un df llamado df.

# COMMAND ----------

df_vigentes = df.filter("FTN_NUM_CTA_INVDUAL IS NOT NULL AND FCC_VALOR_IND = 1")
temp_view_vigentes = f"temp_vigentes_{params.sr_id_archivo}"
db.write_delta(temp_view_vigentes, df_vigentes, "overwrite")
if conf.debug:
    display(df_vigentes)

df_novigentes = df.filter("FTN_NUM_CTA_INVDUAL IS NOT NULL AND FCC_VALOR_IND = 0")
temp_view_novigentes = f"temp_novigentes_{params.sr_id_archivo}"
db.write_delta(temp_view_novigentes, df_novigentes, "overwrite")
if conf.debug:
    display(df_novigentes)

# COMMAND ----------

df_temp_union = df_novigentes.union(df_vigentes)
df_temp_no_encontrados = df.join(df_temp_union, ["FCC_VALOR_IND"], "left_anti")
if conf.debug:
    display(df_temp_no_encontrados)
del df_novigentes, df_vigentes, df_temp_union

# COMMAND ----------

df_temp_no_encontrados = df_temp_no_encontrados.select(
    "FTN_NSS_CURP",
    "FTN_NUM_CTA_INVDUAL",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTC_FOLIO",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_CLAVE_ENT_RECEP",
    "FCN_ID_TIPO_SUBCTA"
).withColumn("FTC_IDENTIFICADOS", lit(0)) \
 .withColumn("FTN_ID_DIAGNOSTICO", lit(375)) \
 .withColumn("FTN_VIGENCIA", lit(0))

if conf.debug:
    display(df_temp_no_encontrados)

# COMMAND ----------

DF_TEMP_TRAMITE_ORI = (
    DF_TEMP_TRAMITE_ORI.filter(DF_TEMP_TRAMITE_ORI.FTC_BANDERA.isNull())
    .withColumn("FTC_IDENTIFICADOS", lit(0))
    .withColumn("FTN_ID_DIAGNOSTICO", lit(93))
    .withColumn("FTN_VIGENCIA", lit(0))
)

columnas = [
    "FTN_NSS_CURP",
    "FTN_NUM_CTA_INVDUAL",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTC_FOLIO",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_CLAVE_ENT_RECEP",
    "FCN_ID_TIPO_SUBCTA",
    "FTC_IDENTIFICADOS",
    "FTN_ID_DIAGNOSTICO",
    "FTN_VIGENCIA",
]
DF_TEMP_TRAMITE_ORI = DF_TEMP_TRAMITE_ORI.select(*columnas)
if conf.debug:
    display(DF_TEMP_TRAMITE_ORI)

# COMMAND ----------

df_final = df_temp_no_encontrados.union(DF_TEMP_TRAMITE_ORI)
temp_view_noencontrados = f"temp_noencontrados_{params.sr_id_archivo}"
db.write_delta(temp_view_noencontrados, df_final, "overwrite")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
