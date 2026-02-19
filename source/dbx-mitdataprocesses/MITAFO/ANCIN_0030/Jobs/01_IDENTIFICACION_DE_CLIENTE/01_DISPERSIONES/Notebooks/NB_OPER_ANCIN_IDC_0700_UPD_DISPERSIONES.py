# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams(
    {
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
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0700_UPD_DISPERSIONES")))

# COMMAND ----------

statement_001 = query.get_statement(
    "IDC_0700_UPD_DISPERSIONES​_001.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

temp_view_001 = "TEMP_PREDISPERSIONES" + "_" + params.sr_id_archivo
db.write_delta(temp_view_001, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view_001))

# COMMAND ----------

ID_CAT_CATALOGO_1 = 138  # VALOR PARA IMSS / AEIM
ID_CAT_CATALOGO_2 = 140  # VALOR PARA IMSS / AEIM
if params.var_tramite == "ISSSTE":
    ID_CAT_CATALOGO_1 = 139
    ID_CAT_CATALOGO_2 = 140

# COMMAND ----------

statement_002 = query.get_statement(
    "IDC_0700_UPD_DISPERSIONES​_002.sql",
    SR_FOLIO=params.sr_folio,
    ID_CAT_CATALOGO_1=ID_CAT_CATALOGO_1,
    ID_CAT_CATALOGO_2=ID_CAT_CATALOGO_2,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

temp_view_002 = "TEMP_NIVEL_SIEFORE" + "_" + params.sr_id_archivo
db.write_delta(temp_view_002, db.read_data("default", statement_002), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view_002))

# COMMAND ----------

statement_003 = query.get_statement(
    "IDC_0700_UPD_DISPERSIONES​_003.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG=SETTINGS.GENERAL.CATALOG,
    SCHEMA=SETTINGS.GENERAL.SCHEMA,
)

# COMMAND ----------

temp_view_003 = 'TEMP_DISPERSION_07' + '_'+ params.sr_id_archivo
db.write_delta(temp_view_003, db.sql_delta(statement_003), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view_003))

# COMMAND ----------

# MAGIC %md
# MAGIC ##NB_OPER_ANCIN_IDC_0710_UPD_DISP

# COMMAND ----------

statement_004 = query.get_statement(
    "IDC_0700_UPD_DISPERSIONES​_004.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(
    statement=statement_004, async_mode=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###NB_OPER_ANCIN_IDC_0720_UPD_DISP​

# COMMAND ----------

statement_005 = query.get_statement(
    "IDC_0700_UPD_DISPERSIONES​_005.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG=SETTINGS.GENERAL.CATALOG,
    SCHEMA=SETTINGS.GENERAL.SCHEMA,
)

# COMMAND ----------

df = db.sql_delta(statement_005)
if conf.debug:
    display(df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_utc_timestamp,
    to_timestamp,
)

df = (
    df.withColumn(
        "FTD_FEC_CRE", F.to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
    )
    .withColumn("FTC_USU_ACT", lit(None).cast("string"))  # ← Aquí está el cambio correcto
    # .withColumn(
    #     "FTD_FEC_ACT", F.to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
    # )
)

if conf.debug:
    display(df)

# COMMAND ----------

table_name_009 = "CIERREN_ETL.TTSISGRAL_ETL_DISPERSION"
db.write_data(df, table_name_009, "default", "append")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
