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
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0900_ARCH_CLI")))

# COMMAND ----------

import inspect

def fix_created_file(file_name):
    full_file_name_aux = full_file_name + "_TEMP"
    dbutils.fs.rm(full_file_name_aux, recurse=True)
    file_name_tmp = dbutils.fs.ls(full_file_name)
    file_name_new = list(filter(lambda x: x[0].endswith("csv"), file_name_tmp))
    dbutils.fs.cp(file_name_new[0][0], full_file_name_aux)
    dbutils.fs.rm(full_file_name, recurse=True)
    dbutils.fs.cp(full_file_name_aux, full_file_name)
    dbutils.fs.rm(full_file_name_aux, recurse=True)

# COMMAND ----------

statement_001 = query.get_statement(
    "IDC_0900_ARCH_CLI_001.sql",
    sr_folio=params.sr_folio,
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

df = db.read_data("default", statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

full_file_name =  SETTINGS.GENERAL.EXTERNAL_LOCATION + conf.out_repo_path + '/' + params.sr_folio + '_' + conf.output_file_name_001

file_name = full_file_name + '_TEMP'

header = True

df.write.format("csv").mode('overwrite').option("header", header).save(full_file_name + '_TEMP')
dataframe = spark.read.option('header', header).csv(full_file_name + '_TEMP')
dataframe.coalesce(1).write.format('csv').mode('overwrite').option('header', True).save(full_file_name)

# COMMAND ----------

failed_task = fix_created_file(full_file_name)
lines = dbutils.fs.head(full_file_name)
if (lines.count('\n') == 1 and header) or (lines.count('\n') == 0):
    dbutils.fs.rm(full_file_name, recurse = True)

# COMMAND ----------

# Copy the generated file to the assigned volume for downloading to the local machine
# dbutils.fs.cp(
#     full_file_name,
#     '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/202504281458012289_Identificacion_Cliente.csv')


# COMMAND ----------

Notify.send_notification("INFO", params)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
