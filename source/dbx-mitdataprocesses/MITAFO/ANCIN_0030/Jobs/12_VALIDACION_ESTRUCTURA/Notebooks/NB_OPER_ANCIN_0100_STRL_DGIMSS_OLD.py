# Databricks notebook source
'''
Descripcion:
    Validacion de estructura DGIMSS
Subetapa:
    439 - Validacion de estructura
Tramite:
    Gubernamental IMSS
Tablas Input:
    NA
Tablas Output:
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL
Tablas DELTA:
    dbx_mit_dev_1udbvf_workspace.delta_001_{params.sr_id_archivo}
    dbx_mit_dev_1udbvf_workspace.delta_002_{params.sr_id_archivo}

Archivos SQL:
    valid_struct_0001_doimss.sql
    valid_struct_0003_doimss_2.sql
    valid_struct_0004_doimss.sql
    valid_struct_0005_doimss.sql
    valid_struct_0006_doimss.sql
'''

# COMMAND ----------

# DBTITLE 1,FRAMEWORK UPLOAD
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,WIDGETS
# Crear la instancia con los parámetros esperados
params = WidgetParams({    
    "sr_proceso": str,    
    "sr_subproceso": str,      
    "sr_subetapa": str,
    "sr_id_archivo":str,
    "sr_path_arch" :str
})

# Validar widgets
params.validate()
# 🚀 **Ejemplo de Uso**
conf = ConfManager()
db = DBXConnectionManager()
query = QueryManager()

# COMMAND ----------

# DBTITLE 1,LOAD FILE FUNCTION
def load_file(storage_location, sr_path_arch):    
    df = spark.read.text(storage_location + sr_path_arch)
    df = df.withColumnRenamed("value", "FTC_LINEA")
    indexed_df = df.withColumn("idx", monotonically_increasing_id() + 1)
    w = Window.orderBy("idx")
    indexed_df = indexed_df.withColumn("FTN_NO_LINEA", row_number().over(w))
    indexed_df = indexed_df.withColumnRenamed("_c0", "FTC_LINEA")
    indexed_df = indexed_df.drop("idx")
    
    return indexed_df


# COMMAND ----------

# DBTITLE 1,LOAD FILE
dfLoaded = load_file(SETTINGS.GENERAL.EXTERNAL_LOCATION,params.sr_path_arch)
if conf.debug:
    display(dfLoaded)

# COMMAND ----------

# DBTITLE 1,CREATE DELTA TABLE 001
delta_001 = f"delta_001_{params.sr_id_archivo}"
db.write_delta(delta_001, dfLoaded, "overwrite")

# COMMAND ----------

# DBTITLE 1,CREATE STATEMENT 001
statement_demo_001 = query.get_statement(
    "valid_struct_0001_doimss.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    SR_SUBETAPA=params.sr_subetapa,
    MAX_LENGTH=conf.val_records_length,
    #FECHA_CARGA=FECHA_CARGA,
    # DELTA_001 = "dbx_mit_dev_1udbvf_workspace.default.delta_001_1"
    DELTA_001=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + delta_001,
)

# COMMAND ----------

# DBTITLE 1,EXECUTE STATEMENT
dfLoaded2 = db.sql_delta(statement_demo_001)
if conf.debug:
    display(dfLoaded2)

# COMMAND ----------

# DBTITLE 1,CREATE DATA TABLE 02
delta_002 = f"delta_002_{params.sr_id_archivo}"
db.write_delta(delta_002, dfLoaded2, "overwrite")
if conf.debug:
    display(delta_002)

# COMMAND ----------

# DBTITLE 1,QUERY STATEMENT 0003
statement_demo_0003 = query.get_statement(
    "valid_struct_0003_doimss_2.sql",
    temp_view = f"temp_view_001_{params.sr_id_archivo}",
    DELTA_002=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + delta_002,
)

dfLoaded3 = db.sql_delta(statement_demo_0003)

if conf.debug:
    display(dfLoaded3)

# COMMAND ----------

# DBTITLE 1,CREATE CSV
full_file_name = (
    SETTINGS.GENERAL.EXTERNAL_LOCATION + conf.err_repo_path + "/" + params.sr_id_archivo + "_" + conf.output_file_name_001
)

dfLoaded3.write.mode("overwrite").option("header", "true").csv(full_file_name)

# COMMAND ----------

# Inicializa la clase
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Llama al método para generar el archivo
full_file_name = (
    SETTINGS.GENERAL.EXTERNAL_LOCATION + conf.err_repo_path + "/" + params.sr_id_archivo + "_" + conf.output_file_name_001 + ".csv"
)
# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=dfLoaded3,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=conf.header,
    calcular_md5=False  # Cambia a False si no quieres calcular el MD5
)

# COMMAND ----------

df = spark.read.csv(full_file_name, header="true", inferSchema="true", sep=",")
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,QUERY STATEMENT 0004
statement_demo_0004 = query.get_statement(
    "valid_struct_0004_doimss.sql",
    DELTA_002=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + delta_002,
)

# COMMAND ----------

dfLoaded4 = db.sql_delta(statement_demo_0004)
dfLoaded4.createOrReplaceTempView(f"temp_view_002_{params.sr_id_archivo}")
if conf.debug:
    display(dfLoaded4)


# COMMAND ----------

# DBTITLE 1,QUERY STATEMENT 0005
statement_demo_0005 = query.get_statement(
    "valid_struct_0005_doimss.sql",
    temp_view_002 = f"temp_view_002_{params.sr_id_archivo}",
)
df_output_table = db.sql_delta(statement_demo_0005)
if conf.debug:
    display(df_output_table)

# COMMAND ----------

statement_demo_006 = query.get_statement(
    "valid_struct_0006_doimss.sql",
    FTN_ID_ARCHIVO=params.sr_id_archivo,
    FTC_ID_SUBETAPA=params.sr_subetapa,
)
execution = db.execute_oci_dml(
   statement=statement_demo_006, async_mode=False
)


# COMMAND ----------

# DBTITLE 1,APPEND CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL

target_table = conf.conn_schema_default + '.' + conf.table_001
db.write_data(df_output_table, target_table, "default", "append")

# COMMAND ----------

db.drop_delta(f"delta_001_{params.sr_id_archivo}")
db.drop_delta(f"delta_002_{params.sr_id_archivo}")

# COMMAND ----------

Notify.send_notification("VAL_ESTRUCT", params)
