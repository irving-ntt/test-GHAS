# Databricks notebook source
'''
Descripcion:
    Validacion de estructura para Archivo de Respuesta de ACLESP
Subetapa:
    Validacion de estructura
Tramite:
- Validacion de estructura para el Archivo de Respuesta de ACLESP
Tablas Input:
    NA
Tablas Output:
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL
Tablas DELTA:
    dbx_mit_dev_1udbvf_workspace.delta_001_{params.sr_id_archivo}
    dbx_mit_dev_1udbvf_workspace.delta_002_{params.sr_id_archivo}
    dbx_mit_dev_1udbvf_workspace.delta_003_{params.sr_id_archivo}

Archivos SQL:
    VALID_STRUCT_ARCH_RESP_ACESP_001.sql
    VALID_STRUCT_ARCH_RESP_ACESP_002.sql
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

DELTA_001 = f"DELTA_001_{params.sr_id_archivo}"

db.write_delta(DELTA_001, load_file(SETTINGS.GENERAL.EXTERNAL_LOCATION,params.sr_path_arch), "overwrite")

# COMMAND ----------

# DBTITLE 1,CREATE STATEMENT 001
statement_001 = query.get_statement(
    "VALID_STRUCT_ARCH_RESP_ACESP_001.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    SR_SUBETAPA=params.sr_subetapa,
    MAX_LENGTH=conf.VAL_RECORDS_LENGTH_ARCH_RESP_ACESP,
    #FECHA_CARGA=FECHA_CARGA,
    # DELTA_001 = "dbx_mit_dev_1udbvf_workspace.default.delta_001_1"
    DELTA_TABLE_NAME_001=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + DELTA_001,
)

# COMMAND ----------

# DBTITLE 1,CREATE DATA TABLE 02
DELTA_002 = f"DELTA_002_{params.sr_id_archivo}"

db.write_delta(DELTA_002, db.sql_delta(statement_001), "overwrite")
if conf.debug:
    display(DELTA_002)

# COMMAND ----------

# DBTITLE 1,QUERY STATEMENT 0003
statement_002 = query.get_statement(
    "VALID_STRUCT_GENERAL_001.sql",
    DELTA_TABLE_NAME_001=SETTINGS.GENERAL.CATALOG + "." + SETTINGS.GENERAL.SCHEMA + "." + DELTA_002,
    ESTADOS_DET="'02','03','05'",
)

dfLoaded3 = db.sql_delta(statement_002)

if conf.debug:
    display(dfLoaded3)

# COMMAND ----------



#full_file_name2 = (
 #   SETTINGS.GENERAL.EXTERNAL_LOCATION + conf.err_repo_path + "/" + params.sr_id_archivo + "_" + 'prueba.csv'
#)

#dfLoaded3.write.mode("overwrite").format('csv').option("header", "true").option("encoding","UTF-8").option("charset","latin1").csv(full_file_name2)

file_manager = FileManager(err_repo_path=conf.err_repo_path)
full_file_name = (
    SETTINGS.GENERAL.EXTERNAL_LOCATION + conf.err_repo_path + "/" + params.sr_id_archivo + "_" + conf.output_file_name_001 + ".csv"
)
file_manager.generar_archivo_flexible(
    df=dfLoaded3,
    full_file_name=full_file_name,
    opciones={
        "header": "true",
        "encoding": "ISO-8859-1",
        "delimiter": ",",
        "quote": '"',
        "charset": "latin1",
        "escapeQuotes": False
    },
    calcular_md5=False
)

# COMMAND ----------

statement_003 = query.get_statement(
    "VALID_STRUCT_GENERAL_002.sql",
    DELTA_TABLE_NAME_001=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + DELTA_002,
)

# COMMAND ----------

DELTA_003 = f"DELTA_003_{params.sr_id_archivo}"

db.write_delta(DELTA_003, db.sql_delta(statement_003), "overwrite")
if conf.debug:
    display(DELTA_003)

# COMMAND ----------

statement_004 = query.get_statement(
    "VALID_STRUCT_GENERAL_003.sql",
    DELTA_TABLE_NAME_001=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + DELTA_003,
)

df_output_table = db.sql_delta(statement_004)

# COMMAND ----------

# DBTITLE 1,APPEND CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL
target_table = conf.conn_schema_default + '.' + conf.table_001
db.write_data(df_output_table, target_table, "default", "append")

# COMMAND ----------

db.drop_delta(f"delta_001_{params.sr_id_archivo}")
db.drop_delta(f"delta_002_{params.sr_id_archivo}")
db.drop_delta(f"delta_003_{params.sr_id_archivo}")

# COMMAND ----------

Notify.send_notification("VAL_ESTRUCT", params)
