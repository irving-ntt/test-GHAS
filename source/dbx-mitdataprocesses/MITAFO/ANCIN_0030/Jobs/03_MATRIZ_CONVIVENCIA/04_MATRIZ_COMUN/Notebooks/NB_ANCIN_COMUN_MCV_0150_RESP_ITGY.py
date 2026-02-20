# Databricks notebook source
'''
Descripcion:
    Actualiza el número de registros recibidos en la tabla de control  TTAFOGRAL_RESPUESTA_ITGY, resguarda la información de la desmarca en integrity en la tabla  THAFOGRAL_MAR_DES_ITGY   y generara el archivo con los registros que  no fueron desmarcados.
Subetapa:
    MATRIZ DE CONVIVENCIA - COMUN 
Tramite:
-Solicitud de Marca de Cuentas por 43 BIS	
-Transferencias de Acreditados Infonavit	
-Transferencia por Anualidad Garantizada	
-Uso de Garantía por 43 BIS	
-Desmarca de crédito de vivienda por 43 BIS	
-Transferencia de Acreditados Fovissste
-Devolución de Excedentes por 43 BIS	
-Devolución de Excedentes por T.A. Fovissste	
-Devolución de Saldos Excedentes por T.A.A.G.	
-Desmarca Fovissste
Tablas Input:
    NA
Tablas Output:
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL
Tablas DELTA:
    dbx_mit_dev_1udbvf_workspace.delta_001_{params.sr_id_archivo}
    dbx_mit_dev_1udbvf_workspace.delta_002_{params.sr_id_archivo}
    dbx_mit_dev_1udbvf_workspace.delta_003_{params.sr_id_archivo}

Archivos SQL:
    valid_struct_0001_traspasos.sql
    valid_struct_0003_traspasos_2.sql
    valid_struct_0004_traspasos.sql
    valid_struct_0005_traspasos.sql
    valid_struct_0006_traspasos.sql
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
    "sr_folio": str,
    "sr_usuario": str,
    "sr_tipo_mov": str,
    "sr_conv_ingty": str,
    "sr_path_arch": str,
    "sr_tipo_ejecucion": str,
})
# Validar widgets
params.validate()



# COMMAND ----------

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
    "valid_struct_0001_traspasos.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    SR_SUBETAPA=params.sr_subetapa,
    MAX_LENGTH=conf.val_records_length_traspasos,
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
    "valid_struct_0003_traspasos_2.sql",
    temp_view = f"temp_view_001_{params.sr_id_archivo}",
    MAX_LENGTH=conf.val_records_length_traspasos,
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

df = spark.read.csv(full_file_name, header="true", inferSchema="true", sep=",",encoding="latin1")

if conf.debug:
    display(df)

# COMMAND ----------

delta_003 = f"delta_003_{params.sr_id_archivo}"
db.write_delta(delta_003, df, "overwrite")


# COMMAND ----------

# DBTITLE 1,QUERY STATEMENT 0004
statement_demo_0004 = query.get_statement(
    "valid_struct_0004_traspasos.sql",
SR_ID_ARCHIVO=params.sr_id_archivo,
    SR_SUBETAPA=params.sr_subetapa,
    MAX_LENGTH=conf.val_records_length_traspasos,
    DELTA_003=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + delta_003,
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
    "valid_struct_0005_traspasos.sql",
    temp_view_002 = f"temp_view_002_{params.sr_id_archivo}",
)
df_output_table = db.sql_delta(statement_demo_0005)
if conf.debug:
    display(df_output_table)

# COMMAND ----------

statement_demo_006 = query.get_statement(
    "valid_struct_0006_traspasos.sql",
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
db.drop_delta(f"delta_003_{params.sr_id_archivo}")

# COMMAND ----------

Notify.send_notification("VAL_ESTRUCT", params)

# COMMAND ----------


