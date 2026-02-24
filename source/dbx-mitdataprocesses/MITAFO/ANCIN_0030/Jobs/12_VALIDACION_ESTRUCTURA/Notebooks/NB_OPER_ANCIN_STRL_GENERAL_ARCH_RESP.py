# Databricks notebook source
'''
Descripcion:
    Validacion de estructura general para generacion de archivo de respuesta
Subetapa:
    832
Tramite:
    Solicitud de Marca de Cuentas por 43 BIS
    Transferencias de Acreditado Fovissste
    Transferencias de Acreditado Infonavit
    Transferencia por Anualidad Garantizada
    Uso de garantias por 43 BIS

Tablas Input:
    NA
Tablas Output:
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL
Tablas DELTA:
    dbx_mit_dev_1udbvf_workspace.delta_001_{params.sr_id_archivo}
    dbx_mit_dev_1udbvf_workspace.delta_002_{params.sr_id_archivo}
    dbx_mit_dev_1udbvf_workspace.delta_003_{params.sr_id_archivo}
    dbx_mit_dev_1udbvf_workspace.delta_004_{params.sr_id_archivo}

Archivos SQL:
    
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
    "sr_path_arch" :str,
    "sr_origen_arc": str,
    "sr_folio" :str,
    "sr_instancia_proceso" :str,
    "sr_usuario" :str,
    "sr_etapa" :str,
    "sr_id_snapshot" :str,
    "sr_paso" :str,
    "sr_id_archivo_siguiente": str,
    "sr_fec_arc": str
})

# Validar widgets
#params.validate()
id_proceso = params.sr_id_archivo if params.sr_id_archivo != "1" else params.sr_folio
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
delta_001 = f"delta_001_{id_proceso}"
db.write_delta(delta_001, dfLoaded, "overwrite")

# COMMAND ----------

# DBTITLE 1,CREATE STATEMENT 001
statement_demo_001 = query.get_statement(
    "VALID_STRUCT_GENERAL_ARCH_RESP_001.sql",
    SR_ID_ARCHIVO=id_proceso,
    SR_SUBETAPA=params.sr_subetapa,
    MAX_LENGTH=conf.VAL_RECORDS_LENGTH_ARCH_RESP_GRAL,
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
# DBTITLE 1,CREATE DATA TABLE 02
from pyspark.sql.functions import lit

# Agregar FTC_FOLIO desde los parámetros de entrada
dfLoaded2 = dfLoaded2.withColumn("FTC_FOLIO", lit(params.sr_folio))

delta_002 = f"delta_002_{id_proceso}"
db.write_delta(delta_002, dfLoaded2, "overwrite")

if conf.debug:
    display(delta_002)


# COMMAND ----------

# DBTITLE 1,QUERY STATEMENT 0003
statement_demo_0003 = query.get_statement(
    "VALID_STRUCT_0003_GENERAL_ARCH_RESP_2.sql",
    temp_view = f"temp_view_001_{id_proceso}",
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

# Inicializa la clase
file_manager = FileManager(err_repo_path=conf.err_repo_path_tras)

# Llama al método para generar el archivo
full_file_name = (
    SETTINGS.GENERAL.EXTERNAL_LOCATION + conf.err_repo_path_tras + "/" + id_proceso + "_" + conf.output_file_name_002 + ".csv"
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

delta_003 = f"delta_003_{id_proceso}"
db.write_delta(delta_003, dfLoaded3, "overwrite")

# COMMAND ----------

# DBTITLE 1,QUERY STATEMENT 0004
statement_demo_0004 = query.get_statement(
    "VALID_STRUCT_0004_GENERAL_ARCH_RESP.sql",
    DELTA_002=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + delta_002,
    DELTA_003=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + delta_003,
)

# COMMAND ----------

dfLoaded4 = db.sql_delta(statement_demo_0004)
dfLoaded4.createOrReplaceTempView(f"temp_view_002_{id_proceso}")
if conf.debug:
    display(dfLoaded4)


# COMMAND ----------

delta_004 = f"delta_004_{id_proceso}"
db.write_delta(delta_004, dfLoaded4, "overwrite")

# COMMAND ----------

# DBTITLE 1,QUERY STATEMENT 0005
statement_demo_0005 = query.get_statement(
    "VALID_STRUCT_0005_GENERAL_ARCH_RESP.sql",
    DELTA_002=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + delta_002,
    DELTA_004=SETTINGS.GENERAL.CATALOG
    + "."
    + SETTINGS.GENERAL.SCHEMA
    + "."
    + delta_004,
)
df_output_table = db.sql_delta(statement_demo_0005)
if conf.debug:
    display(df_output_table)

# COMMAND ----------

statement_demo_006 = query.get_statement(
    "VALID_STRUCT_0006_GENERAL_ARCH_RESP.sql",
    FTN_ID_ARCHIVO=id_proceso,
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

db.drop_delta(f"delta_001_{id_proceso}")
db.drop_delta(f"delta_002_{id_proceso}")
db.drop_delta(f"delta_003_{id_proceso}")
db.drop_delta(f"delta_004_{id_proceso}")

# COMMAND ----------

Notify.send_notification("VAL_ESTRUCT", params)
