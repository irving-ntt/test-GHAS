# Databricks notebook source
# MAGIC %sql
# MAGIC /*
# MAGIC %md
# MAGIC TRAPASOS:
# MAGIC NoteBook que Genera el archivo de respuesta del SubPoceso 3286 - Transferencia de Recursos por Portabilidad
# MAGIC */

# COMMAND ----------

# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_folio": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("GAR_0")))

# COMMAND ----------

# DBTITLE 1,Construcción Actualizacion de la tabla PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
statement_001 = query.get_statement(
    "GAR_0500_GEN_ARCHIVO_RESP.sql",
    DELTA_ENCABEZADO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0100_encabezado_{params.sr_folio}",
	DELTA_DETALLE=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0200_detalle_{params.sr_folio}",
	DELTA_SUMARIO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0300_sumario_{params.sr_folio}"
)

# COMMAND ----------

# DBTITLE 1,Ejecución Archivo de Respuesta
df_final = spark.sql(statement_001)  #Se uso asi por las tablas que se encuentran creadas dentro de databricks
#df = db.read_data("default", statement_001)  # Replace 'your_table_name' with the actual table name.
if conf.debug:
    display(df_final)
    display(str(df_final.count()) + " Registros Totales")

# COMMAND ----------

# DBTITLE 1,Consecutivo Archivo
statement_002 = query.get_statement(
    "GAR_0600_CONSEC_ARCHIVO.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Genera Nombre del archivo y la ruta
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# Genera Consecutivo de Archivo

df_archivo_consecutivo = db.read_data(
    "default", statement_002
)


#Toma del Dataframe solo el valor de la Columna FTN_ID_ARCHIVO generada con el statement_002
#id_consecutivo = df_archivo_consecutivo.collect()[0]["FTN_ID_ARCHIVO"]
id_consecutivo = df_archivo_consecutivo.first()["FTN_ID_ARCHIVO"]


# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + conf.prefijo_3282
        + fecha_actual
        + "_"
        + str(id_consecutivo)
        +conf.nom_archivo_3286
        + ".txt"
    )

if conf.debug:
    display(full_file_name)

# COMMAND ----------

# DBTITLE 1,Genera Archivo de Respuesta
# Genera el archivo y decide si calcular MD5 o no


file_manager.generar_archivo_flexible(
    df=df_final,
    full_file_name=full_file_name,
    opciones={
        "header": "false",
        "encoding": "ISO-8859-1",
        #"delimiter": "\n",
        #"quote": "",
        "charset": "latin1",
        "escapeQuotes": True,
        "ignoreLeadingWhiteSpace": "false",
        "ignoreTrailingWhiteSpace": "false"
    },
    calcular_md5=False
)

# COMMAND ----------

# DBTITLE 1,Notificación
#Notify.send_notification("INFO", params) 

# COMMAND ----------

# DBTITLE 1,Depuración de Dataframes usados
CleanUpManager.cleanup_notebook(locals())
