# Databricks notebook source
# MAGIC %sql
# MAGIC /*
# MAGIC %md
# MAGIC TRAPASOS:
# MAGIC NoteBook Que genera el archivo de respuesta TRP y Actualiza la tabla PROCESOS.TTAFOTRAS_TRANS_REC_PORTA referente a los datos del archivo de respuesta del SubPoceso 3286 - Transferencia de Recursos por Portabilidad
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
        "sr_id_archivo": str,
        "sr_fec_arc": str,
        "sr_paso": str,
        "sr_id_archivo_06": str,
        "sr_id_archivo_09": str
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
if conf.debug:
   display(queries_df.filter(col("Archivo SQL").startswith("GAR_0")))

# COMMAND ----------

# DBTITLE 1,Contruccion de los datos de  la generacion del archivo TRP
statement_001 = query.get_statement(
    "GAR_0500_GEN_ARCHIVO_RESP.sql",
    DELTA_ENCABEZADO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0100_encabezado_{params.sr_folio}",
	DELTA_DETALLE=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0200_detalle_{params.sr_folio}",
	DELTA_SUMARIO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0300_sumario_{params.sr_folio}"
)

# COMMAND ----------

#Genera DataFrame para la creacion del archivo
df_final = spark.sql(statement_001)

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
fecha_actual_archivo = datetime.now(mexico_tz).strftime("%Y%m%d")

# Genera la fecha actual en el formato DD-MM-YYYY
fecha_actual_bd = datetime.now(mexico_tz).strftime("%d-%m-%Y")

#fecha timeStamp
fecha_actual_timestamp = datetime.now(mexico_tz).strftime("%d-%m-%Y %H:%M:%S.%f")



# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + conf.mascara_archivo_3286
    )

  

# COMMAND ----------

# DBTITLE 1,Genera Archivo de Respuesta
# Genera el archivo y decide si calcular MD5 o no

file_manager.generar_archivo_flexible(
    df=df_final,
    full_file_name=full_file_name,
    opciones={
        "header": "false",
        "encoding": "ISO-8859-1",
        #"delimiter": "",
        "linesep": "\r\n",
        "quote": "",
        "charset": "latin1",
        "escapeQuotes": False,
        "ignoreLeadingWhiteSpace": "false",
        "ignoreTrailingWhiteSpace": "false"
    },
    calcular_md5=False
)

# COMMAND ----------

# DBTITLE 1,Construcción Actualizacion de la tabla PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
statement_003 = query.get_statement(
    "GAR_0400_UPD_PROC_TRP.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    SR_FECHA_ACTUAL=fecha_actual_bd,
    SR_FECHA_ACTUAL_TIMESTAMP=fecha_actual_timestamp,
    SR_MASCARA_ARCHIVO=conf.mascara_archivo_3286,
    SR_USUARIO=params.sr_usuario
)

# COMMAND ----------

# DBTITLE 1,Ejecucion de Merge
execution = db.execute_oci_dml(
    statement=statement_003, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Depuración de Dataframes usados
CleanUpManager.cleanup_notebook(locals())
