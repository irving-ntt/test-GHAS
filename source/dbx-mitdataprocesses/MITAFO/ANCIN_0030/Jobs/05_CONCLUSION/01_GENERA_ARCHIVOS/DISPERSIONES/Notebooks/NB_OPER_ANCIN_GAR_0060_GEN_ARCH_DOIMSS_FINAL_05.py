# Databricks notebook source
'''
Descripcion:
    Creación de archivo CTINDI subsecuente infonavit. 
Subetapa:
    GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      9 - SUBSECUENTE INFONAVIT
Tablas INPUT:
    CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    GAR_0060_GEN_ARCH_DOIMSS_FINAL_007.SQL
'''

# COMMAND ----------

# DBTITLE 1,Inicio - Configuraciones Principales
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_folio": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_subetapa": str,
        "sr_sec_lote": str,
        "sr_fecha_lote": str,
        "sr_fecha_acc": str,
        "sr_tipo_archivo": str,
        "sr_estatus_mov": str,
        "sr_tipo_mov": str,
        "sr_accion": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str, # es obligatorio para servicios
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(
    queries_df.filter(
        col("Archivo SQL").startswith("GAR_0060_GEN_ARCH_DOIMSS")
    )
)

# COMMAND ----------

# DBTITLE 1,Construcción de Principal
statement_001 = query.get_statement(
    "GAR_0060_GEN_ARCH_DOIMSS_FINAL_007.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

# DBTITLE 1,Extraccion de datos
df = db.read_data("default", statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,Validación de Archivo Vacío
if df.limit(1).count() == 0:
    Notify.send_notification("INFO", params)
    dbutils.notebook.exit("No hay información para generar el archivo.")

# COMMAND ----------

# DBTITLE 1,Concatenación de columnas para el Archivo Final
from pyspark.sql.functions import concat_ws

# Concatenar las columnas especificadas en una sola columna "ARCHIVO_CTINDI" sin separador
df = df.withColumn(
    "ARCHIVO_CTINDI", 
    concat_ws(
        "", 
        df["CVE_SIEFORE"], df["FTN_NUM_CTA_INVDUAL"], df["DVCUE"], df["NUMREG"], 
        df["FFN_CODMOV"], df["NSSTRA"], df["FCD_FECPRO"], df["SECPRO"], 
        df["FTD_FEHCCON"], df["FCN_VALOR_ACCION"], df["FECTRA"], df["SECLOT"], 
        df["FNN_CORREL"], df["FND_FECHA_PAGO"], df["FNC_PERIODO_PAGO_PATRON"], 
        df["FNC_FOLIO_PAGO_SUA"], df["FECSUA"], df["NSSEMP"], df["MONPES1"], df["MONCUO1"],
        df["MONPES2"], df["MONCUO2"], df["MONPES3"], df["MONCUO3"], df["MONPES4"], df["MONCUO4"],
        df["MONPES5"], df["MONCUO5"], df["MONPES6"], df["MONCUO6"], df["MONPES7"], df["MONCUO7"],
        df["MONPES8"], df["MONCUO8"], df["MONPES9"], df["MONCUO9"], df["FECHA_1"], df["FECHA_2"],
        df["FND_FECVRCV"], df["FND_FECHA_VALOR_IMSS_ACV_VIV"], df["FECVGUB"], df["CVESERV"], df["VARMAX"]
    )
)

# Seleccionar solo la columna "ARCHIVO_CTINDI" y asignarla a un nuevo DataFrame
df_final = df.select("ARCHIVO_CTINDI")

if conf.debug:
    display(df_final)

# COMMAND ----------

# DBTITLE 1,Configuraciones del archivo
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
if (
    params.sr_subproceso == "8"
    and params.sr_subetapa == "428"
    and params.sr_tipo_archivo == "425"
):
    full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + "/CTINDI1_"
        + fecha_actual
        + "_001.DAT"
    )
    full_file_name_md5 = (
        conf.external_location
        + conf.err_repo_path
        + "/MD5_CTINDI1_"
        + fecha_actual
        + "_001.DAT.md5"
    )
elif (
    params.sr_subproceso == "9"
    and params.sr_subetapa == "428"
    and params.sr_tipo_archivo == "425"
):
    full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + "/SVIC1_"
        + fecha_actual
        + "_668.DAT"
    )
    full_file_name_md5 = (
        conf.external_location
        + conf.err_repo_path
        + "/MD5_SVIC1_"
        + fecha_actual
        + "_668.DAT.md5"
    )
else:
    logger.error("Parametros de entrada no validos para generar Archivos CTINDI")
    raise ValueError("Parametros de entrada no validos para generar Archivos CTINDI")
full_path = (
    "/Volumes/"
    + SETTINGS.GENERAL.CATALOG
    + "/"
    + SETTINGS.GENERAL.SCHEMA
    + "/doimss_carga_archivo/CTINDI1_"
    + fecha_actual
    + "_001"
)

# COMMAND ----------

# DBTITLE 1,Creación de Archivo CTINDI
# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df_final,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=conf.header,
    calcular_md5=True  # Cambia a False si no quieres calcular el MD5
)

# COMMAND ----------

# DBTITLE 1,Copia del Archivo
#En caso de que se requiera validar el archivo en la maquina local
#dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/CTINDI1_20250603_001.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

# COMMAND ----------

# DBTITLE 1,Listado de archivos
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')

# COMMAND ----------

# DBTITLE 1,Notificación de proceso finalizado
Notify.send_notification("INFO", params)

# COMMAND ----------

# DBTITLE 1,Fin - Limpieza
CleanUpManager.cleanup_notebook(locals())
