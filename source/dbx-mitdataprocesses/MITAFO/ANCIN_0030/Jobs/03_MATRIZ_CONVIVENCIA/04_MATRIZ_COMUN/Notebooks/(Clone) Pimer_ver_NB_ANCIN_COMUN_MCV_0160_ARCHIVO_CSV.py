# Databricks notebook source
"""
Descripcion:
    160 GENERA ARCHIVO CSV
    lee la informacion de  MATRIZ y genera archivo .CSV con los resultados de la convivencia
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
Tablas input:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
    DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}        
Tablas output:
    TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
Tablas Delta:
    N/A
Archivos SQL:
   
    """

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

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

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# Query para los datos
statement_001 = query.get_statement(
    "COMUN_MCV_120_TD_08_DB_EXTRAE_INFO_ARCHIVO_CSV.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

df = db.read_data("default", statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,extraccion a un data frame
df = db.read_data("default", statement_001) if conf.debug else None; display(df) if conf.debug else None 

# COMMAND ----------

if df.limit(1).count() == 0:
    Notify.send_notification("INFO", params)
    dbutils.notebook.exit("No hay información para generar el archivo.")

# COMMAND ----------

# DBTITLE 1,Prepara datos para el archivo
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# #p_RT_PATH_INTEGRITY##p_SR_FOLIO#_Matriz_Convivencia#p_EXT_ARC_CSV#
# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + f"/{params.sr_folio}"
    #    + fecha_actual
        + "_Matriz_Convivencia.CSV"
    )
full_file_name_md5 = (
        conf.external_location
        + conf.err_repo_path
        + "/MD5_TEST1_"
        + fecha_actual
        + "_001.DAT.md5"
    )    
    
full_path = (
    "/Volumes/"
    + SETTINGS.GENERAL.CATALOG
    + "/"
    + SETTINGS.GENERAL.SCHEMA
    + "/doimss_carga_archivo/CTTEST_"
    + fecha_actual
    + "_001"
)

# COMMAND ----------

# DBTITLE 1,Configuraciones de archivo
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
    params.sr_subproceso == "354"
    and params.sr_subetapa == "25"
    # and params.sr_tipo_archivo == "425"
):
    full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + "/TEST1_"
        + fecha_actual
        + "_001.DAT"
    )
    full_file_name_md5 = (
        conf.external_location
        + conf.err_repo_path
        + "/MD5_TEST1_"
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
    + "/doimss_carga_archivo/CTTEST_"
    + fecha_actual
    + "_001"
)

# COMMAND ----------

#df.write.csv(full_file_name, header=True, mode="overwrite", sep=","),

# COMMAND ----------

#En caso de que se requiera validar el archivo en la maquina local
#dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/CTINDI1_20250521_001.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
#En caso de que se requiera validar el archivo en la maquina local
#dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/TEST1_20250603_001.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

# COMMAND ----------

# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=conf.header,
    calcular_md5=True  # Cambia a False si no quieres calcular el MD5
)

# COMMAND ----------

dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/TEST1_20250603_001.DAT/', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Descripcion del error
# MAGIC Al ejecutar la instruccion de la celda 10, se creo un directorio en `abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/` con el mismo nombre del archivo. Esto lo puede corrborar con (ver celda 14) `dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')` por eso al subir el archivo con la funcion `generar_archivo_ctindi()` los fragmentos se guardaron dentro de ese directorio y por esa razon la instruccion `dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/TEST1_20250603_001.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')` falla. 
# MAGIC
# MAGIC ## Solucion:
# MAGIC - 1. cambiar el nombre del archvivo y no ejecutar (borrar) la celda 10. 
# MAGIC - 2. borrar el directorio y conservar el mismo nombre de archivo y no ejecutar (borrar) la celda 10
# MAGIC
# MAGIC ### Para borrar un directorio:
# MAGIC - `dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/TEST1_20250603_001.DAT/', recurse=True)`

# COMMAND ----------

dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')

# COMMAND ----------

dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/#params.sr_folio_#20250603_001.DAT.md5', recurse=True)

# COMMAND ----------

#En caso de que se requiera validar el archivo en la maquina local
dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/201907091556490001_Matriz_Convivencia.CSV', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
