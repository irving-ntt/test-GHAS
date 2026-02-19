# Databricks notebook source
# MAGIC %md
# MAGIC ## COMUN 140 GENERACION DE ARCHIVO INTEGRITY

# COMMAND ----------

"""
Descripcion:
    160 GENERA ARCHIVO PARA INTEGRITY
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
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
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

# Query para los datos DEL ARCHIVO CSV CON EL RESULTADO DE LOS QUE NO CONVIVEN
# 
statement_001 = query.get_statement(
    "COMUN_MCV_160_TD_08_DB_EXTRAE_INFO_ARCHIVO_CSV.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

df = db.read_data("default", statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

# verificar si se desactiva este mensaje porque pueden coincidir todas las cuentas y no ser un errror 
# if df.limit(1).count() == 0:
#    Notify.send_notification("INFO", params)
#    dbutils.notebook.exit("No hay información para generar el archivo.")

# COMMAND ----------

# DBTITLE 1,Prepara datos para el nombre del archivo
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# Extrae la ruta 
first = f"{params.sr_path_arch.lstrip('/').split('/', 1)[0]}"
print(first)  # "INTEGRITY"

# #p_RT_PATH_INTEGRITY##p_SR_FOLIO#_Matriz_Convivencia#p_EXT_ARC_CSV#
# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual

full_file_namea = (
    SETTINGS.GENERAL.EXTERNAL_LOCATION 
    +  f"{params.sr_path_arch}"
    + "/INF"
    + f"/{params.sr_folio}"
    + "_Matriz_Convivencia.csv")

# opción 1: split (seguro y sencillo)
full_file_name = (
    SETTINGS.GENERAL.EXTERNAL_LOCATION 
    +  "/"
    + first
    + "/INF"
    + f"/{params.sr_folio}"
    + "_Matriz_Convivencia.csv")

# COMMAND ----------

# DBTITLE 1,Generacion del archivo
# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=False,
    calcular_md5=False  # Cambia a False si no quieres calcular el MD5
)

# COMMAND ----------

# DBTITLE 1,Lista los archivos de la ruta
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')

# COMMAND ----------

# DBTITLE 1,Elimina archivos
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/#params.sr_folio_#20250603_001.DAT.md5', recurse=True)

# COMMAND ----------

# DBTITLE 1,Mueve a Catalog Local
#En caso de que se requiera validar el archivo en la maquina local
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/202004130851050001_Matriz_Convivencia.CSV', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# dbutils.fs.cp(full_file_name, '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
