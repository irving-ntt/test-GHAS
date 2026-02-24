# Databricks notebook source
# MAGIC %md
# MAGIC ## COMUN 160 GENERACION DE ARCHIVO CSV

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

# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=False,
    calcular_md5=False  # Cambia a False si no quieres calcular el MD5
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version con coding y _sin_ encabezado 

# COMMAND ----------

df_final = db.read_data("default", statement_001)
if conf.debug:
    display(df_final)

# COMMAND ----------

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
        + "/INF"
    #    + params.sr_folio
        + f"/{params.sr_folio}"
    #    + fecha_actual
        + "_Matriz_Convivencia.csv"
    )

final_file_path  =  (
    SETTINGS.GENERAL.EXTERNAL_LOCATION 
    +  "/"
    + first
    + "/INF"
    + f"/{params.sr_folio}"
    + "_Matriz_Convivencia.csv")
    
definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/PROCESS/DBX/'+ f"/PPA_RCAT064_{params.sr_folio}" + "_2.DAT"


# COMMAND ----------

full_file_name = (
    SETTINGS.GENERAL.EXTERNAL_LOCATION 
    +  f"{params.sr_path_arch}"+"/INF"
    + f"/{params.sr_folio}"
    + "_Matriz_Convivencia.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC aca

# COMMAND ----------

# full_file_name es el nombre a generar del archivo
# llega /INTEGRITY/ en los parametros de ejecucion 
# longitud = len(df_final.take(1))
# 1. Guarda el DataFrame como texto en una carpeta temporal
temp_path = final_file_path.replace('.csv', '_temp')  # Cambia la extensión para evitar conflictos
    #  asi solo genera formato unix LF al final
    #  df_final.coalesce(1).write.mode("overwrite").text(temp_path)
    #  genera en formato windows con CRLF
(df_final.coalesce(1)
.write.mode("overwrite")
    .option("header","false")
    .option("sep",",")
#    .option("encoding","UTF-8")
    .option("encoding","ISO-8859-1")
    .option("delimiter",",")
    .option("nullValue","")
    .option("emptyValue","")
    .option("charset","latin1")
    .option("lineSep", "\r\n")
    .csv(temp_path))
# 2. Mueve y renombra el archivo que se genera
# La ruta del archivo generado señalado por el sistema será algo así como temp_path/part-00000-...
import glob

# Busca el archivo en la carpeta temporal
import os

    # Listar archivos generados en la carpeta temporal
temp_files = dbutils.fs.ls(temp_path)

    # Verificamos si hay archivos y movemos el primero encontrado
if temp_files:
    for file_info in temp_files:
            # Asegúrate de que solo mueves el archivo correcto
        if file_info.name.endswith(".csv"):
            dbutils.fs.mv(file_info.path, final_file_path)
            break

    # 3. Elimina la carpeta temporal que se creó
dbutils.fs.rm(temp_path, recurse=True)

    # 4. Espera 30 segundos para asegurar que el archivo este escrito correctamente y evitar que el servicio de busqueda tome un archivo incompleto
#    import time
#    time.sleep(30)
    # 5. Renombra el archivo final listo para que lo tome el servicio de busqueda
#    dbutils.fs.mv(final_file_path, definitive_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Prueba Ubaldo
# MAGIC

# COMMAND ----------

file_manager.generar_archivo_flexible(
    df=df_final,
    full_file_name=full_file_name,
    opciones={
        "header": "False",
        "encoding": "ISO-8859-1",
        "delimiter": ",",
        "quote": '"',
        "charset": "latin1",
        "escapeQuotes": False
    },
    calcular_md5=False
)


# full_file_name es el nombre a generar del archivo
# llega /INTEGRITY/ en los parametros de ejecucion 
# longitud = len(df_final.take(1))
if len(df_final.take(1)) == 1:
    # Define la ruta final donde quieres guardar el archivo .dat
    #final_file_path      = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/'+ f"/ANT_PPA_RCAT064_{params.sr_folio}" + "_2.DAT"
    #definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/'+ f"/PPA_RCAT064_{params.sr_folio}" + "_2.DAT"
    final_file_path      = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/PROCESS/DBX/'+ f"/ANT_PPA_RCAT064_{params.sr_folio}" + "_2.DAT"
    definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/PROCESS/DBX/'+ f"/PPA_RCAT064_{params.sr_folio}" + "_2.DAT"

# 1. Guarda el DataFrame como texto en una carpeta temporal
    temp_path = final_file_path.replace('.csv', '_temp')  # Cambia la extensión para evitar conflictos
    #  asi solo genera formato unix LF al final
    #  df_final.coalesce(1).write.mode("overwrite").text(temp_path)
    #  genera en formato windows con CRLF
    df_final.coalesce(1)
            .write.mode("overwrite")
            .option("header","false")
            .option("sep",",")
            .option("encoding","UTF-8")
            .option("lineSep", "\r\n")
            .text(temp_path)
df_final.coalesce(1)
    .write.mode("overwrite")
    .option("header","false")
    .option("sep",",")
    .option("encoding","UTF-8")
    .option("lineSep", "\r\n")
    .text(temp_path
# 2. Mueve y renombra el archivo que se genera
# La ruta del archivo generado señalado por el sistema será algo así como temp_path/part-00000-...
    import glob

# Busca el archivo en la carpeta temporal
    import os

    # Listar archivos generados en la carpeta temporal
    temp_files = dbutils.fs.ls(temp_path)

    # Verificamos si hay archivos y movemos el primero encontrado
    if temp_files:
        for file_info in temp_files:
            # Asegúrate de que solo mueves el archivo correcto
            if file_info.name.endswith(".txt"):
                dbutils.fs.mv(file_info.path, final_file_path)
                break

    # 3. Elimina la carpeta temporal que se creó
    dbutils.fs.rm(temp_path, recurse=True)

    # 4. Espera 30 segundos para asegurar que el archivo este escrito correctamente y evitar que el servicio de busqueda tome un archivo incompleto
    import time
    time.sleep(30)
    # 5. Renombra el archivo final listo para que lo tome el servicio de busqueda
    dbutils.fs.mv(final_file_path, definitive_file_path)

# COMMAND ----------

(df_final.coalesce(1)
.write.mode("overwrite")
    .option("header","false")
    .option("sep",",")
    .option("encoding","UTF-8")
    .option("lineSep", "\r\n")
    .csv(temp_path))

# COMMAND ----------

# DBTITLE 1,Lista los archivos de la ruta
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/INF/')
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/INF/201912172142010002_Matriz_Convivencia.csv/')

# COMMAND ----------

# DBTITLE 1,Elimina archivos
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/#params.sr_folio_#20250603_001.DAT.md5', recurse=True)

# COMMAND ----------

# DBTITLE 1,Mueve a Catalog Local
#En caso de que se requiera validar el archivo en la maquina local
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/202004130851050001_Matriz_Convivencia.CSV', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
