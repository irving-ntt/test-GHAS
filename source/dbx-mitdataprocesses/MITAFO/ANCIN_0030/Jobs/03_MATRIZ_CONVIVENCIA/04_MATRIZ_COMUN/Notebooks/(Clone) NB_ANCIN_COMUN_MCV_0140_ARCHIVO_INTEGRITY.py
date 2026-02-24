# Databricks notebook source
""" 
Descripcion:
    140 GENERA INFORMACION PARA ARCHIVO INTEGRITY 
    lee la informacion de  MATRIZ y genera tabla delta con el resultado 
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

# Query para los datos para el archivo integrity
statement_001 = query.get_statement(
    "COMUN_MCV_140_TD_08_DB_EXTRAE_INFO_ARCHIVO_INTEGRITY.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

# DBTITLE 1,Almacenamos los datos del query en una tabla delta
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}", db.read_data("default", statement_001), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}"))
#  -------------------------------------------------------------------------------------


# COMMAND ----------

# DBTITLE 1,Pasamos el resultado de la tabla delta a un df
df = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}")
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,Datos del query a dataframe eliminar
df = db.read_data("default", statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,extraccion a un data frame eliminar
df = db.read_data("default", statement_001) if conf.debug else None; display(df) if conf.debug else None 

# COMMAND ----------

# DBTITLE 1,Verifica que tenga resultados el dataframe
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

if df.limit(1).count() == 0:
    #    Notify.send_notification("INFO", params)
    #    dbutils.notebook.exit("No hay información para generar el archivo.")
    # Query para los datos para el archivo integrity 65
    statement_002 = query.get_statement(
        "COMUN_MCV_140_TD_08_DB_EXTRAE_INFO_DUMMY_ARCH_65.sql",
        # SR_FOLIO=params.sr_folio, NO NECESITAMOS FOLIO
        hints="/*+ PARALLEL(8) */",
    )
    db.write_delta(
        f"TEMP_DELTA_COMUN_MCV_DELTA_09_ARCHIVO_65_{params.sr_folio}",
        db.read_data("default", statement_002),
        "overwrite",
    )
    if conf.debug:
        display(
            db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_09_ARCHIVO_65_{params.sr_folio}")
        )
    # 
    df2 = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_09_ARCHIVO_65_{params.sr_folio}")
    if conf.debug:
        display(df2)
    # #p_PATH_INTEGRITY#ANT_PPA_RCAT065_#p_SR_FOLIO#_2#p_EXT_ARC_DAT#
    full_file_name = (
        conf.external_location
        + conf.err_repo_path
        # + "/ANT_PPA_RCAT065_"
        + f"/ANT_PPA_RCAT065_{params.sr_folio}"
        # f"/{params.sr_folio}"
        #    + fecha_actual
        + "_2.DAT"
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
    file_manager.generar_archivo_ctindi(
        df_final=df2,  # DataFrame que se va a guardar
        full_file_name=full_file_name,
        header=conf.header,
        calcular_md5=True,  # Cambia a False si no quieres calcular el MD5
    # fala moverlo a la ruta 
    #sleep 30;mv #$RT_PATH_INTEGRITY#ANT_PPA_RCAT065_#$SR_FOLIO#_2#$EXT_ARC_DAT# #$RT_PATH_INTEGRITY#PPA_RCAT065_#$SR_FOLIO#_2#$EXT_ARC_DAT#
    )
else:
     # #p_PATH_INTEGRITY#ANT_PPA_RCAT064_#p_SR_FOLIO#_2#p_EXT_ARC_DAT#
    full_file_name = (
        conf.external_location
        + conf.err_repo_path
        # + "/ANT_PPA_RCAT064_"
        + f"/ANT_PPA_RCAT064_{params.sr_folio}"
        # f"/{params.sr_folio}"
        #    + fecha_actual
        + "_2.DAT"
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
    file_manager.generar_archivo_ctindi(
        df_final=df,  # DataFrame que se va a guardar
        full_file_name=full_file_name,
        header=conf.header,
        calcular_md5=True,  # Cambia a False si no quieres calcular el MD5
    )
    #         mv #$RT_PATH_INTEGRITY#ANT_PPA_RCAT064_#$SR_FOLIO#_2#$EXT_ARC_DAT# #$RT_PATH_INTEGRITY#PPA_RCAT064_#$SR_FOLIO#_2#$EXT_ARC_DAT#
    #sleep 30;mv #$RT_PATH_INTEGRITY#ANT_PPA_RCAT065_#$SR_FOLIO#_2#$EXT_ARC_DAT# #$RT_PATH_INTEGRITY#PPA_RCAT065_#$SR_FOLIO#_2#$EXT_ARC_DAT#


# COMMAND ----------

# DBTITLE 1,Datos del dataframe a tabla delta BORRAR
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}", db.read_data("default", statement_001), "overwrite")

# COMMAND ----------

# DBTITLE 1,Datos del query a tabla delta BORRAR
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}", db.read_data("default", statement_001), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}"))
#  -------------------------------------------------------------------------------------


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

# #p_PATH_INTEGRITY#ANT_PPA_RCAT065_#p_SR_FOLIO#_2#p_EXT_ARC_DAT# ARCHIVO VACIO 0 REGISTROS EN DATASET
# #p_PATH_INTEGRITY#ANT_PPA_RCAT064_#p_SR_FOLIO#_2#p_EXT_ARC_DAT# ARCHIVO CUANDO EXISTEN REGISTROS EN DATASET
# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + f"/{params.sr_folio}"
    #    + fecha_actual
        + "_Matriz_Convivencia.CSV"
    )
#full_file_name_md5 = (
#        conf.external_location
#        + conf.err_repo_path
#        + "/MD5_TEST1_"
#        + fecha_actual
#        + "_001.DAT.md5"
#    )    
# EL PATH QUEDA IGUAL     
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

#En caso de que se requiera validar el archivo en la maquina local
#dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/CTINDI1_20250521_001.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
#En caso de que se requiera validar el archivo en la maquina local
#dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/TEST1_20250603_001.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

# COMMAND ----------

# DBTITLE 1,Generacion del archivo
# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=conf.header,
    calcular_md5=True  # Cambia a False si no quieres calcular el MD5
)

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

# DBTITLE 1,Lista los archivos de la ruta
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')

# COMMAND ----------

# DBTITLE 1,Elimina archivos
dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/_ANT_PPA_RCAT065_201907091556490001_2.DAT.md5', recurse=True)

# COMMAND ----------

# DBTITLE 1,Mueve a Catalog Local
#En caso de que se requiera validar el archivo en la maquina local
dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/201907091556490001_Matriz_Convivencia.CSV', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
