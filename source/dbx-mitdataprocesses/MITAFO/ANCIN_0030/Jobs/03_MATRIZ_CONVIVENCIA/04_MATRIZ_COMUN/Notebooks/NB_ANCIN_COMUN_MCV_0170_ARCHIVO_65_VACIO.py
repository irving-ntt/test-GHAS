# Databricks notebook source
""" 
Descripcion:
    170 GENERA INFORMACION PARA ARCHIVO 65 VACIO
    genera el archivo de respuesta de marca de integrity cuando no convive ningun registro en NCI
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
Tablas input:
    N/A       
Tablas output:
    N/A
Tablas Delta:
    TEMP_DELTA_COMUN_MCV_DELTA_09_ARCHIVO_65_{params.sr_folio}
Archivos SQL:
   COMUN_MCV_170_TD_08_DB_EXTRAE_INFO_DUMMY_ARCH_65.sql
    """

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
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

# DBTITLE 1,GENERA DATAFRAME PARA ARCHIVO 65
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# SE GENERA EL ARCHIVO 65 PORQUE NO HUBO REGISTROS MARCADOS EN MATRIZ CONVIVENVCIA Y SE INDICO CONVIVENCIA CON INTEGRITY
statement_002 = query.get_statement(
    "COMUN_MCV_170_TD_08_DB_EXTRAE_INFO_DUMMY_ARCH_65.sql",
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
# SE PASA A DATAFRAME 
# df2 = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_09_ARCHIVO_65_{params.sr_folio}")
# df_final_65 = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_09_ARCHIVO_65_{params.sr_folio}")
df_final = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_09_ARCHIVO_65_{params.sr_folio}")

# if conf.debug:
#    display(df_final_65)

if conf.debug:
    display(df_final)

# COMMAND ----------

# DBTITLE 1,se agrega CRLF
from pyspark.sql.functions import concat, lit
# no se usa este codigo, en su lugar se aplica en    df_final.coalesce(1).write.mode("overwrite").option("lineSep", "\r\n").text(temp_path)
# Agrega un salto de línea (puedes usar '\n' para LF o '\r\n' para CRLF)
# df_final = df_final_65.withColumn("DETALLE", concat(col("DETALLE"), lit("\r\n")))  # Cambia '\n' por '\r\n' si necesitas CRLF
# df_final = df_final.withColumn("DETALLE", concat(col("DETALLE"), lit("\r")))  # Cambia '\n' por '\r\n' si necesitas CRLF


# COMMAND ----------

# DBTITLE 1,GENERACION DEL ARCHIVO

# longitud = len(df_final.take(1))
if len(df_final.take(1)) == 1:
    # Define la ruta final donde quieres guardar el archivo .dat
    #final_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/'+ f"/ANT_PPA_RCAT065_{params.sr_folio}" + "_2.DAT"
    final_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + f"{params.sr_path_arch}" + f"/ANT_PPA_RCAT065_{params.sr_folio}" + "_2.DAT"

    # Define la ruta y nombre definitivo .dat
    definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + f"{params.sr_path_arch}" + f"/PPA_RCAT065_{params.sr_folio}" + "_2.DAT"


# 1. Guarda el DataFrame como texto en una carpeta temporal
    temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
    #  asi solo genera formato unix LF al final
    #  df_final.coalesce(1).write.mode("overwrite").text(temp_path)
    #  genera en formato windows con CRLF
    df_final.coalesce(1).write.mode("overwrite").option("lineSep", "\r\n").text(temp_path)

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

# DBTITLE 1,LISTA Y ELIMINA PRUEBAS
# lista los archivos
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# dbutils.fs.ls(final_file_path)
#En caso de que se requiera validar el archivo en la maquina local
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# elimina el archivo
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', recurse=True)
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2_2.DAT', recurse=True)


# COMMAND ----------

# DBTITLE 1,Elimina archivos
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/_ANT_PPA_RCAT065_201907091556490001_2.DAT.md5', recurse=True)

# COMMAND ----------

# DBTITLE 1,Mueve a Catalog Local
#En caso de que se requiera validar el archivo en la maquina local
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_201907091556490001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
