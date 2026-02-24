# Databricks notebook source
# DBTITLE 1,DOCUMENTACION
""" 
Descripcion:
    140 GENERA INFORMACION PARA ARCHIVO INTEGRITY 
    Basado en JP_PANCIN_MCV_0140_ARCH_ITGY
    lee la informacion de MATRIZ y genera tabla delta para con esta generar el archivo de intercambio 64 para Integrity 
    Inserta/actualiza PROCESOS.TTAFOGRAL_RESPUESTA_ITGY
Subetapa: 
    25 - Matriz de Convivencia
Trámite:
    COMUN - MATRIZ
Tablas input:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
    CIERREN_ETL.TLSISGRAL_ETL_BUC  
    CIERREN.TMSISGRAL_MAP_NCI_ITGY     
Tablas output:
    CIERREN_DATAUX.TTAFOGRAL_RESPUESTA_ITGY_AUX
    PROCESOS.TTAFOGRAL_RESPUESTA_ITGY
Tablas Delta:
    TEMP_DELTA_COMUN_MCV_DELTA_10_SUB_PROCESO_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_11_PROCESO_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}"
Archivos SQL:
    COMUN_MCV_140_TD_08_DB_EXTRAE_INFO_ARCHIVO_INTEGRITY.sql
   
    """

# COMMAND ----------

# MAGIC %md
# MAGIC %md NB_ANCIN_COMUN_MCV_0140_ARCHIVO_INTEGRITY - 20251216
# MAGIC
# MAGIC Cambia la nomenlcatura del archivo 64 a 73

# COMMAND ----------

# DBTITLE 1,Cargar framework
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

# DBTITLE 1,Valida Parametros
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

# DBTITLE 1,Cargar configuraciones locales
conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,QUERY DATOS ARCHIVO INTEGRITY
# Query para los datos para el archivo integrity, solo los que conviven FTB_ESTATUS_MARCA = 1
statement_001 = query.get_statement(
    "COMUN_MCV_140_TD_08_DB_EXTRAE_INFO_ARCHIVO_INTEGRITY.sql",
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
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
df_final = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}")
if conf.debug:
    display(df_final)

# COMMAND ----------

# DBTITLE 1,PROCESO, SUB PROCESO Y MAP NCI INTEGRITY

#  1 Extraemos las tablas de sub proceso y map nic itgy
# SUB PROCESO --------------------------------------------------------------------------------------
statement_sp = query.get_statement(
    "COMUN_MCV_145_DB_100_EXT_INFO_SUBPROCESO.sql",
    sr_folio=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_10_SUB_PROCESO_{params.sr_folio}", db.read_data("default", statement_sp), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_10_SUB_PROCESO_{params.sr_folio}"))
# PROCESO --------------------------------------------------------------------------------------
statement_p = query.get_statement(
    "COMUN_MCV_145_DB_200_EXT_INFO_PROCESO.sql",
    sr_folio=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_11_PROCESO_{params.sr_folio}", db.read_data("default", statement_p), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_11_PROCESO_{params.sr_folio}"))



# COMMAND ----------

# MAGIC %md
# MAGIC VERSION CON QUERY DE PROCESOS
# MAGIC

# COMMAND ----------

# DBTITLE 1,PROCESO Y SUBPROCESO DE PROCESOS.TCSISGRAL_MAP_NCI_ITGY_PROCESO
"""
#  1 Extraemos PROCESO Y SUB PROCESO DE PROCESOS.TCSISGRAL_MAP_NCI_ITGY_PROCESO
# SUB PROCESO --------------------------------------------------------------------------------------
statement_sp = query.get_statement(
    "COMUN_MCV_145_DB_100_EXT_INFO_PROCESO_Y_SUBPROCESO.sql",
    sr_folio=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
"""

# COMMAND ----------

# DBTITLE 1,ENCABEZADO

#  QUERY ENCABEZADO  E INICIALIZA DELTA PARA ARCH 64
from datetime import datetime
import pytz
# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")
# Genera la fecha actual en el formato YYYYMMDD, se usa en el query del encabezado 
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

statement_64_ENC = query.get_statement(
    "COMUN_MCV_145_TD_11_GENERA_ARCHIVO_64_ENC.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}",
    FECHA_PROCESO = fecha_actual
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_13_ARCHIVO_64_ENC_{params.sr_folio}", db.sql_delta(statement_64_ENC), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_13_ARCHIVO_64_ENC_{params.sr_folio}"))

# DATOS TAMBIEN A DELTA FINAL -- e inicializa la tabla delta final 
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}", db.sql_delta(statement_64_ENC), "overwrite")

# COMMAND ----------

# DBTITLE 1,DETALLE

#  QUERY DETALLE Y APPEND A  DELTA PARA ARCH 64
statement_64_DET = query.get_statement(
    "COMUN_MCV_145_TD_10_GENERA_ARCHIVO_64_DET.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}",
    DELTA_TABLA_SUB = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_10_SUB_PROCESO_{params.sr_folio}",
    DELTA_TABLA_PRO = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_11_PROCESO_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_12_ARCHIVO_64_DET_{params.sr_folio}", db.sql_delta(statement_64_DET), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_12_ARCHIVO_64_DET_{params.sr_folio}"))

# DATOS TAMBIEN A DELTA FINAL -- APPEND
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}", db.sql_delta(statement_64_DET), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,TOTALES
#  QUERY TOTALES Y APPEND A  DELTA PARA ARCH 64
statement_64_TOT = query.get_statement(
    "COMUN_MCV_145_TD_11_GENERA_ARCHIVO_64_TOTALES.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}",
     FECHA_PROCESO = fecha_actual,
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_ARCHIVO_64_TOT_{params.sr_folio}", db.sql_delta(statement_64_TOT), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_ARCHIVO_64_TOT_{params.sr_folio}"))

# DATOS TAMBIEN A DELTA FINAL -- APPEND
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}", db.sql_delta(statement_64_TOT), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}"))


# COMMAND ----------

# DBTITLE 1,ORDENA LOS DATOS Y DATAFRAME FINAL
# -- Query para ordenar  los datos a presentar 
statement_64_FIN_ORDER = query.get_statement(
    "COMUN_MCV_145_TD_12_GENERA_ARCHIVO_64_FORMAT.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}",
 )

# DATOS  A DELTA FINAL FMT 
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}", db.sql_delta(statement_64_FIN_ORDER), "overwrite") 

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}"))
# DATOS A DATAFRAME FINAL 
df_final = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}")
if conf.debug:
    display(df_final)

# COMMAND ----------

# DBTITLE 1,GENERAMOS ARCHIVO
# longitud = len(df_final.take(1))
if len(df_final.take(1)) == 1:
    # Define la ruta final donde quieres guardar el archivo .dat
    #final_file_path      = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/'+ f"/ANT_PPA_RCAT064_{params.sr_folio}" + "_2.DAT"
    #definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/'+ f"/PPA_RCAT064_{params.sr_folio}" + "_2.DAT"
    #final_file_path      = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/PROCESS/DBX/'+ f"/ANT_PPA_RCAT064_{params.sr_folio}" + "_2.DAT"
    #definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/PROCESS/DBX/'+ f"/PPA_RCAT064_{params.sr_folio}" + "_2.DAT"
    final_file_path       = SETTINGS.GENERAL.EXTERNAL_LOCATION + f"{params.sr_path_arch}PROCESS/DBX/" + f"/ANT_PPA_RCAT073_{params.sr_folio}" + "_2.DAT"
    definitive_file_path  = SETTINGS.GENERAL.EXTERNAL_LOCATION + f"{params.sr_path_arch}PROCESS/DBX/" + f"/PPA_RCAT073_{params.sr_folio}" + "_2.DAT"

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

    # 4. Espera 30 segundos para asegurar que el archivo este escrito correctamente y evitar que el servicio de busqueda tome un archivo incompleto, asi esta en datastage
    import time
    time.sleep(30)
    # 5. Renombra el archivo final listo para que lo tome el servicio de busqueda
    dbutils.fs.mv(final_file_path, definitive_file_path)


# COMMAND ----------

final_file_path
definitive_file_path

# COMMAND ----------

# DBTITLE 1,LISTA Y ELIMINA
# lista los archivos
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# dbutils.fs.ls(final_file_path)
# dbutils.fs.ls(definitive_file_path)
#En caso de que se requiera validar el archivo en la maquina local
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# dbutils.fs.cp(final_file_path, '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# elimina el archivo
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', recurse=True)
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2_2.DAT', recurse=True)


# COMMAND ----------

# DBTITLE 1,GENERA DELTA PARA CIERREN_DATAUX.TTAFOGRAL_RESPUESTA_ITGY_AUX
#  Generacion de tabla y actualizacion de la tabla PROCESOS.TTAFOGRAL_RESPUESTA_ITGY
#  
from datetime import datetime
import pytz
# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")
# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")
# tabla origen DELTA para los datos
if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}"))

statement_RESP_INTGY = query.get_statement(
    "COMUN_MCV_145_TD_09_GENERA_INF_A_RESPUESTA_ITGY.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}",
    FECHA_PROCESO = fecha_actual,
    SR_FOLIO = params.sr_folio,
    SR_PROCESO = params.sr_proceso,
    SR_SUBPROCESO = params.sr_subproceso,
    SR_USUARIO = params.sr_usuario,
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_INFO_A_RESPUESTA_ITGY_{params.sr_folio}", db.sql_delta(statement_RESP_INTGY), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_INFO_A_RESPUESTA_ITGY_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,ELIMINAMOS Y CARGA INFO A CIERREN_DATAUX.TTAFOGRAL_RESPUESTA_ITGY_AUX
# Cargamos a tabla en CIERREN_DATAUX.TTAFOGRAL_RESPUESTA_ITGY_AUX
# ELIMINA REGISTROS DEL FOLIO EN respuesta integrity aux
statement = query.get_statement(
    "COMUN_MCV_150_DB_09_DEL_TTAFOGRAL_RESPUESTA_ITGY_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)
  
#INSERTA EN RESPUESTA INTEGRITY AUX
table_name = "CIERREN_DATAUX.TTAFOGRAL_RESPUESTA_ITGY_AUX"
db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_INFO_A_RESPUESTA_ITGY_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# DBTITLE 1,MERGE TTAFOGRAL_RESPUESTA_ITGY_AUX Y TTAFOGRAL_RESPUESTA_ITGY

# REALIZA MERGE TTAFOGRAL_RESPUESTA_ITGY_AUX Y TTAFOGRAL_RESPUESTA_ITGY
statement = query.get_statement(
    "COMUN_MCV_145_DB_10_MERGE_TTAFOGRAL_RESPUESTA_ITGY_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)


# COMMAND ----------

# DBTITLE 1,|
CleanUpManager.cleanup_notebook(locals())

# COMMAND ----------

Notify.send_notification("INFO", params)
