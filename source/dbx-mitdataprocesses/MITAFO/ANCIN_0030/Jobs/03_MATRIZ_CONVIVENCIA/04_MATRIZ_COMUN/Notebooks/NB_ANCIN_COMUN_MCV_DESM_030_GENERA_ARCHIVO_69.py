# Databricks notebook source
""" 
Descripcion:
    NB_ANCIN_COMUN_MCV_DESM_030_GENERA_ARCHIVO_69
Subetapa: 
    DESMARCA DE CUENTAS
Trámite:
    COMUN - 
Tablas input:
    TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01      
Tablas output:
    TTAFOGRAL_RESPUESTA_ITGY
Tablas Delta:
    N/A
Archivos SQL:
    ATRAN_DESM_030_0300_INFO_PARA_DESMARCA_TIPO_PROCESO.sql
    ATRAN_DESM_030_0400_INFO_PARA_DESMARCA_TIPO_SUB_PROCESO.sql
    ATRAN_DESM_030_0500_INFO_ARCHIVO_69_ENC.sql
    ATRAN_DESM_030_0600_INFO_ARCHIVO_69_DET.sql
    ATRAN_DESM_030_0700_INFO_ARCHIVO_69_ORDENA_FMT.sql
    ATRAN_DESM_030_0800_INFO_ARCHIVO_69_SUMARIO.sql
    ATRAN_DESM_030_0900_DEL_TTAFOGRAL_RESPUESTA_ITGY.sql
    ATRAN_DESM_030_1000_INFO_TTAFOGRAL_RESPUESTA_ITGY.sql
   
    """

# COMMAND ----------

# DBTITLE 1,Cargar Framework 🚀
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

# DBTITLE 1,Definir y validar parametros
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_path_arch": str,
    "sr_bandera": str,
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

# aplicamos las comparaciones necesarias para determinar la busqueda en la tabla CIERREN.TMSISGRAL_MAP_NCI_ITGY y los datos
# comparaciones tomadas de la secuencia JQ_PATRIF_DMC_0010_DESMARCA al ejecutar el job JP_PATRIF_0030_DESMARCA_ARC
#
# -- If $SR_SUBPROCESO=347 Then 354 Else  $SR_SUBPROCESO P_SR_SUBPROCESO
# -- If $SR_SUBPROCESO=347 Then $SR_SUBPROCESO Else 0 --> P_SR_PROCESO_DESM
# -- If $SR_SUBPROCESO = 363  Then "PROCESOS ISSSTE" Else  "PROCESOS"  --> P_DES_PROCESO
# -- If $SR_SUBPROCESO = 354 AND $SR_BANDERA = 0 Then $CLV_SUFIJO_09 Else $CLV_SUFIJO_01

# If $SR_SUBPROCESO=347 Then 354 Else  $SR_SUBPROCESO --> P_SR_SUBPROCESO
if params.sr_subproceso == "347":
    P_SR_SUBPROCESO = 354
else:
    P_SR_SUBPROCESO = params.sr_subproceso

# -- If $SR_SUBPROCESO=347 Then $SR_SUBPROCESO Else 0 --> P_SR_PROCESO_DESM
if params.sr_subproceso == "347":
    P_SR_PROCESO_DESM = params.sr_subproceso
else:
    P_SR_PROCESO_DESM = 0

# -- If $SR_SUBPROCESO = 363  Then "PROCESOS ISSSTE" Else  "PROCESOS"  --> P_DES_PROCESO
if params.sr_subproceso == "363":
    P_DES_PROCESO = "PROCESOS ISSSTE"
else:
    P_DES_PROCESO = "PROCESOS" 

# -- If $SR_SUBPROCESO = 354 AND $SR_BANDERA = 0 Then $CLV_SUFIJO_09 Else $CLV_SUFIJO_01
if params.sr_subproceso == "354" and params.sr_bandera == "0":
    P_CLV_SUFIJO = "09"
else:
    P_CLV_SUFIJO = "01"

# COMMAND ----------

# DBTITLE 1,BUSCAMOS LOS DATOS PARA EL PROCESO Y SUBPROCESO
# Extrae DATOS DE MAP NCI con los datos de la variable
# Datos almacenados en TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_03 
# TIPO PROCESO
statement_desmc_tipo_proceso = query.get_statement(
    "ATRAN_DESM_030_0300_INFO_PARA_DESMARCA_TIPO_PROCESO.sql",
    SR_PROCESO=params.sr_proceso,
    DES_PROCESO=P_DES_PROCESO,
    hints="/*+ PARALLEL(8) */",
)

# TIPO SUB PROCESO
statement_desmc_tipo_sub_proceso = query.get_statement(
    "ATRAN_DESM_030_0400_INFO_PARA_DESMARCA_TIPO_SUB_PROCESO.sql",
    P_SR_SUBPROCESO=P_SR_SUBPROCESO,
    hints="/*+ PARALLEL(8) */",
)



# COMMAND ----------

# DBTITLE 1,ALMACENAMOS LOS DATOS EN UNA DELTA PROCESO
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_03_PROCESO_{params.sr_folio}", db.read_data("default", statement_desmc_tipo_proceso), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_03_PROCESO_{params.sr_folio}"))
#  -------------------------------------------------------------------------------------

# COMMAND ----------

# DBTITLE 1,ALMACENAMOS LOS DATOS DEL SUB PROCESO
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_04_SUB_PROCESO_{params.sr_folio}", db.read_data("default", statement_desmc_tipo_sub_proceso), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_04_SUB_PROCESO_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,ENCABEZADO ARCHIVO 69
# Prepara los datos para el archivo 69, Datos almacenados en TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01, 03 y 04 

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}"))
# ---
#  QUERY ENCABEZADO  E INICIALIZA DELTA PARA ARCH 64
from datetime import datetime
import pytz
# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")
# Genera la fecha actual en el formato YYYYMMDD, se usa en el query del encabezado 
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")
# Obtener el timestamp actual con fecha y hora en la zona horaria de México
timestamp_actual = datetime.now(mexico_tz).strftime("%Y-%m-%d %H:%M:%S")

statement_69_ENC = query.get_statement(
    "ATRAN_DESM_030_0500_INFO_ARCHIVO_69_ENC.sql",
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}",
    FECHA_PROCESO = fecha_actual
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_ENC_{params.sr_folio}", db.sql_delta(statement_69_ENC), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_ENC_{params.sr_folio}"))

# DATOS TAMBIEN A DELTA FINAL -- e inicializa la tabla delta final 
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_{params.sr_folio}", db.sql_delta(statement_69_ENC), "overwrite")

# COMMAND ----------

# DBTITLE 1,DETALLE ARCHIVO 69

#  QUERY DETALLE Y APPEND A  DELTA PARA ARCH 69
statement_69_DET = query.get_statement(
    "ATRAN_DESM_030_0600_INFO_ARCHIVO_69_DET.sql",
    sr_folio=params.sr_folio,
    sr_proceso=params.sr_proceso,
    sr_subproceso=params.sr_subproceso,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}",
    DELTA_TABLA_SUB = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_04_SUB_PROCESO_{params.sr_folio}",
    DELTA_TABLA_PRO = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_03_PROCESO_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_DET_{params.sr_folio}", db.sql_delta(statement_69_DET), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_DET_{params.sr_folio}"))

# DATOS TAMBIEN A DELTA FINAL -- APPEND
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_{params.sr_folio}", db.sql_delta(statement_69_DET), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,SUMARIO (TOTALES) ARCH 69
#  QUERY TOTALES Y APPEND A  DELTA PARA ARCH 64
statement_69_TOT = query.get_statement(
    "ATRAN_DESM_030_0800_INFO_ARCHIVO_69_SUMARIO.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}",
     FECHA_PROCESO = fecha_actual,
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_SUM_{params.sr_folio}", db.sql_delta(statement_69_TOT), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_SUM_{params.sr_folio}"))

# DATOS TAMBIEN A DELTA FINAL -- APPEND
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_{params.sr_folio}", db.sql_delta(statement_69_TOT), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,ORDENA LOS DATOS
# -- Query para ordenar  los datos a presentar 
statement_69_FIN_ORDER = query.get_statement(
    "COMUN_MCV_145_TD_12_GENERA_ARCHIVO_64_FORMAT.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_{params.sr_folio}",
 )

# DATOS  A DELTA FINAL FMT 
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_ORD_{params.sr_folio}", db.sql_delta(statement_69_FIN_ORDER), "overwrite") 

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_ORD_{params.sr_folio}"))


# COMMAND ----------

# DBTITLE 1,DATAFRAME FINAL
# DATOS A DATAFRAME FINAL 
df_final = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_ORD_{params.sr_folio}")
if conf.debug:
    display(df_final)

# COMMAND ----------

# longitud = len(df_final.take(1))
if len(df_final.take(1)) == 1:
    # Define la ruta final donde quieres guardar el archivo .dat
    # primero con el prefijo ANT -- ruta modificada 17 sep de 2025 por solicitud de Rodolfo y Susana
    # final_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + f"{params.sr_path_arch}" + f"/ANT_PPA_RCAT069_{params.sr_folio}" + "_3.DAT"
    final_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/PROCESS/DBX/' + f"/ANT_PPA_RCAT069_{params.sr_folio}" + "_3.DAT"

    # Define la ruta y nombre definitivo .dat (ya sin prefijo ANT) -- ruta modificada 17 sep de 2025 por solicitud de Rodolfo y Susana
    # definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + f"{params.sr_path_arch}" + f"/PPA_RCAT069_{params.sr_folio}" + "_3.DAT"
    definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/PROCESS/DBX/' + f"/PPA_RCAT069_{params.sr_folio}" + "_3.DAT"

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

# lista los archivos
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# dbutils.fs.ls(final_file_path)
#En caso de que se requiera validar el archivo en la maquina local
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# dbutils.fs.cp(final_file_path, '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# elimina el archivo
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', recurse=True)
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2_2.DAT', recurse=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Aqui termina la generacion del archivo, sigue el delete y el insert a la tabla

# COMMAND ----------

# MAGIC %md
# MAGIC ## insertamos registros en PROCESOS.TTAFOGRAL_RESPUESTA_ITGY (previo delete por folio)

# COMMAND ----------

# DBTITLE 1,ELIMINAMOS POR FOLIO EN PROCESOS.TTAFOGRAL_RESPUESTA_ITGY
# ELIMINA REGISTROS DEL FOLIO EN PROCESOS.TTAFOGRAL_RESPUESTA_ITGY
statement = query.get_statement(
    "ATRAN_DESM_030_0900_DEL_TTAFOGRAL_RESPUESTA_ITGY.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)


# COMMAND ----------

# DBTITLE 1,PREPARAMOS INFO PARA TTAFOGRAL_RESPUESTA_ITGY

timestamp_actual = datetime.now(mexico_tz).strftime("%Y-%m-%d %H:%M:%S")

# preparamos info con una delta para insertar en respuesta integrity
statement_resp_itgy = query.get_statement(
    "ATRAN_DESM_030_1000_INFO_TTAFOGRAL_RESPUESTA_ITGY.sql",
    sr_folio=params.sr_folio,
    sr_proceso=params.sr_proceso,
    sr_subproceso=params.sr_subproceso,
    sr_usuario=params.sr_usuario,
    sr_proceso_desm=P_SR_PROCESO_DESM,
    sr_fecha_creacion=timestamp_actual,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}",
)

# DATOS  A DELTA PARA CARGA
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_05_RESP_ITGY_{params.sr_folio}", db.sql_delta(statement_resp_itgy), "overwrite") 

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_05_RESP_ITGY_{params.sr_folio}"))

#INSERTA EN RESPUESTA INTEGRITY 

table_name = "PROCESOS.TTAFOGRAL_RESPUESTA_ITGY"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_05_RESP_ITGY_{params.sr_folio}"), table_name, "default", "append")


