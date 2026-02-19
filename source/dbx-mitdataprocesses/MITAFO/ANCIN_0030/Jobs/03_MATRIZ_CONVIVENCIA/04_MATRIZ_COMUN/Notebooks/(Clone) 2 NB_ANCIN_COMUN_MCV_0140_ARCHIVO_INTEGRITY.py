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

# DBTITLE 1,QUERY DATOS ARCHIVO INTEGRITY
# Query para los datos para el archivo integrity, solo los que conviven FTB_ESTATUS_MARCA = 1
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

# DBTITLE 1,Verifica los registros del dataframe y genera archivo 64 o 65
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

full_path = (
   "/Volumes/"
    + SETTINGS.GENERAL.CATALOG
    + "/"
    + SETTINGS.GENERAL.SCHEMA
    + "/doimss_carga_archivo/CTTEST_"
    + fecha_actual
    + "_001"
)

# SI NO CONVIVEN LAS CUENTAS (DF=0) SE GENERA EL ARCHIVO 65 

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
  
    file_manager.generar_archivo_ctindi(
        df_final=df2,  # DataFrame df2 dummy 
        full_file_name=full_file_name,
        header=conf.header,
        calcular_md5=True,  # Cambia a False si no quieres calcular el MD5
    # fala moverlo a la ruta 
    #sleep 30;mv #$RT_PATH_INTEGRITY#ANT_PPA_RCAT065_#$SR_FOLIO#_2#$EXT_ARC_DAT# #$RT_PATH_INTEGRITY#PPA_RCAT065_#$SR_FOLIO#_2#$EXT_ARC_DAT#
    )
else:
    # EN CASO DE TENER 1 REGISTRO O MAS SE GENERA EL ARCHIVO 64  
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
  
    file_manager.generar_archivo_ctindi(
        df_final=df,  # DataFrame que se va a guardar
        full_file_name=full_file_name,
        header=conf.header,
        calcular_md5=True,  # Cambia a False si no quieres calcular el MD5
    )
    #         mv #$RT_PATH_INTEGRITY#ANT_PPA_RCAT064_#$SR_FOLIO#_2#$EXT_ARC_DAT# #$RT_PATH_INTEGRITY#PPA_RCAT064_#$SR_FOLIO#_2#$EXT_ARC_DAT#
    #sleep 30;mv #$RT_PATH_INTEGRITY#ANT_PPA_RCAT065_#$SR_FOLIO#_2#$EXT_ARC_DAT# #$RT_PATH_INTEGRITY#PPA_RCAT065_#$SR_FOLIO#_2#$EXT_ARC_DAT#


# COMMAND ----------

full_path

# COMMAND ----------

# DBTITLE 1,Genereacion del archivo 64
# QueryS generacion archivo 64  Remueve los duplicados de los que no conviven (NO_CONVIV) 
#   120 MATRIZ CARGO
#
#  1 Extraemos las tablas de sub proceso y map nic itgy
#
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

#full_path = (
#   "/Volumes/"
#    + SETTINGS.GENERAL.CATALOG
#    + "/"
#    + SETTINGS.GENERAL.SCHEMA
#    + "/doimss_carga_archivo/CTTEST_"
#    + fecha_actual
#    + "_001"
# )

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

# ---------------------- QUERY ENCABEZADO  ---------------------------------------------------------------------------------------
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

# ---------------------------- QUERY DETALLE  ------------------------------------------------------------------------------
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

# DATOS TAMBIEN A DELTA FINAL
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}", db.sql_delta(statement_64_DET), "append")
# ----------------------- QUERY TOTALES ------------------------------------------------------------------------------------------------
statement_64_TOT = query.get_statement(
    "COMUN_MCV_145_TD_11_GENERA_ARCHIVO_64_TOTALES.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}",
     FECHA_PROCESO = fecha_actual,
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_ARCHIVO_64_TOT_{params.sr_folio}", db.sql_delta(statement_64_TOT), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_ARCHIVO_64_TOT_{params.sr_folio}"))

# DATOS TAMBIEN A DELTA FINAL
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}", db.sql_delta(statement_64_TOT), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}"))

# ------------------------- terminam querys --  ----------------------------------------------------------------------------------
    full_file_name2 = (
        conf.external_location
        + conf.err_repo_path
        + "/ANT_PPA_RCAT064_"
        #+ f"/ANT_PPA_RCAT064_{params.sr_folio}"  # es el usado 
        + f"{params.sr_folio}"
        #    + fecha_actual
        + "_2_2.DAT"
    )
  
    file_manager.generar_archivo_ctindi(
        df_final=df,  # DataFrame que se va a guardar
        full_file_name=full_file_name,
        header=conf.header,
        calcular_md5=True,  # Cambia a False si no quieres calcular el MD5
    )
#   -- Query para ordenar los datos a presentar 
statement_64_FIN_ORDER = query.get_statement(
    "COMUN_MCV_145_TD_12_GENERA_ARCHIVO_64_FORMAT.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}",
 )

# DATOS  A DELTA FINAL FMT 
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}", db.sql_delta(statement_64_FIN_ORDER), "overwrite") 

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}"))

df_final64 = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}")
if conf.debug:
    display(df_final64)
# 
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
  
file_manager.generar_archivo_ctindi(
    df_final=df_final64,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
#    header=conf.header,
    header=False,
    calcular_md5=False,  # Cambia a False si no quieres calcular el MD5
)

# lista los archivos
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/PROCESAR/')
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/1_ARCH_6664_temp/')

# elimina
dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/1_ARCH_6664_temp/', recurse=True)

#En caso de que se requiera validar el archivo en la maquina local
dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/1_ARCH_6664.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

# codigo de Mariel
#   temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
#    df_final.coalesce(1).write.mode("overwrite").text(temp_path)

if len(df_final.take(1)) == 1:
    # Define la ruta final donde quieres guardar el archivo .dat
    final_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/1_ARCH_6664.DAT'
# 1. Guarda el DataFrame como texto en una carpeta temporal
    temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
    df_final.coalesce(1).write.mode("overwrite").text(temp_path)
# ------------------------------------------------------------------------------------------------------------------
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/1_ARCH_6664_temp/')
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# 1. Guarda el DataFrame como texto en una carpeta temporal
    temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
    df_final.coalesce(1).write.mode("overwrite").text(temp_path)
# 2. Mueve y renombra el archivo que se genera
# La ruta del archivo generado señalado por el sistema será algo así como temp_path/part-00000-...
    import glob
# Busca el archivo en la carpeta temporal
    import os
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net//INTEGRITY/1_ARCH_6664_temp')


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

    # full_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + 'RCDI/NCI/PROCESAR/' + 'PREFT.DP.A01534.CONFAC.GDG.DAT'

    # Genera el archivo y decide si calcular MD5 o no
    # file_manager.generar_archivo_ctindi(
    #     df_final=df_final,  # DataFrame que se va a guardar
    #     full_file_name=full_path,
    #     header=False,
    #     calcular_md5=False  # Cambia a False si no quieres calcular el MD5
    # )
# --- ***********************************************************************************************************
#temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
temp_path = full_file_name
df_final=df_final64
# escriturq de Mariel 
df_final.coalesce(1).write.mode("overwrite").text(final_file_path)

df_final.coalesce(1).write.mode("overwrite").text(final_file_path)
df_final.write.format("csv").option("header", "true").save(final_file_path)

df_final.coalesce(1).write.format('csv').option('header', 'true').save(final_file_path)

if conf.debug:
    display(df_final)
# termina codigo de Mariel 
# GAIA   *************************************************************************************
# Convertir el DataFrame a una lista de listas
data = [list(row) for row in df_final.collect()]
if conf.debug:
    display(data)

# Función para ajustar el texto a una longitud fija
def ajustar_longitud(texto, longitud=128):
    return texto.ljust(longitud)[:longitud]  # Rellena con espacios y corta si es necesario

# Definir la ruta completa del archivo
full_file_name = 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2.DAT'  # Cambia esto a la ruta deseada

# Exportar los datos a un archivo .txt con longitud fija
with open(full_file_name, 'w') as f:
    for row in data:
        # Ajustar cada valor de la fila a la longitud fija y unirlos
        linea = ''.join(ajustar_longitud(str(valor)) for valor in row)
        f.write(linea + '\n')  # Escribir la línea en el archivo

# Comentarios:
# - Se convierte el DataFrame a una lista de listas usando 'values.tolist()'.
# - Se define una función 'ajustar_longitud' para asegurar que cada valor tenga 128 caracteres.
# - Se itera sobre cada fila en la lista 'data'.
# - Se unen los valores de cada fila en una sola línea y se escribe en el archivo.
# - Se utiliza 'with' para manejar el archivo, asegurando que se cierre correctamente.
# - 'full_file_name' contiene la ruta completa donde se guardará el archivo .txt.




# COMMAND ----------

# DBTITLE 1,Genereacion del archivo 64
# QueryS generacion archivo 64  Remueve los duplicados de los que no conviven (NO_CONVIV) 
#   120 MATRIZ CARGO
#
#  1 Extraemos las tablas de sub proceso y map nic itgy
#
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

#full_path = (
#   "/Volumes/"
#    + SETTINGS.GENERAL.CATALOG
#    + "/"
#    + SETTINGS.GENERAL.SCHEMA
#    + "/doimss_carga_archivo/CTTEST_"
#    + fecha_actual
#    + "_001"
# )

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

# ---------------------- QUERY ENCABEZADO  ---------------------------------------------------------------------------------------
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

# ---------------------------- QUERY DETALLE  ------------------------------------------------------------------------------
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

# DATOS TAMBIEN A DELTA FINAL
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}", db.sql_delta(statement_64_DET), "append")
# ----------------------- QUERY TOTALES ------------------------------------------------------------------------------------------------
statement_64_TOT = query.get_statement(
    "COMUN_MCV_145_TD_11_GENERA_ARCHIVO_64_TOTALES.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}",
     FECHA_PROCESO = fecha_actual,
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_ARCHIVO_64_TOT_{params.sr_folio}", db.sql_delta(statement_64_TOT), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_ARCHIVO_64_TOT_{params.sr_folio}"))

# DATOS TAMBIEN A DELTA FINAL
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}", db.sql_delta(statement_64_TOT), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}"))

# ------------------------- terminam querys --  ----------------------------------------------------------------------------------
    full_file_name2 = (
        conf.external_location
        + conf.err_repo_path
        + "/ANT_PPA_RCAT064_"
        #+ f"/ANT_PPA_RCAT064_{params.sr_folio}"  # es el usado 
        + f"{params.sr_folio}"
        #    + fecha_actual
        + "_2_2.DAT"
    )
  
    file_manager.generar_archivo_ctindi(
        df_final=df,  # DataFrame que se va a guardar
        full_file_name=full_file_name,
        header=conf.header,
        calcular_md5=True,  # Cambia a False si no quieres calcular el MD5
    )
#   -- Query para ordenar los datos a presentar 
statement_64_FIN_ORDER = query.get_statement(
    "COMUN_MCV_145_TD_12_GENERA_ARCHIVO_64_FORMAT.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_15_ARCHIVO_64_FINAL_{params.sr_folio}",
 )

# DATOS  A DELTA FINAL FMT 
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}", db.sql_delta(statement_64_FIN_ORDER), "overwrite") 

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}"))

df_final64 = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_ARCHIVO_64_FINAL_FMT_{params.sr_folio}")
if conf.debug:
    display(df_final64)
# 
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
  
file_manager.generar_archivo_ctindi(
    df_final=df_final64,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
#    header=conf.header,
    header=False,
    calcular_md5=False,  # Cambia a False si no quieres calcular el MD5
)

# lista los archivos
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/PROCESAR/')
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/1_ARCH_6664_temp/')

# elimina
dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/1_ARCH_6664_temp/', recurse=True)

#En caso de que se requiera validar el archivo en la maquina local
dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/1_ARCH_6664.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

# codigo de Mariel
#   temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
#    df_final.coalesce(1).write.mode("overwrite").text(temp_path)

if len(df_final.take(1)) == 1:
    # Define la ruta final donde quieres guardar el archivo .dat
    final_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/1_ARCH_6664.DAT'
# 1. Guarda el DataFrame como texto en una carpeta temporal
    temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
    df_final.coalesce(1).write.mode("overwrite").text(temp_path)
# ------------------------------------------------------------------------------------------------------------------
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/1_ARCH_6664_temp/')
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# 1. Guarda el DataFrame como texto en una carpeta temporal
    temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
    df_final.coalesce(1).write.mode("overwrite").text(temp_path)
# 2. Mueve y renombra el archivo que se genera
# La ruta del archivo generado señalado por el sistema será algo así como temp_path/part-00000-...
    import glob
# Busca el archivo en la carpeta temporal
    import os
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net//INTEGRITY/1_ARCH_6664_temp')


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

    # full_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + 'RCDI/NCI/PROCESAR/' + 'PREFT.DP.A01534.CONFAC.GDG.DAT'

    # Genera el archivo y decide si calcular MD5 o no
    # file_manager.generar_archivo_ctindi(
    #     df_final=df_final,  # DataFrame que se va a guardar
    #     full_file_name=full_path,
    #     header=False,
    #     calcular_md5=False  # Cambia a False si no quieres calcular el MD5
    # )
# --- ***********************************************************************************************************
#temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
temp_path = full_file_name
df_final=df_final64
# escriturq de Mariel 
df_final.coalesce(1).write.mode("overwrite").text(final_file_path)

df_final.coalesce(1).write.mode("overwrite").text(final_file_path)
df_final.write.format("csv").option("header", "true").save(final_file_path)

df_final.coalesce(1).write.format('csv').option('header', 'true').save(final_file_path)

if conf.debug:
    display(df_final)
# termina codigo de Mariel 
# GAIA   *************************************************************************************
# Convertir el DataFrame a una lista de listas
data = [list(row) for row in df_final.collect()]
if conf.debug:
    display(data)

# Función para ajustar el texto a una longitud fija
def ajustar_longitud(texto, longitud=128):
    return texto.ljust(longitud)[:longitud]  # Rellena con espacios y corta si es necesario

# Definir la ruta completa del archivo
full_file_name = 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2.DAT'  # Cambia esto a la ruta deseada

# Exportar los datos a un archivo .txt con longitud fija
with open(full_file_name, 'w') as f:
    for row in data:
        # Ajustar cada valor de la fila a la longitud fija y unirlos
        linea = ''.join(ajustar_longitud(str(valor)) for valor in row)
        f.write(linea + '\n')  # Escribir la línea en el archivo

# Comentarios:
# - Se convierte el DataFrame a una lista de listas usando 'values.tolist()'.
# - Se define una función 'ajustar_longitud' para asegurar que cada valor tenga 128 caracteres.
# - Se itera sobre cada fila en la lista 'data'.
# - Se unen los valores de cada fila en una sola línea y se escribe en el archivo.
# - Se utiliza 'with' para manejar el archivo, asegurando que se cierre correctamente.
# - 'full_file_name' contiene la ruta completa donde se guardará el archivo .txt.




# COMMAND ----------



# COMMAND ----------

file_info

# COMMAND ----------

final_file_path

# COMMAND ----------

temp_path

# COMMAND ----------

# DBTITLE 1,Codigo del asistente
# Define the path where you want to save the text file
output_path = "/mnt/your_mount_point/your_directory/your_file.txt"

# Export the DataFrame to a text file
df_final64.coalesce(1).write.mode("overwrite").text(output_path)

# If you want to display the DataFrame for debugging purposes
if conf.debug:
    display(df_final64)

# COMMAND ----------

# DBTITLE 1,Actualiza TTAFOGRAL RESPUESTA ITGY
#  Generacion de tabla y actualizacion de la tabla PROCESOS.TTAFOGRAL_RESPUESTA_ITGY
#  ---------------------- QUERY ENCABEZADO Y  TOTALES ---------------------------------------------------------------------------------------

from datetime import datetime
import pytz
# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# tabla origen para los datos
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

# Cargamos a tabla en CIERREN_DATAUX.TTAFOGRAL_RESPUESTA_ITGY_AUX

# ELIMINA REGISTROS DEL FOLIO EN respuesta integrity aux
statement = query.get_statement(
    "COMUN_MCV_145_DB_09_DEL_TTAFOGRAL_RESPUESTA_ITGY_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)
    
#df = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_09_ARCHIVO_64_{params.sr_f

#INSERTA EN RESPUESTA INTEGRITY AUX

table_name = "CIERREN_DATAUX.TTAFOGRAL_RESPUESTA_ITGY_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_14_INFO_A_RESPUESTA_ITGY_{params.sr_folio}"), table_name, "default", "append")

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
dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_201907091556490001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
