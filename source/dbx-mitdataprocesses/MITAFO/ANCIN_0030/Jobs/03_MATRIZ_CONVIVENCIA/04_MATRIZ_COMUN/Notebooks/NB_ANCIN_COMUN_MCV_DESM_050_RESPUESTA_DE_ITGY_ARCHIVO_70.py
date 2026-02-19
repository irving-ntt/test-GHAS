# Databricks notebook source
# MAGIC %md
# MAGIC ## NB_ANCIN_COMUN_MCV_DESM_050_RESPUESTA_DE_ITGY_ARCHIVO_70

# COMMAND ----------

""" 
Descripcion:
     Lectura de archivo 070 (respuesta del archivo 69) y no fueron desmarcados.
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
Tablas input:
   
Tablas output:
    N/A
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

# 🚀 **Ejemplo de Uso**
conf = ConfManager()
#Archivos SQL
query = QueryManager()
#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# Define la ruta del archivo 70 
# #p_PATH_INTEGRITY_ARC##p_SR_NOM_RESPUESTA##p_SR_FOLIO##p_CLV_SUFIJO_3#.DAT
#path_arch_70 =  f"{params.sr_path_arch}"+ "ArchivosOK" + f"/PPA_RCAT070_{params.sr_folio}" + "_3.DAT"

path_arch_70 =  f"{params.sr_path_arch}"+ f"/PPA_RCAT070_{params.sr_folio}" + "_3.DAT"

# COMMAND ----------

# DBTITLE 1,LOAD FILE FUNCTION esta da un warning
#def load_file(storage_location, path_arch_65):    
    # df = spark.read.text(storage_location + sr_path_arch)
#    df = spark.read.text(storage_location + path_arch_65)
#    df = df.withColumnRenamed("value", "FTC_LINEA")
#    indexed_df = df.withColumn("idx", monotonically_increasing_id() + 1)
#    w = Window.orderBy("idx")
#    indexed_df = indexed_df.withColumn("FTN_NO_LINEA", row_number().over(w))
#    indexed_df = indexed_df.withColumnRenamed("_c0", "FTC_LINEA")
#    indexed_df = indexed_df.drop("idx")
    
#    return indexed_df


# COMMAND ----------

ruta = SETTINGS.GENERAL.EXTERNAL_LOCATION + f"{params.sr_path_arch}" 

# Listar los archivos y carpetas en la ruta especificada
archivos = dbutils.fs.ls(ruta)

# Inicializar la variable
nombre_encontrado = None

# Buscar si existe "ArchivosOK" o "archivosOK" y asignar el valor encontrado
for f in archivos:
    nombre = f.name.rstrip('/')
    if nombre in ["ArchivosOK", "archivosOK"]:
        nombre_encontrado = nombre
        break

if nombre_encontrado:
    print(f"Existe '{nombre_encontrado}' en la ruta.")
else:
    print("No existe 'ArchivosOK' ni 'archivosOK' en la ruta.")


# COMMAND ----------

# DBTITLE 1,LOAD FILE FUNCTION
#from pyspark.sql.functions import monotonically_increasing_id

def load_file(storage_location, path_arch_70):    
    # Leer el archivo de texto
    df = spark.read.text(storage_location + path_arch_70)
    
    # Renombrar la columna "value" a "FTC_LINEA"
    df = df.withColumnRenamed("value", "FTC_LINEA")
    
    # Agregar un número de línea único
    indexed_df = df.withColumn("FTN_NO_LINEA", monotonically_increasing_id() + 1)
    
    return indexed_df

# COMMAND ----------

# DBTITLE 1,CARGA EL ARCHIVO EN UN DATAFRAME
# dfLoaded = load_file(SETTINGS.GENERAL.EXTERNAL_LOCATION, f"{params.sr_path_arch}"+ "/archivosOk" + f"/PPA_RCAT065_{params.sr_folio}" + "_2.DAT")
dfLoaded = load_file(SETTINGS.GENERAL.EXTERNAL_LOCATION, path_arch_70)
if conf.debug:
    display(dfLoaded)

# COMMAND ----------

# DBTITLE 1,LISTA ARCHIVOS
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/archivosOK/')
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK/')
#dbutils.fs.ls('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT070_202506250000364910_3.DAT')

# dbutils.fs.ls(final_file_path)

# lista los archivos
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK')
# dbutils.fs.ls(final_file_path)
#En caso de que se requiera validar el archivo en la maquina local
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# dbutils.fs.cp(final_file_path, '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# elimina el archivo
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', recurse=True)
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2_2.DAT', recurse=True)
# copiamos los nuevos 70
dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT070_202208241640309239_3.DAT','abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')


#abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK/PPA_RCAT070_202506250000364910_3.DAT
dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')


# COMMAND ----------

# DBTITLE 1,Creacion de tabla delta con los datos del archivo cargados
delta_001 = f"TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_{params.sr_folio}"
db.write_delta(delta_001, dfLoaded, "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_{params.sr_folio}"))


# COMMAND ----------

# DBTITLE 1,QUERY  PARA DETALLE -- > PROCESOS.THAFOGRAL_MAR_DES_ITGY
# Query para los datos para TTAFOGRAL_RESPUESTA_ITGY 
statement_RI_DET = query.get_statement(
    "COMUN_MCV_DESM_050_EXTRAE_INFO_RESPUESTA_70_DET.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_{params.sr_folio}",
 #   FECHA_PROCESO = fecha_actual
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_DET_{params.sr_folio}", db.sql_delta(statement_RI_DET), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_DET_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,ELIMINA E INSERTA EN THAFOGRAL_MAR_DES_ITGY

#  -------------------------------------------------------------------------------------
# Eliminamos registros en THAFOGRAL_MAR_DES_ITGY 
# Datastage elimina y despues inserta 
# COMUN_MCV_150_DB_20_DEL_PROCESOS_THAFOGRAL_MAR_DES_ITGY.sql
#  PROCESOS.THAFOGRAL_MAR_DES_ITGY
# ELIMINA REGISTROS DEL FOLIO EN respuesta THAFOGRAL_MAR_DES_ITGY
statement_del_mar_des = query.get_statement(
    "COMUN_MCV_150_DB_20_DEL_PROCESOS_THAFOGRAL_MAR_DES_ITGY.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement_del_mar_des, async_mode=False
)

#INSERTA EN RESPUESTA INTEGRITY AUX

table_name = "PROCESOS.THAFOGRAL_MAR_DES_ITGY"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_DET_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# DBTITLE 1,QUERY PARA UPDATE A TTAFOGRAL RESPUESTA INTGY
# Query para los datos para TTAFOGRAL_RESPUESTA_ITGY 
statement_RI_ENC = query.get_statement(
    "COMUN_MCV_DESM_050_EXTRAE_INFO_RESPUESTA_70_ENC.sql",
    SR_FOLIO=params.sr_folio,
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_{params.sr_folio}",
 #   FECHA_PROCESO = fecha_actual
 )

# COMMAND ----------

# DBTITLE 1,INSERTA EN TABLA DELTA EL ENCAB LOCALIZADO
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_ENC_{params.sr_folio}", db.sql_delta(statement_RI_ENC), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_ENC_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,UPDATE A TTAFOGRAL RESPUESTA INTGY

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

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_ENC_{params.sr_folio}"), table_name, "default", "append")

# REALIZA MERGE TTAFOGRAL_RESPUESTA_ITGY_AUX Y TTAFOGRAL_RESPUESTA_ITGY
statement = query.get_statement(
    "COMUN_MCV_150_DB_10_MERGE_TTAFOGRAL_RESPUESTA_ITGY_AUX_UPD.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)


# COMMAND ----------

# DBTITLE 1,QUERY PARA DF DE LOS NO DESMARCADOS
# COMUN_MCV_DESM_050_EXTRAE_INFO_RESPUESTA_70_NO_DESMARCADOS.sql
statement_RI_NODESM = query.get_statement(
    "COMUN_MCV_DESM_050_EXTRAE_INFO_RESPUESTA_70_NO_DESMARCADOS.sql",
    SR_FOLIO=params.sr_folio,
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_DESM_RESP_ITGY_ARCH_70_{params.sr_folio}", 
 )

# sql_delta --> para consultar tablas delta con un query
# read_delta --> para hacerle un select * from a una tabla delta sin necesidad de usar query

df_from_delta=db.sql_delta(statement_RI_NODESM)

if conf.debug:
    display(df_from_delta)



# COMMAND ----------

# MAGIC %md
# MAGIC  GENERAR ARCHIVO CSV CON LOS QUE NO SE DESMARCARON EN INTEGRITY 

# COMMAND ----------

# df_from_delta
if len(df_from_delta.take(1)) == 1:
    # Define la ruta final donde quieres guardar el archivo .dat
    final_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/'+ f"/ANT_{params.sr_folio}" + "_DESMARCA_INTEGRITY.CSV"
    definitive_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + '/INTEGRITY/'+ f"/{params.sr_folio}" + "_DESMARCA_INTEGRITY.CSV"

# 1. Guarda el DataFrame como texto en una carpeta temporal
    temp_path = final_file_path.replace('.CSV', '_temp')  # Cambia la extensión para evitar conflictos
    #  asi solo genera formato unix LF al final
    #  df_final.coalesce(1).write.mode("overwrite").text(temp_path)
    #  genera en formato windows con CRLF
    # df_from_delta.coalesce(1).write.mode("overwrite").option("lineSep", "\r\n").text(temp_path)

    # Escribe un CSV (posible múltiples part- files; usamos coalesce(1) si queremos un único archivo)
    (df_from_delta
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("lineSep", "\r\n") \
        .option("encoding", "UTF-8") \
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
 
    # 4. Renombra el archivo final listo para que lo tome el servicio de busqueda
    dbutils.fs.mv(final_file_path, definitive_file_path)

# COMMAND ----------

# lista los archivos temp_path
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# dbutils.fs.ls(final_file_path)
# dbutils.fs.ls(temp_path)
#En caso de que se requiera validar el archivo en la maquina local
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# dbutils.fs.cp(final_file_path, '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
# elimina el archivo
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT065_202004130851050001_2.DAT', recurse=True)
# dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ANT_PPA_RCAT064_202004130851050001_2_2.DAT', recurse=True)
