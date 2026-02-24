# Databricks notebook source
""" 
Descripcion:
     Procesa la respuesta del archivo integrity 65, Actualiza el número de registros recibidos en la tabla de control  TTAFOGRAL_RESPUESTA_ITGY, resguarda la información de la desmarca en integrity en la tabla  THAFOGRAL_MAR_DES_ITGY   y generara el archivo con los registros que  no fueron desmarcados.
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

# 🚀 **Ejemplo de Uso**
conf = ConfManager()
#Archivos SQL
query = QueryManager()
#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# Define la ruta del archivo 65 
#path_arch_65 =  f"{params.sr_path_arch}"+ "ArchivosOK" + f"/PPA_RCAT065_{params.sr_folio}" + "_2.DAT"
path_arch_65 =  f"{params.sr_path_arch}"

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
# copiamos el archivo a archivos Ok
# dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT065_202501091933016835_2.DAT','abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK/PPA_RCAT065_202501091933016835_2.DAT')
# 
#dbutils.fs.ls('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT065_202501091933016835_2.DAT')
#dbutils.fs.ls('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT065_202501091933016835_2.DAT')
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK/')
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK/')
#dbutils.fs.mkdirs('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK')
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK/PPA_RCAT065_202501091933016835_2.DAT')


# COMMAND ----------

# DBTITLE 1,LOAD FILE FUNCTION
#from pyspark.sql.functions import monotonically_increasing_id

def load_file(storage_location, path_arch_65):    
    # Leer el archivo de texto
    df = spark.read.text(storage_location + path_arch_65)
    
    # Renombrar la columna "value" a "FTC_LINEA"
    df = df.withColumnRenamed("value", "FTC_LINEA")
    
    # Agregar un número de línea único
    indexed_df = df.withColumn("FTN_NO_LINEA", monotonically_increasing_id() + 1)
    
    return indexed_df

# COMMAND ----------

# DBTITLE 1,CARGA EL ARCHIVO EN UN DATAFRAME
# dfLoaded = load_file(SETTINGS.GENERAL.EXTERNAL_LOCATION, f"{params.sr_path_arch}"+ "/archivosOk" + f"/PPA_RCAT065_{params.sr_folio}" + "_2.DAT")
dfLoaded = load_file(SETTINGS.GENERAL.EXTERNAL_LOCATION, path_arch_65)
if conf.debug:
    display(dfLoaded)

# COMMAND ----------

# DBTITLE 1,LISTA ARCHIVOS
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')
# dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/archivosOk')
# dbutils.fs.ls(final_file_path)

# COMMAND ----------

# DBTITLE 1,Creacion de tabla delta con los datos del archivo cargados
delta_001 = f"TEMP_DELTA_COMUN_MCV_DELTA_15_RESPUESTA_INTEGRITY_{params.sr_folio}"
db.write_delta(delta_001, dfLoaded, "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_RESPUESTA_INTEGRITY_{params.sr_folio}"))


# COMMAND ----------

# DBTITLE 1,QUERY - PARA TTAFOGRAL RESPUESTA DETALLE
# Query para los datos para TTAFOGRAL_RESPUESTA_ITGY 
statement_RI_DET = query.get_statement(
    "COMUN_MCV_150_TD_01_EXTRAE_DETALLE_DE_ARCHIVO_RESPUESTA.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_15_RESPUESTA_INTEGRITY_{params.sr_folio}",
 #   FECHA_PROCESO = fecha_actual
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_RESPUESTA_INTEGRITY_DET_{params.sr_folio}", db.sql_delta(statement_RI_DET), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_RESPUESTA_INTEGRITY_DET_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,ELIMINA E INSERTA EN THAFOGRAL_MAR_DES_ITGY
# Eliminamos registros en THAFOGRAL_MAR_DES_ITGY -- aqui nos quedamos 
# Datastage elimina y despues inserta 
# COMUN_MCV_150_DB_20_DEL_PROCESOS_THAFOGRAL_MAR_DES_ITGY.sql

# ELIMINA REGISTROS DEL FOLIO EN respuesta THAFOGRAL_MAR_DES_ITGY
statement_del_mar_des = query.get_statement(
    "COMUN_MCV_150_DB_20_DEL_PROCESOS_THAFOGRAL_MAR_DES_ITGY.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement_del_mar_des, async_mode=False
)

#INSERTA EN PROCESOS.THAFOGRAL_MAR_DES_ITGY 

table_name = "PROCESOS.THAFOGRAL_MAR_DES_ITGY"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_15_RESPUESTA_INTEGRITY_DET_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# DBTITLE 1,QUERY PARA UPDATE A TTAFOGRAL RESPUESTA INTGY
# Query para los datos para TTAFOGRAL_RESPUESTA_ITGY 
statement_RI_ENC = query.get_statement(
    "COMUN_MCV_150_TD_02_EXTRAE_ENCAB_DE_ARCHIVO_RESPUESTA.sql",
    SR_FOLIO=params.sr_folio,
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_15_RESPUESTA_INTEGRITY_{params.sr_folio}",
 #   FECHA_PROCESO = fecha_actual
 )

# COMMAND ----------

# DBTITLE 1,UPDATE A TTAFOGRAL RESPUESTA INTGY
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_RESPUESTA_INTEGRITY_ENC_{params.sr_folio}", db.sql_delta(statement_RI_ENC), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_RESPUESTA_INTEGRITY_ENC_{params.sr_folio}"))

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

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_RESPUESTA_INTEGRITY_ENC_{params.sr_folio}"), table_name, "default", "append")

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

CleanUpManager.cleanup_notebook(locals())
