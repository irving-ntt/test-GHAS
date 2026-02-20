# Databricks notebook source
# DBTITLE 1,Framework load
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parametros
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

# DBTITLE 1,MANEJADOR SQL Y VARIABLES
conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Jobs parametros
dbutils.jobs.taskValues.set(key='sr_proceso', value=params.sr_proceso)
dbutils.jobs.taskValues.set(key='sr_subproceso', value=params.sr_subproceso)
dbutils.jobs.taskValues.set(key='sr_subetapa', value=params.sr_subetapa)
dbutils.jobs.taskValues.set(key='sr_folio', value=params.sr_folio)
dbutils.jobs.taskValues.set(key='sr_usuario', value=params.sr_usuario)
dbutils.jobs.taskValues.set(key='sr_tipo_mov', value=params.sr_tipo_mov)
dbutils.jobs.taskValues.set(key='sr_conv_ingty', value=params.sr_conv_ingty)
dbutils.jobs.taskValues.set(key='sr_path_arch', value=params.sr_path_arch)
dbutils.jobs.taskValues.set(key='sr_tipo_ejecucion', value=params.sr_tipo_ejecucion)

# COMMAND ----------

# DBTITLE 1,QUERY PARA VERIFICAR CUANTOS SE MARCARON
# Query para los datos para el archivo integrity, solo los que conviven FTB_ESTATUS_MARCA = 1
statement_001 = query.get_statement(
    "COMUN_MCV_140_TD_08_DB_EXTRAE_INFO_ARCHIVO_INTEGRITY.sql",
    SR_PROCESO=params.sr_proceso,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

# DBTITLE 1,RESULTADOS A TABLA DELTA
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}", db.read_data("default", statement_001), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}"))
#  -------------------------------------------------------------------------------------

# COMMAND ----------

# DBTITLE 1,ALMACENAMOS TAMBIEN EN UN DATAFRAME
df = db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_08_ARCHIVO_INTEGRITY_{params.sr_folio}")
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,DECIDE SI GENERA 64 O 065
var_existe_marca = None

if df.limit(1).count() == 0:
    #SI NO EXISTEN REGISTROS CON CONVIVENVIA EN MATRIZ SE GENERA DIRECTAMENTE EL ARCHIVO 65 
    var_existe_marca = "NO_EXISTE_MARCA"
else:
    # si existen registros con marca se genera archivo 64
    var_existe_marca = "EXISTE_MARCA"
    
dbutils.jobs.taskValues.set(key='var_existe_marca', value=var_existe_marca)


# COMMAND ----------

# DBTITLE 1,Info var_existe_marca
logger.info(f"var_existe_marca: {var_existe_marca}")
