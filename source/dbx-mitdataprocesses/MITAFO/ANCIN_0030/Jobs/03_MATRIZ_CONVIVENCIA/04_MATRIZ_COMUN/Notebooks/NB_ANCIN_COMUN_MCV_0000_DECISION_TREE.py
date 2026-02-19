# Databricks notebook source
# DBTITLE 1,Framework load
# MAGIC %run "./startup"

# COMMAND ----------

# MAGIC %md
# MAGIC Se estandarizan los parametros de tipo de ejecucion para ser los mismos de PRE MATRIZ - 2025 08 06
# MAGIC - Anterior    -->    Actual      Tipo de Ejecucion
# MAGIC - 1    -->             0           Carga Inicial
# MAGIC - 2    -->             1           Reproceso
# MAGIC - 3    -->             2           Procesar Archivo Integrity
# MAGIC

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
dbutils.jobs.taskValues.set(key='sr_etapa', value=params.sr_etapa)
dbutils.jobs.taskValues.set(key='sr_instancia_proceso', value=params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key='sr_id_snapshot', value=params.sr_id_snapshot)
dbutils.jobs.taskValues.set(key='sr_paso', value=params.sr_paso)

# COMMAND ----------

# DBTITLE 1,Set var_tipo_ejecucion
var_tipo_ejecucion = None  # Inicializamos la variable
var_genera_64 = None  # Inicializamos la variable
var_valida_convivencia = None # Iniclializamos la variable

# Verificamos las combinaciones de TIPO DE EJECUCION
if params.sr_tipo_ejecucion == '0' or params.sr_tipo_ejecucion == '1':
    var_valida_convivencia = "VALIDA_CONVIVENCIA"
    if params.sr_tipo_mov == '180':
        var_tipo_ejecucion = "PROCESA_CARGO"
    elif params.sr_tipo_mov == '181':
        var_tipo_ejecucion = "PROCESA_ABONO"
    else:
        logger.error("sr_tipo_mov no válido para determinar var_tipo_ejecucion Cargo o Abono")
elif params.sr_tipo_ejecucion == '2':
    var_valida_convivencia = "NO_VALIDA_CONVIVENCIA"
    var_tipo_ejecucion = "RESPUESTA_ITGY"
else:
    logger.error("sr_tipo_ejecucion no válido para determinar var_tipo_ejecucion")
    raise ValueError("sr_tipo_ejecucion no válido para determinar var_tipo_ejecucion")
# Generacion de archivo de convivencia integrity, en caso de no generar archivo se va directamente a la generacion del csv
if params.sr_conv_ingty == '1':
    var_genera_64 = "GENERA_ARCHIVO_64"
elif params.sr_conv_ingty == '0':
    var_genera_64 = "NO_GENERA_ARCHIVO_64"
else:
    logger.error("sr_conv_ingty valor no válido para determinar si genera archivo 64 y/o convive con Integrity y determinar var_genera_64")
# Si var_tipo_ejecucion es válido, lo establecemos en los valores de tarea
dbutils.jobs.taskValues.set(key='var_tipo_ejecucion', value=var_tipo_ejecucion)
dbutils.jobs.taskValues.set(key='var_genera_64', value=var_genera_64)
dbutils.jobs.taskValues.set(key='var_valida_convivencia', value=var_valida_convivencia)

# COMMAND ----------

# DBTITLE 1,Info var_tipo_ejecucion
logger.info(f"var_tipo_ejecucion: {var_tipo_ejecucion}")
logger.info(f"var_genera_64: {var_genera_64}")
logger.info(f"var_valida_convivencia: {var_valida_convivencia}")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
