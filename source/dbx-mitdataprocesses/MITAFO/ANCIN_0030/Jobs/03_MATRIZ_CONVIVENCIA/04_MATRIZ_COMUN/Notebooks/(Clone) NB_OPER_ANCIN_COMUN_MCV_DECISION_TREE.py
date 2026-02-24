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

# COMMAND ----------

# DBTITLE 1,Set var_tipo_ejecucion
var_tipo_ejecucion = None  # Inicializamos la variable

# Verificamos las combinaciones de TIPO DE EJECUCION
if params.sr_tipo_ejecucion == '1':
    if params.sr_tipo_mov == '180':
        var_tipo_ejecucion = "INICIAL_CARGO"
    elif params.sr_tipo_mov == '181':
        var_tipo_ejecucion = "INICIAL_ABONO"
    else:
        logger.error("sr_tipo_mov no válido para determinar var_tipo_ejecucion")
elif params.sr_tipo_ejecucion == '2':
    if params.sr_tipo_mov == '180':
        var_tipo_ejecucion = "REPROCESO_CARGO"
    elif params.sr_tipo_mov == '181':
        var_tipo_ejecucion = "REPROCESO_ABONO"
    else:
        logger.error("sr_tipo_mov no válido para determinar var_tipo_ejecucion")
elif params.sr_tipo_ejecucion == '3':
    var_tipo_ejecucion = "RESPUESTA_ITGY"
else:
    logger.error("sr_tipo_ejecucion no válido para determinar var_tipo_ejecucion")
    raise ValueError("sr_tipo_ejecucion no válido para determinar var_tipo_ejecucion")

# Verificamos las combinaciones de TIPO DE EJECUCION
if params.sr_tipo_ejecucion == '1':
    if params.sr_tipo_mov == '180':
        var_tipo_ejecucion = "INICIAL_CARGO"
    elif params.sr_tipo_mov == '181':
        var_tipo_ejecucion = "INICIAL_ABONO"
    else:
        logger.error("sr_tipo_mov no válido para determinar var_tipo_ejecucion")
elif params.sr_tipo_ejecucion == '2':
    if params.sr_tipo_mov == '180':
        var_tipo_ejecucion = "REPROCESO_CARGO"
    elif params.sr_tipo_mov == '181':
        var_tipo_ejecucion = "REPROCESO_ABONO"
    else:
        logger.error("sr_tipo_mov no válido para determinar var_tipo_ejecucion")
elif params.sr_tipo_ejecucion == '3':
    var_tipo_ejecucion = "RESPUESTA_ITGY"
else:
    logger.error("sr_tipo_ejecucion no válido para determinar var_tipo_ejecucion")
    raise ValueError("sr_tipo_ejecucion no válido para determinar var_tipo_ejecucion")

# Si var_tipo_ejecucion es válido, lo establecemos en los valores de tarea
dbutils.jobs.taskValues.set(key='var_tipo_ejecucion', value=var_tipo_ejecucion)

# COMMAND ----------

# DBTITLE 1,Info var_tipo_ejecucion
logger.info(f"var_tipo_ejecucion: {var_tipo_ejecucion}")
