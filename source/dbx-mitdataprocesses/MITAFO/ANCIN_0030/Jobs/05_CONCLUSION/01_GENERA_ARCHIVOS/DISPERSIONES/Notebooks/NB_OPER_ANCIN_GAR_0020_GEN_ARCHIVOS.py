# Databricks notebook source
# DBTITLE 1,Descripción
'''
Descripcion:
    Realiza la validación del subproceso que se va a ejecutar para determinar que tipo de archivo debe generar (CTINDI,PPA_RCDI o SVIC1)
Subetapa:
    GENERACIÓN DE ARCHIVOS
Trámite:
    8 - DO IMSS
    118 - ACLARACIONES ESPECIALES
    439 - GUBER IMSS
    121 - INTERESES IMSS
    120 - DO ISSSTE
    210 - GUBER ISSSTE
    122 - INTERESES ISSSTE
Tablas INPUT:
    N/A
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    N/A
'''

# COMMAND ----------

# DBTITLE 1,Incio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Párametros
params = WidgetParams(
    {
        "sr_folio": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_subetapa": str,
        "sr_sec_lote": str,
        "sr_fecha_lote": str,
        "sr_fecha_acc": str,
        "sr_tipo_archivo": str,
        "sr_estatus_mov": str,
        "sr_tipo_mov": str,
        "sr_accion": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str, # es obligatorio para servicios
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Validaciónes para Generar Archivos
stage = 0
# Generación de Archivos CTINDI - Contiene todos los registros del folio de la dispersión
#Subproceso 8 = Dispersión Ordinaria IMSS
#Subproceso 439 = Dispersión Gubernamental IMSS
if (params.sr_proceso == '7') and (params.sr_subproceso == '8' or params.sr_subproceso == '439' or params.sr_subproceso == '118' or params.sr_subproceso == '121' or params.sr_subproceso == '120' or params.sr_subproceso == '122' or params.sr_subproceso == '210') and (params.sr_subetapa == '428') and (params.sr_tipo_archivo == '425'): 
    stage = "CTINDI"
# Generación de Archivos PPA_RCDI - Contiene solo aquellos registros que traen credito de vivienda para el folio de la dispersión
elif (params.sr_subproceso == '8' or params.sr_subproceso == '118' or params.sr_subproceso == '120') and (params.sr_subetapa == '978') and (params.sr_tipo_archivo == '426'):
    stage = "PPA_RCDI"
# Generación de Archivos SVIC1 - Contiene los registros de vivienda de las diferentes dispersiones. Los que se hayan agrupado en el mes.
elif (params.sr_subproceso == '9' or params.sr_subproceso == '105') and (params.sr_subetapa == '428') and (params.sr_tipo_archivo == '425'):
    stage = "SVIC1"
else:
    logger.error("Parametros de entrada no validos para generar Archivos CTINDI")
    raise ValueError("Parametros de entrada no validos para generar Archivos CTINDI")

# Obtiene todos los parámetros de entrada desde los widgets de Databricks.
input_parameters = dbutils.widgets.getAll().items()
dbutils.jobs.taskValues.set(key = "", value = stage)
logger.info(f"params: {input_parameters}")
logger.info(f"sr_stage: {stage}")

dbutils.jobs.taskValues.set(key="sr_stage", value=stage)
dbutils.jobs.taskValues.set(key="sr_subproceso", value=params.sr_subproceso)

# Muestra el valor de la variable 'stage' si el modo debug está activado
if conf.debug:
    Mensaje = "El archivo a generar es un " + stage + " para el subproceso " + params.sr_subproceso
    display(Mensaje)
