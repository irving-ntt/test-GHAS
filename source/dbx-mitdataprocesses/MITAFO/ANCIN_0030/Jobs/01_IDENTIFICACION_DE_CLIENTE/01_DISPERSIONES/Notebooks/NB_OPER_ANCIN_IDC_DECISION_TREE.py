# Databricks notebook source
# DBTITLE 1,Framework load
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parametros
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_origen_arc": str,
    "sr_dt_org_arc": str,
    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
})
params.validate()

# COMMAND ----------

# DBTITLE 1,Jobs parametros
dbutils.jobs.taskValues.set(key='sr_proceso', value=params.sr_proceso)
dbutils.jobs.taskValues.set(key='sr_subproceso', value=params.sr_subproceso)
dbutils.jobs.taskValues.set(key='sr_subetapa', value=params.sr_subetapa)
dbutils.jobs.taskValues.set(key='sr_origen_arc', value=params.sr_origen_arc)
dbutils.jobs.taskValues.set(key='sr_dt_org_arc', value=params.sr_dt_org_arc)
dbutils.jobs.taskValues.set(key='sr_folio', value=params.sr_folio)
dbutils.jobs.taskValues.set(key='sr_id_archivo', value=params.sr_id_archivo)
dbutils.jobs.taskValues.set(key='sr_tipo_layout', value=params.sr_tipo_layout)
dbutils.jobs.taskValues.set(key='sr_instancia_proceso', value=params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key='sr_usuario', value=params.sr_usuario)
dbutils.jobs.taskValues.set(key='sr_etapa', value=params.sr_etapa)
dbutils.jobs.taskValues.set(key='sr_id_snapshot', value=params.sr_id_snapshot)
dbutils.jobs.taskValues.set(key='sr_paso', value=params.sr_paso)

# COMMAND ----------

# DBTITLE 1,Set var_tramite
# Diccionario de trámites mapeados individualmente a var_tramite
tramites_97 = {
    # INFONAVIT
    '354': "SOL_MARCA",        # Solicitud de Marca de Cuentas por 43 BIS
    '364': "TRANSFERENCIAS",   # Transferencias Acreditados Infonavit
    '365': "TRANSFERENCIAS",   # Transferencia por Anualidad Garantizada
    '368': "TRANSFERENCIAS",   # Uso de Garantía por 43 BIS
    '348': "DEV_SALDOS",       # Devolución de Excedentes por 43 BIS
    '349': "DEV_SALDOS",       # Devolución de Saldos Excedentes por T.A.A.G.
    '347': "SOL_DESMARCA",     #Desmarca Crédito de Vivienda por 43 BIS
    #3832': "DPSJL",           # Devolución de Pago sin Justificación Legal

    # FOVISSSTE
    '3286': "TRP",             # Transferencia de Recursos por Portabilidad
    '363':  "TRANSF_FOV",      # Transferencia de Acreditados Fovissste    
    '350':  "DSEF",            # Devolución de Excedentes por T.A. Fovissste
    '3283': "SDDF",            # Desmarca Fovissste
}

# Inicializamos la variable
var_tramite = None

# Lógica para proceso 7 - Dispersiones Ordinarias
if params.sr_proceso == '7':
    if params.sr_subproceso in ['8', '439', '121']:
        var_tramite = "IMSS"
    elif params.sr_subproceso == '118':
        var_tramite = "AEIM"
    elif params.sr_subproceso in ['120', '210', '122']:
        var_tramite = "ISSSTE"
    elif params.sr_subproceso == '3832':
        var_tramite = "DPSJL"
    else:
        logger.error("Combinación de sr_proceso y sr_subproceso no válida para determinar var_tramite")
        raise ValueError("Combinación de sr_proceso y sr_subproceso no válida")

# Lógica para proceso 97 - Traspasos
elif params.sr_proceso == '97':
    tramite = str(params.sr_subproceso)
    if tramite in tramites_97:
        var_tramite = tramites_97[tramite]
    else:
        logger.error(f"subproceso '{tramite}' no válido para proceso 97")
        raise ValueError("subproceso no válido para Traspasos")

# Otros casos
else:
    logger.error("sr_proceso no válido para determinar var_tramite")
    raise ValueError("sr_proceso no válido para determinar var_tramite")

# Guardamos el valor si es válido
dbutils.jobs.taskValues.set(key='var_tramite', value=var_tramite)

# COMMAND ----------

# DBTITLE 1,Info var_tramite
logger.info(f"var_tramite: {var_tramite}")
