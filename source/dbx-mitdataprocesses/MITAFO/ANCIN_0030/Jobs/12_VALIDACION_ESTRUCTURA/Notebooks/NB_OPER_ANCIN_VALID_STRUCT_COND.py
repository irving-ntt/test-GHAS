# Databricks notebook source
'''
Descripcion:
    Validacion de estructura condicional
Subetapa:
    Validacion de estructura
Tramite:
    Todos los tramites
Tablas Input:
    NA
Tablas Output:
    NA
Tablas DELTA:
    NA

Archivos SQL:
    NA
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Crear la instancia con los parámetros esperados
params = WidgetParams({    
    "sr_proceso": str,    
    "sr_subproceso": str,      
    "sr_subetapa": str,
    "sr_id_archivo": str,
    "sr_path_arch" : str,
    "sr_folio" : str,
    "sr_instancia_proceso" : str,
    "sr_usuario" : str,
    "sr_etapa" : str,
    "sr_id_snapshot" : str,
    "sr_paso" : str,
    "sr_id_archivo_siguiente" : str,
    "sr_fec_arc" : str,
    "sr_origen_arc": str
})



# COMMAND ----------

stage = 0

if params.sr_subetapa == "23":
    if params.sr_subproceso == "8":
        stage="DISPERSION ORDINARIA IMSS"
    elif params.sr_subproceso == "439":
        stage="DISPERSION GUBERNAMENTAL IMSS"
    elif params.sr_subproceso == "121":
        stage="DISPERSION INTERESES EN TRANSITO"
    elif params.sr_subproceso == "210":
        stage="DISPERSION GUBERNAMENTAL ISSSTE"
    ##TRASPASOS
    elif params.sr_subproceso == "354":
        stage="SOLICITUD DE MARCA DE CUENTAS POR 43 BIS"
    elif params.sr_subproceso == "364":
        stage="TRANSFERENCIAS DE ACREDITADOS INFONAVIT"
    elif params.sr_subproceso == "365":
        stage="TRANSFERENCIA POR ANUALIDAD GARANTIZADA"
    elif params.sr_subproceso == "368":
        stage="USO DE GARANTIA POR 43 BIS"
    elif params.sr_subproceso == "347":
        stage="DESMARCA DE CREDITO DE VIVIENDA POR 43 BIS"
    elif params.sr_subproceso == "363":
        stage="TRANSFERENCIA DE ACREDITADOS FOVISSSTE"
    elif params.sr_subproceso == "3286":
        stage="TRANSFERENCIA DE RECURSOS POR PORTABILIDAD"
    elif params.sr_subproceso == "348":
        stage="DEVOLUCION DE EXCEDENTES POR 43 BIS"
    elif params.sr_subproceso == "350":
        stage="DEVOLUCION DE EXCEDENTES POR T. A. FOVISSSTE"
    elif params.sr_subproceso == "349":
        stage="DEVOLUCION DE SALDOS EXCEDENTES POR T. A. A. G."
    elif params.sr_subproceso == "3283":
        stage="DESMARCA FOVISSSTE"
    elif params.sr_subproceso == "3832":
        stage="DEVOLUCION DE PAGO SIN JUSTIFICACION LEGAL IMSS"
    elif params.sr_subproceso == "122":
        stage="DISPERSION INTERESES EN TRANSITO ISSSTE"
    elif params.sr_subproceso == "120":
        stage="DISPERSION ORDINARIA ISSSTE"
    elif params.sr_subproceso == "118":
        stage="DISPERSION DE ACLARACIONES ESPECIALES IMSS"
    ##TRASPASOS
elif params.sr_subetapa in ["25","4030"]:
    if params.sr_subproceso == "354":
        stage="SOLICITUD DE MARCA DE CUENTAS POR 43 BIS"
    elif params.sr_subproceso == "364":
        stage="TRANSFERENCIAS DE ACREDITADOS INFONAVIT"
    elif params.sr_subproceso == "365":
        stage="TRANSFERENCIA POR ANUALIDAD GARANTIZADA"
    elif params.sr_subproceso == "368":
        stage="USO DE GARANTIA POR 43 BIS"
    elif params.sr_subproceso == "347":
        stage="DESMARCA DE CREDITO DE VIVIENDA POR 43 BIS"
    elif params.sr_subproceso == "363":
        stage="TRANSFERENCIA DE ACREDITADOS FOVISSSTE"
    elif params.sr_subproceso == "3286":
        stage="TRANSFERENCIA DE RECURSOS POR PORTABILIDAD"
    elif params.sr_subproceso == "348":
        stage="DEVOLUCION DE EXCEDENTES POR 43 BIS"
    elif params.sr_subproceso == "350":
        stage="DEVOLUCION DE EXCEDENTES POR T. A. FOVISSSTE"
    elif params.sr_subproceso == "349":
        stage="DEVOLUCION DE SALDOS EXCEDENTES POR T. A. A. G."
    elif params.sr_subproceso == "3283":
        stage="DESMARCA FOVISSSTE"
    elif params.sr_subproceso == "3832":
        stage="DEVOLUCION DE PAGO SIN JUSTIFICACION LEGAL IMSS"
elif params.sr_subetapa == "832":
    if params.sr_subproceso == "122":
        stage="DISPERSION INTERESES EN TRANSITO ISSSTE 832"
    elif params.sr_subproceso == "120":
        stage="DISPERSION ORDINARIA ISSSTE 832"
    elif params.sr_subproceso == "118":
        stage="DISPERSION DE ACLARACIONES ESPECIALES IMSS 832"
elif params.sr_subetapa == "3356":
    if params.sr_subproceso == "354":
        stage="SOLICITUD DE MARCA DE CUENTAS POR 43 BIS"
    elif params.sr_subproceso == "364":
        stage="TRANSFERENCIAS DE ACREDITADOS INFONAVIT"
    elif params.sr_subproceso == "365":
        stage="TRANSFERENCIA POR ANUALIDAD GARANTIZADA"
    elif params.sr_subproceso == "368":
        stage="USO DE GARANTIA POR 43 BIS"
    elif params.sr_subproceso == "363":
        stage="TRANSFERENCIA DE ACREDITADOS FOVISSSTE"
    
#elif params.sr_subetapa != "832":
    

##ARCHIVO DE RESPUESTA
#elif params.sr_subproceso == "118":
#    stage="ARCHIVO DE RESPUESTA"
#    sub_stage="ARCHIVO DE RESPUESTA ACLARACIONES ESPECIALES"
#    dbutils.jobs.taskValues.set(key = "sr_sub_stage", value=sub_stage)
#elif params.sr_subproceso == "120":
#    stage="ARCHIVO DE RESPUESTA"
#    sub_stage="ARCHIVO DE RESPUESTA ACLARACIONES ESPECIALES"
#    dbutils.jobs.taskValues.set(key = "sr_sub_stage", value=sub_stage)
#elif params.sr_subproceso == "122":
#    stage="ARCHIVO DE RESPUESTA"
#    sub_stage="ARCHIVO DE RESPUESTA ACLARACIONES ESPECIALES"
#    dbutils.jobs.taskValues.set(key = "sr_sub_stage", value=sub_stage)

dbutils.jobs.taskValues.set(key = "sr_stage", value=stage)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_id_archivo", value = params.sr_id_archivo)
dbutils.jobs.taskValues.set(key = "sr_path_arch", value = params.sr_path_arch)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
if params.sr_folio != None:
    dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
    dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = params.sr_instancia_proceso)
    dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_usuario)
    dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
    dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = params.sr_id_snapshot)
    dbutils.jobs.taskValues.set(key = "sr_paso", value = params.sr_paso)
    dbutils.jobs.taskValues.set(key = "sr_id_archivo_siguiente", value = params.sr_id_archivo_siguiente)
    dbutils.jobs.taskValues.set(key = "sr_fec_arc", value = params.sr_fec_arc)
    dbutils.jobs.taskValues.set(key = "sr_origen_arc", value = params.sr_origen_arc)

