# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams(
    {
        "sr_folio": str,
        "sr_folio_rel": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_origen_arc": str,
        "sr_fecha_acc": (str, "YYYYMMDD"),
        "sr_fecha_liq": (str, "YYYYMMDD"),
        "sr_tipo_mov": str,
        "sr_reproceso": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_id_snapshot": str,
    }
)
params.validate()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creacion de parametros con valores fijos

# COMMAND ----------

dbutils.jobs.taskValues.set(key="sr_fcc_usu_cre", value="EJE_CIERRENDATA")
dbutils.jobs.taskValues.set(key="sr_flc_usu_reg", value="EJE_CIERRENDATA")
dbutils.jobs.taskValues.set(key="cx_cre_esquema", value="CIERREN_ETL")
dbutils.jobs.taskValues.set(key="tl_cre_dispersion", value="TTSISGRAL_ETL_DISPERSION")
dbutils.jobs.taskValues.set(key="cx_crn_esquema", value="CIERREN")
dbutils.jobs.taskValues.set(key="tl_crn_matriz_convivencia", value="TTAFOGRAL_MATRIZ_CONVIVENCIA")
dbutils.jobs.taskValues.set(key="cx_pro_esquema", value="PROCESOS")
dbutils.jobs.taskValues.set(key="tl_pro_aeim", value="TTAFOAE_AEIM")
dbutils.jobs.taskValues.set(key="tl_crn_tipo_subcta", value="TCCRXGRAL_TIPO_SUBCTA")
dbutils.jobs.taskValues.set(key="tl_crn_valor_accion", value="TCAFOGRAL_VALOR_ACCION")
dbutils.jobs.taskValues.set(key="tl_crn_config_concep_mov", value="TFAFOGRAL_CONFIG_CONCEP_MOV")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Cambio de valor del parametro `SR_TIPO_MOV`**
# MAGIC
# MAGIC Originalmente BUS envia el parametro `SR_TIPO_MOV` con el valor de 'ABONO' o 'CARGO'. Este se debe cambiar a su id respectivo.

# COMMAND ----------

# Normalizar el valor a mayúsculas desde el parámetro
sr_tipo_mov = params.sr_tipo_mov.upper()

# Evaluar y reasignar el valor según las reglas de negocio
if sr_tipo_mov == "ABONO" or sr_tipo_mov == "2":
    sr_tipo_mov = "2"
    logger.info(f"El valor de sr_tipo_mov ha sido cambiado a {sr_tipo_mov}")
else:
    sr_tipo_mov = "1"
    logger.info(f"El valor de sr_tipo_mov ha sido cambiado a {sr_tipo_mov}")

# Guardar el valor final en el contexto del job con key en minúsculas
dbutils.jobs.taskValues.set(key="sr_tipo_mov", value=sr_tipo_mov)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Cambio del valor del parametro `SR_ETAPA` (No lo envian si no que se crea a partir de sr_reproceso)**
# MAGIC
# MAGIC Este siempre toma el valor del parametro `SR_REPROCESO`

# COMMAND ----------

sr_etapa = params.sr_reproceso
dbutils.jobs.taskValues.set(key="sr_etapa", value=sr_etapa)
logger.info(f"Valor de sr_etapa: {sr_etapa}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculamos el valor de Factor

# COMMAND ----------

# Asegurar que sr_etapa sea un entero
sr_etapa_int = int(sr_etapa)

# Determinar el valor de sr_factor según la etapa
sr_factor = 1 if sr_etapa_int in (1, 3) else -1

# Loggear el resultado
logger.info(f"Valor calculado para sr_factor: {sr_factor} (a partir de sr_etapa={sr_etapa})")

# Guardar el valor como job parameter
dbutils.jobs.taskValues.set(key="sr_factor", value=sr_factor)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculamos `SR_FOLIO_REL`

# COMMAND ----------

sr_folio_rel = params.sr_folio_rel

# Normalizar el valor si es "NA" o "na"
if sr_folio_rel.lower() == "na":
    sr_folio_rel = "null"

# Registrar el valor como job param
dbutils.jobs.taskValues.set(key="sr_folio_rel", value=sr_folio_rel)

# Log opcional
logger.info(f"Valor final de sr_folio_rel: {sr_folio_rel}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculamos `SR_FECHA_ACC`

# COMMAND ----------

sr_fecha_acc = params.sr_fecha_acc

# Normalizar el valor si es "NA" o "na"
if sr_fecha_acc.lower() == "na":
    sr_fecha_acc = "null"

# Registrar el valor como job param
dbutils.jobs.taskValues.set(key="sr_fecha_acc", value=sr_fecha_acc)

# Log opcional
logger.info(f"Valor final de sr_fecha_acc: {sr_fecha_acc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validaciones del flujo

# COMMAND ----------

# Inicializar stage
stage = "0"

# Validar condiciones para determinar el stage
if params.sr_subetapa == "273":
    if params.sr_subproceso == "118" and sr_tipo_mov == "2":
        if sr_etapa == "1":
            stage = "AEIM_ABONO"
        elif sr_etapa == "2":
            stage = "ABONO RECHAZO"
        elif sr_etapa == "3":
            stage = "AEIM_ABONO_RECALCULO"
        else:
            logger.error("Parámetros no válidos para AEIM_ABONO")
            raise ValueError("Parámetros no válidos para AEIM_ABONO")
    elif params.sr_subproceso != "101":
        if sr_tipo_mov == "2":  # ABONO
            if sr_etapa == "1":
                stage = "ABONO"
            elif sr_etapa == "2":
                stage = "ABONO RECHAZO"
            elif sr_etapa == "3":
                stage = "ABONO RECALCULO"
        else:  # CARGO
            if sr_etapa == "1":
                stage = "CARGO"
            elif sr_etapa == "2":
                stage = "CARGO RECHAZO"
            elif sr_etapa == "3":
                stage = "CARGO RECALCULO"
    else:
        logger.error("Parámetros de entrada no válidos para generar movimientos")
        raise ValueError("Parámetros de entrada no válidos para generar movimientos")
else:
    logger.error("Parámetros de entrada no válidos para generar movimientos")
    raise ValueError("Parámetros de entrada no válidos para generar movimientos")

# Registrar el resultado como job param
dbutils.jobs.taskValues.set(key="sr_stage", value=stage)

# Log del valor resultante
logger.info(f"Valor de sr_stage: {stage}")

# COMMAND ----------

for key, value in params.to_dict().items():
    if key not in ("sr_folio_rel", "sr_tipo_mov", "sr_fecha_acc"):
        dbutils.jobs.taskValues.set(key=key, value=value)
