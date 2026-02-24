# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0030_MOVS_BALANCE_DATASET
# MAGIC
# MAGIC **Descripción:** Generar dataset final (sufijo 04) como Delta para consumo posterior
# MAGIC
# MAGIC **Subetapa:** Generar dataset (Delta)
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:** TEMP_SALIDA_0_DS300_{sr_folio}
# MAGIC
# MAGIC **Tablas Output:** RESULTADO_MOVS_BALANCE_{sr_folio} (Delta)
# MAGIC
# MAGIC **Tablas DELTA:** RESULTADO_MOVS_BALANCE_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:** NA
# MAGIC
# MAGIC **Flow:** Calcular salida 0 (TF_150) y materializar Delta para el notebook 0035
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/215045258748370?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_DATASET.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, expr, lit, when, from_utc_timestamp
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,DEFINICIÓN DE PARÁMETROS
# Definir parámetros del notebook
params = WidgetParams(
    {
        "sr_paso": str,
        "sr_etapa": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_id_snapshot": str,
        "sr_folio": str,
        "sr_fec_liq": (str, "YYYYMMDD"),
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_fec_acc": (str, "YYYYMMDD"),
        "sr_tipo_mov": str,
        "sr_subetapa": str,
        #"sr_id_archivo": str,
        "sr_reproceso": str,
        "sr_etapa_bit": str,
    }
)

# Validar parámetros
params.validate()

# Cargar configuración
conf = ConfManager()
db = DBXConnectionManager()

logger.info("=== INICIANDO: NB_PANCIN_MOV_0030_MOVS_BALANCE_DATASET ===")
logger.info(f"Parámetros: {params}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reglas TF_150 aplicadas (Salida 0 - LK_TF150_TO_DS300)
# MAGIC
# MAGIC Basado en el transformer TF_150 del XML `JP_PANCIN_MOV_0030_MOVS_BALANCE.xml`:
# MAGIC
# MAGIC - ### Reglas para el calculo:
# MAGIC   - Las varaibles calculadas ya estan en el set de datos (`TEMP_JOIN_MAXIMOS`)
# MAGIC   - Las transformaciones deben ser aplicadas solo a aquellos registros donde la columna calculada SEMAFORO = 190 O SEMAFORO = 192.
# MAGIC
# MAGIC - ### Con las variables de arriba ahora calcular, "TRANSFORMACIONES":
# MAGIC | **Derivación** | **Nombre de columna** |
# MAGIC |----------------|------------------------|
# MAGIC | CONTADOR | FTN_ID_BAL_MOV |
# MAGIC | FTC_FOLIO_REL | FTC_FOLIO_REL |
# MAGIC | if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_PESOS * (-1) else if (FCN_ID_TIPO_MOV = 181) then 0 else 0 | FTN_DISP_PESOS |
# MAGIC | if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_ACCIONES * (-1) else if (FCN_ID_TIPO_MOV = 181) then 0 else 0 | FTN_DISP_ACCIONES |
# MAGIC | if (FCN_ID_TIPO_MOV = 180) then 0 else if (FCN_ID_TIPO_MOV = 181) then FTF_MONTO_PESOS otherwise 0 | FTN_PDTE_PESOS |
# MAGIC | if (FCN_ID_TIPO_MOV = 180) then 0 else if (FCN_ID_TIPO_MOV = 181) then FTF_MONTO_ACCIONES otherwise 0 | FTN_PDTE_ACCIONES |
# MAGIC | if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_PESOS else if (FCN_ID_TIPO_MOV = 181) then 0 else 0 | FTN_COMP_PESOS |
# MAGIC | if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_ACCIONES else if (FCN_ID_TIPO_MOV = 181) then 0 else 0 | FTN_COMP_ACCIONES |
# MAGIC | if (FCN_ID_TIPO_MOV = 180) then 0 else if (FCN_ID_TIPO_MOV = 181) then 0 | FTN_DIA_PESOS |
# MAGIC | if (FCN_ID_TIPO_MOV = 180) then 0 else if (FCN_ID_TIPO_MOV = 181) then 0 | FTN_DIA_ACCIONES |
# MAGIC | FTN_NUM_CTA_INVDUAL | FTN_NUM_CTA_INVDUAL |
# MAGIC | FTN_ORIGEN_APORTACION | FTN_ORIGEN_APORTACION |
# MAGIC | FCN_ID_TIPO_SUBCTA | FCN_ID_TIPO_SUBCTA |
# MAGIC | FCN_ID_SIEFORE | FCN_ID_SIEFORE |
# MAGIC | FCN_ID_VALOR_ACCION | FCN_ID_VALOR_ACCION |
# MAGIC | Fecha: substring(SR_FEC_LIQ, "yyyy/mm/dd") | FTD_FEH_LIQUIDACION |
# MAGIC | FCN_ID_CONCEPTO_MOV | FCN_ID_CONCEPTO_MOV |
# MAGIC | FCC_TABLA_NCI_MOV | FCC_TABLA_NCI_MOV |
# MAGIC | CurrentTimestamp | FCD_FEH_CRE |
# MAGIC | px_CRE_USUARIO | FCC_USU_ACT |
# MAGIC | FTN_DEDUCIBLE | FTN_DEDUCIBLE |
# MAGIC | FCN_ID_PLAZO | FCN_ID_PLAZO |

# COMMAND ----------

# MAGIC %md
# MAGIC > Nota: Este notebook genera la Delta `RESULTADO_MOVS_BALANCE_{sr_folio}`. La inserción en Oracle se realiza en el notebook `NB_PANCIN_MOV_0035_BALANCE.py`.

# COMMAND ----------

# DBTITLE 2,DS_300 - GENERAR DELTA RESULTADO_MOVS_BALANCE
# Guardar resultado final en dataset (sufijo 04) como Delta
# Equivalente al stage DS_300

logger.info(f"Generando Delta RESULTADO_MOVS_BALANCE_{params.sr_folio}")

# Leer insumo único: TEMP_JOIN_MAXIMOS_{sr_folio}
df_tf = db.read_delta(f"TEMP_JOIN_MAXIMOS_{params.sr_folio}")

# Salida 0: SEMAFORO in (190,192) y columnas derivadas
# Aplicar transformaciones solo a registros con SEMAFORO = 190 o 192
# Optimización: Usar select con todas las columnas en una sola operación para mejor rendimiento
salida0 = df_tf.filter(col("SEMAFORO").isin(190, 192)).select(
    # FTN_ID_BAL_MOV: CONTADOR
    col("CONTADOR").alias("FTN_ID_BAL_MOV"),
    # FTC_FOLIO
    col("FTC_FOLIO"),
    # FTC_FOLIO_REL
    col("FTC_FOLIO_REL"),
    # FTN_DISP_PESOS: if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_PESOS * (-1) else 0
    when(col("FCN_ID_TIPO_MOV") == 180, col("FTF_MONTO_PESOS") * lit(-1))
    .otherwise(lit(0))
    .alias("FTN_DISP_PESOS"),
    # FTN_DISP_ACCIONES: if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_ACCIONES * (-1) else 0
    when(col("FCN_ID_TIPO_MOV") == 180, col("FTF_MONTO_ACCIONES") * lit(-1))
    .otherwise(lit(0))
    .alias("FTN_DISP_ACCIONES"),
    # FTN_PDTE_PESOS: if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_PESOS else 0
    when(col("FCN_ID_TIPO_MOV") == 180, lit(0))
    .when(col("FCN_ID_TIPO_MOV") == 181, col("FTF_MONTO_PESOS"))
    .otherwise(lit(0))
    .alias("FTN_PDTE_PESOS"),
    # FTN_PDTE_ACCIONES: if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_ACCIONES else 0
    when(col("FCN_ID_TIPO_MOV") == 180, lit(0))
    .when(col("FCN_ID_TIPO_MOV") == 181, col("FTF_MONTO_ACCIONES"))
    .otherwise(lit(0))
    .alias("FTN_PDTE_ACCIONES"),
    # FTN_COMP_PESOS: if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_PESOS else 0
    when(col("FCN_ID_TIPO_MOV") == 180, col("FTF_MONTO_PESOS"))
    .otherwise(lit(0))
    .alias("FTN_COMP_PESOS"),
    # FTN_COMP_ACCIONES: if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_ACCIONES else 0
    when(col("FCN_ID_TIPO_MOV") == 180, col("FTF_MONTO_ACCIONES"))
    .otherwise(lit(0))
    .alias("FTN_COMP_ACCIONES"),
    # FTN_DIA_PESOS: if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_PESOS else 0
    when(col("FCN_ID_TIPO_MOV") == 180, lit(0))
    .when(col("FCN_ID_TIPO_MOV") == 181, lit(0))
    .otherwise(lit(0))
    .alias("FTN_DIA_PESOS"),
    # FTN_DIA_ACCIONES: if (FCN_ID_TIPO_MOV = 180) then FTF_MONTO_ACCIONES else 0
    when(col("FCN_ID_TIPO_MOV") == 180, lit(0))
    .when(col("FCN_ID_TIPO_MOV") == 181, lit(0))
    .otherwise(lit(0))
    .alias("FTN_DIA_ACCIONES"),
    # Campos adicionales según tabla de transformaciones
    col("FTN_NUM_CTA_INVDUAL").alias("FTC_NUM_CTA_INVDUAL"),
    col("FTN_ORIGEN_APORTACION"),
    col("FCN_ID_TIPO_SUBCTA"),
    col("FCN_ID_SIEFORE"),
    col("FCN_ID_VALOR_ACCION"),
    col("FCN_ID_TIPO_MOV"),
    # FTD_FEH_LIQUIDACION: substring(SR_FEC_LIQ, "yyyy/mm/dd")
    expr(f"to_timestamp('{params.sr_fec_liq}', 'yyyyMMdd')").alias(
        "FTD_FEH_LIQUIDACION"
    ),
    col("FCN_ID_CONCEPTO_MOV"),
    col("FCC_TABLA_NCI_MOV"),
    # FCD_FEH_CRE: CurrentTimestamp
    from_utc_timestamp(current_timestamp(), "America/Mexico_City").alias("FCD_FEH_CRE"),
    # FCC_USU_CRE: px_CRE_USUARIO (parámetro sr_usuario)
    lit(conf.CX_CRE_USUARIO).alias("FCC_USU_CRE"),
    col("FTN_DEDUCIBLE"),
    col("FCN_ID_PLAZO"),
)

# Optimización: liberar memoria del df_tf ya que no se necesita más
df_tf.unpersist()

# REEMPLAZO: Según ds_catalog.txt, p_CLV_SUFIJO_04 = RESULTADO_MOVS_BALANCE
# Escribir/actualizar Delta de salida (evitar sobreescribir de otra corrida)
if conf.debug:
    display(salida0)
db.write_delta(
    f"RESULTADO_MOVS_BALANCE_{params.sr_folio}",
    salida0,
    "overwrite",
)

logger.info(
    f"Delta RESULTADO_MOVS_BALANCE_{params.sr_folio} generada/actualizada correctamente"
)

# COMMAND ----------

# DBTITLE 3,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())

logger.info("=== JOB COMPLETADO: NB_PANCIN_MOV_0030_MOVS_BALANCE_DATASET ===")
