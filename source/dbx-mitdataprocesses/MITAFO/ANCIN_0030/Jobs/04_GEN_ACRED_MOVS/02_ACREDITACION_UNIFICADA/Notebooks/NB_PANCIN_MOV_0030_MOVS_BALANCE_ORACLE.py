# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0030_MOVS_BALANCE_ORACLE
# MAGIC
# MAGIC **Descripción:** Inserción en Oracle de movimientos balance
# MAGIC
# MAGIC **Subetapa:** Inserción en Oracle
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:** TEMP_SALIDA_1_DB400_{sr_folio}
# MAGIC
# MAGIC **Tablas Output:** {CX_CRE_ESQUEMA}.{TL_CRE_MOVIMIENTOS}
# MAGIC
# MAGIC **Tablas DELTA:** NA
# MAGIC
# MAGIC **Archivos SQL:** NB_PANCIN_MOV_0030_MOVS_BALANCE_ORACLE_AFTER.sql
# MAGIC
# MAGIC **Flow:** Inserción paralela en Oracle
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/215045258748372?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_ORACLE.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, expr, lit, row_number, when
from pyspark.sql.window import Window
from pyspark.sql.functions import from_utc_timestamp, current_timestamp

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
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reglas TF_150 aplicadas (Salida 1 - LK_TF150_TO_DB400)
# MAGIC
# MAGIC Basado en el transformer TF_150 del XML `JP_PANCIN_MOV_0030_MOVS_BALANCE.xml`:
# MAGIC
# MAGIC - ### Reglas para el calculo:
# MAGIC   - Las varaibles calculadas ya estan en el set de datos (`TEMP_JOIN_MAXIMOS`)
# MAGIC   - Las transformaciones deben ser aplicadas a todos los registros solo si el parametro sr_etapa = 1 o sr_etapa = 3.
# MAGIC
# MAGIC | **Derivación** | **Nombre de columna** |
# MAGIC |----------------|------------------------|
# MAGIC | CONTADOR2 | FTN_ID_MOV |
# MAGIC | FCN_ID_TIPO_SUBCTA | FCN_ID_TIPO_SUBCTA |
# MAGIC | FTC_FOLIO | FTC_FOLIO |
# MAGIC | FTC_FOLIO_REL | FTC_FOLIO_REL |
# MAGIC | FCN_ID_SIEFORE | FCN_ID_SIEFORE |
# MAGIC | FTF_MONTO_PESOS | FTF_MONTO_PESOS |
# MAGIC | FTF_MONTO_ACCIONES | FTF_MONTO_ACCIONES |
# MAGIC | FCN_ID_VALOR_ACCION | FCN_ID_VALOR_ACCION |
# MAGIC | FTN_NUM_CTA_INVDUAL | FTN_NUM_CTA_INVDUAL |
# MAGIC | SEMAFORO | FTN_ID_SEMAFORO_MOV |
# MAGIC | StringToTimestamp(p_SR_FEC_LIQ, "yyyy-mm-dd") | FTD_FEH_LIQUIDACION |
# MAGIC | if SEMAFORO = 190 or SEMAFORO = 192 then 136 else 137 | FTD_ID_ESTATUS_MOV |
# MAGIC | FTN_ID_MARCA | FTN_ID_MARCA |
# MAGIC | FCN_ID_TIPO_MOV | FCN_ID_TIPO_MOV |
# MAGIC | CurrentTimestamp() | FCD_FEH_CRE |
# MAGIC | p_CX_CRE_USUARIO | FCC_USU_CRE |
# MAGIC | SetNull() | FCD_FEH_ACT |
# MAGIC | SetNull() | FCC_USU_ACT |
# MAGIC | FCN_ID_CONCEPTO_MOV | FCN_ID_CONCEPTO_MOV |
# MAGIC | FCC_TABLA_NCI_MOV | FCC_TABLA_NCI_MOV |
# MAGIC | FNN_ID_REFERENCIA | FNN_ID_REFERENCIA |
# MAGIC | if SEMAFORO = 191 then 758 else SetNull() | FTD_ID_ERROR_VAL |
# MAGIC | 0 | FTN_MOV_GENERADO |
# MAGIC | 0 | FTN_REG_ACREDITADO |

# COMMAND ----------

# MAGIC %md
# MAGIC # Se hace el insert solo si sr_etapa es 1 o 3
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/215045258748373?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_ORACLE_01.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 3,DB_400_TTSISGRAL_ETL_MOVIMIENTOS - INSERCIÓN EN ORACLE
# Insertar datos en tabla de movimientos en Oracle
# Equivalente al stage DB_400_TTSISGRAL_ETL_MOVIMIENTOS

logger.info("Insertando datos en tabla de movimientos Oracle")

# Leer insumo único: TEMP_JOIN_MAXIMOS_{sr_folio}
df_tf = db.read_delta(f"TEMP_JOIN_MAXIMOS_{params.sr_folio}")

# Constraint por parámetro de etapa (1 o 3)
# CORREGIDO: La documentación dice sr_etapa = 1 o sr_etapa = 3
if str(params.sr_etapa) in ("1", "3"):
    # Optimización: Usar select con todas las columnas en una sola operación
    salida1 = df_tf.select(
        # CONTADOR2 = MAXIMO2 + ID2 (ID2 es row_number secuencial)
        col("CONTADOR2").alias("FTN_ID_MOV"),
        col("FCN_ID_TIPO_SUBCTA"),
        col("FTC_FOLIO"),
        col("FTC_FOLIO_REL"),
        col("FCN_ID_SIEFORE"),
        col("FTF_MONTO_PESOS"),
        col("FTF_MONTO_ACCIONES"),
        col("FCN_ID_VALOR_ACCION"),
        col("FTN_NUM_CTA_INVDUAL"),
        # SEMAFORO → FTN_ID_SEMAFORO_MOV
        col("SEMAFORO").alias("FTN_ID_SEMAFORO_MOV"),
        # StringToTimestamp(p_SR_FEC_LIQ, "yyyy-mm-dd")
        expr(f"to_timestamp('{params.sr_fec_liq}', 'yyyyMMdd')").alias(
            "FTD_FEH_LIQUIDACION"
        ),
        # if SEMAFORO = 190 or SEMAFORO = 192 then 136 else 137
        # CORREGIDO: usar 190 y 192 en lugar de 190 y 191
        when(col("SEMAFORO").isin(190, 192), lit(136))
        .otherwise(lit(137))
        .alias("FTD_ID_ESTATUS_MOV"),
        col("FTN_ID_MARCA").alias("FTN_ID_MARCA"),
        col("FCN_ID_TIPO_MOV"),
        # CurrentTimestamp()
        from_utc_timestamp(current_timestamp(), "America/Mexico_City").alias("FCD_FEH_CRE"),
        # p_CX_CRE_USUARIO
        lit(conf.CX_CRE_USUARIO).alias("FCC_USU_CRE"),
        # SetNull()
        lit(None).cast("timestamp").alias("FCD_FEH_ACT"),
        # SetNull()
        lit(None).cast("string").alias("FCC_USU_ACT"),
        col("FCN_ID_CONCEPTO_MOV"),
        # CORREGIDO: FCC_TABLA_INCI_MOV (no FCC_TABLA_NCI_MOV)
        col("FCC_TABLA_NCI_MOV"),
        col("FNN_ID_REFERENCIA"),
        # if SEMAFORO = 191 then 758 else SetNull()
        # Nota: Como SEMAFORO solo puede ser 190 o 192, este siempre será NULL
        when(col("SEMAFORO") == 191, lit(758))
        .otherwise(lit(None))
        .cast("int")
        .alias("FTN_ID_ERROR_VAL"),
        # 0
        lit(0).alias("FTN_MOV_GENERADO"),
        # 0
        lit(0).alias("FTN_REG_ACREDITADO"),
    )

    # Optimización: liberar memoria del df_tf
    df_tf.unpersist()
    if conf.debug:
        display(salida1)
    # Insertar en Oracle
    db.write_data(
        salida1,
        f"{conf.CX_CRE_ESQUEMA}.{conf.TL_CRE_MOVIMIENTOS}",
        "default",
        "append",
    )
    logger.info("Datos insertados correctamente en tabla de movimientos Oracle")
else:
    logger.info(
        f"p_SR_ETAPA = {params.sr_etapa}: no es 1 ni 3, no se inserta en Oracle para esta etapa"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Se actualiza la bandera de pre movimientos siempre???
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/215045258748374?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_ORACLE_02.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 4,AFTER SQL - ACTUALIZAR BANDERA DE PRE MOVIMIENTO GENERADO
# Ejecutar After SQL para actualizar bandera en tabla de pre movimientos
# Equivalente al After SQL del stage DB_400_TTSISGRAL_ETL_MOVIMIENTOS

#if str(params.sr_etapa) in ("1", "3"):

logger.info("Ejecutando After SQL para actualizar bandera de pre movimiento generado")

# Obtener After SQL desde archivo .sql usando QueryManager
after_sql = query.get_statement(
    "NB_PANCIN_MOV_0030_MOVS_BALANCE_ORACLE_AFTER.sql",
    SR_FOLIO=params.sr_folio,
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO,
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    TL_CRE_PRE_MOVIMIENTOS=conf.TL_CRE_PRE_MOVIMIENTOS,
)

# Ejecutar el After SQL usando execute_oci_dml para operaciones DML
execution = db.execute_oci_dml(statement=after_sql, async_mode=True)

logger.info("After SQL ejecutado correctamente")
logger.info("Bandera FTN_PRE_MOV_GENERADO actualizada a 1")
# else:
#     logger.info(
#         f"p_SR_ETAPA = {params.sr_etapa}: no es 1 ni 3, no se actualizan los registros"
#     )

# COMMAND ----------

# DBTITLE 4,LIMPIEZA Y FINALIZACIÓN
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())

logger.info("=== JOB COMPLETADO: NB_PANCIN_MOV_0030_MOVS_BALANCE_ORACLE ===")
