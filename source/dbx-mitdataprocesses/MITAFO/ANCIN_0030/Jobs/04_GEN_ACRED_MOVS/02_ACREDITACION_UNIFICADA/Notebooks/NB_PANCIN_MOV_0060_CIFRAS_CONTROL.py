# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0060_CIFRAS_CONTROL
# MAGIC
# MAGIC **Descripción:** Inserta cifras de control por subetapa en `{CX_CRN_ESQUEMA}.{TL_CRN_VAL_CIFRAS_CONTROL}` a partir de conteos de semáforos en `{CX_CRE_ESQUEMA}.{TL_CRE_MOVIMIENTOS}`.
# MAGIC
# MAGIC **Subetapa:** Cifras de control
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:** OCI `{CX_CRE_ESQUEMA}.{TL_CRE_MOVIMIENTOS}`
# MAGIC
# MAGIC **Tablas Output:** `{CX_CRN_ESQUEMA}.{TL_CRN_VAL_CIFRAS_CONTROL}`
# MAGIC
# MAGIC **Tablas DELTA:** TEMP_CIFRAS_CONTROL_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0060_CIFRAS_CONTROL_OCI_001.sql
# MAGIC
# MAGIC **Flow:** Extraer conteos (OCI) → Delta temporal → Transformar TF_0120 → Insertar en Oracle con `write_data`
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1359597267196341?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0060_CIFRAS_CONTROL_image.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,DEFINICIÓN DE PARÁMETROS
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

params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1359597267196342?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0060_CIFRAS_CONTROL_image (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 2,OCI → DELTA: OBTENER CONTEOS DE SEMÁFOROS
stmt = query.get_statement(
    "NB_PANCIN_MOV_0060_CIFRAS_CONTROL_OCI_001.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    TL_CRE_MOVIMIENTOS=conf.TL_CRE_MOVIMIENTOS,
    SR_FOLIO=params.sr_folio,
)

db.write_delta(
    f"TEMP_CIFRAS_CONTROL_{params.sr_folio}",
    db.read_data("default", stmt),
    "overwrite",
)

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1359597267196343?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0060_CIFRAS_CONTROL_image (2).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 3,TF_0120: DERIVACIONES Y AUDITORÍA
src = db.read_delta(f"TEMP_CIFRAS_CONTROL_{params.sr_folio}")

salida = src.select(
    lit(params.sr_folio).alias("FTC_FOLIO"),
    lit(273).alias("FTC_ID_SUBETAPA"),
    (
        coalesce(col("COUNT_VERDE"), lit(0)).cast("decimal(8,0)")
        + coalesce(col("COUNT_ROJO"), lit(0)).cast("decimal(8,0)")
        + coalesce(col("COUNT_NARANJA"), lit(0)).cast("decimal(8,0)")
    ).alias("FLN_TOTAL_REGISTROS"),
    (
        coalesce(col("COUNT_VERDE"), lit(0)).cast("decimal(8,0)")
        + coalesce(col("COUNT_NARANJA"), lit(0)).cast("decimal(8,0)")
    ).alias("FLN_REG_CUMPLIERON"),
    coalesce(col("COUNT_ROJO"), lit(0))
    .cast("decimal(8,0)")
    .alias("FLN_REG_NO_CUMPLIERON"),
    lit("").cast("string").alias("FLC_VALIDACION"),
    lit(0).cast("decimal(8,0)").alias("FLN_TOTAL_ERRORES"),
    lit("").cast("string").alias("FLC_DETALLE"),
    current_timestamp().alias("FLD_FEC_REG"),
    lit(conf.CX_CRE_USUARIO).alias("FLC_USU_REG"),
    lit("").cast("string").alias("FTC_FOLIO_REL"),
    lit(None).cast("decimal(10,0)").alias("FTN_ID_ARCHIVO"),
)
if conf.debug:
    display(salida)

# COMMAND ----------

# DBTITLE 1,Borrar la tabla de CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL
del_cifras_control = query.get_statement(
    "NB_PANCIN_MOV_0060_CIFRAS_CONTROL_OCI_002.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRN_VAL_CIFRAS_CONTROL=conf.TL_CRN_VAL_CIFRAS_CONTROL,
    SR_FOLIO=params.sr_folio,
    SR_SUBETAPA=params.sr_subetapa,
)

# COMMAND ----------

execution = db.execute_oci_dml(statement=del_cifras_control, async_mode=False)

# COMMAND ----------

# DBTITLE 4,INSERCIÓN EN ORACLE
db.write_data(
    salida,
    f"{conf.CX_CRN_ESQUEMA}.{conf.TL_CRN_VAL_CIFRAS_CONTROL}",
    "default",
    "append",
)

# COMMAND ----------

# DBTITLE 5,LIMPIEZA
CleanUpManager.cleanup_notebook(locals())
