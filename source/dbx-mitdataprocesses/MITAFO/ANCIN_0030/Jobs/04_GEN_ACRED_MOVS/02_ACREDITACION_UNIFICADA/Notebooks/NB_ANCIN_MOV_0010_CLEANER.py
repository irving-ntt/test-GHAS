# Databricks notebook source
# DBTITLE 1,Cargar framework
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parametros originales
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
        # "sr_id_archivo": str,
        "sr_reproceso": str,
        "sr_etapa_bit": str,
    }
)
params.validate()
conf = ConfManager()

# COMMAND ----------

# DBTITLE 1,Objeto para manejo del datalake
db = DBXConnectionManager()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Control de actualización e inserción en `CONTROL_ARCHIVOS_PANCIN`
# MAGIC
# MAGIC Esta celda gestiona el control de los archivos PANCIN dentro del proceso,
# MAGIC reemplazando la creación de archivos físicos `3281_{SR_FOLIO}.txt` por un manejo
# MAGIC estructurado en una tabla Delta (`CONTROL_ARCHIVOS_PANCIN`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ Lógica del proceso
# MAGIC
# MAGIC 1. **Condición principal:**
# MAGIC    - El bloque se ejecuta solo cuando `SR_SUBETAPA = 273`.
# MAGIC
# MAGIC 2. **Caso `SR_REPROCESO = 2`:**
# MAGIC    - Se actualiza el registro existente en la tabla, cambiando:
# MAGIC      - `ESTADO` → `'False'`
# MAGIC      - `FECHA_ACTUALIZACION` → hora actual en **zona horaria de México** (`America/Mexico_City`).
# MAGIC    - Esto representa que el archivo o proceso fue **marcado como inactivo o reprocesado**.
# MAGIC
# MAGIC 3. **Caso `SR_REPROCESO = 1`:**
# MAGIC    - Se inserta un nuevo registro con los siguientes campos:
# MAGIC      - `SR_FOLIO`, `SR_SUBPROCESO`, `SR_SUBETAPA`
# MAGIC      - `FECHA_CREACION` con hora local de México
# MAGIC      - `ESTADO = 'True'`
# MAGIC    - Esto representa la **creación de un nuevo control activo** para el archivo PANCIN.
# MAGIC
# MAGIC 4. **Caso contrario (`else`):**
# MAGIC    - Si no se cumplen las condiciones anteriores, se registra en log `"Nothing to do"`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Razones por las que este enfoque es óptimo
# MAGIC
# MAGIC - **Eficiencia:**
# MAGIC   Cada operación (`INSERT` o `UPDATE`) actúa directamente sobre registros específicos,
# MAGIC   sin sobrecargar el sistema con merges o escrituras completas.
# MAGIC
# MAGIC - **Escalabilidad:**
# MAGIC   Ideal para tablas Delta con **miles o millones de registros**,
# MAGIC   ya que evita escaneos globales innecesarios.
# MAGIC
# MAGIC - **Claridad:**
# MAGIC   La lógica de negocio es simple, explícita y fácil de mantener.
# MAGIC
# MAGIC - **Consistencia temporal:**
# MAGIC   Se utiliza `from_utc_timestamp(current_timestamp(), 'America/Mexico_City')`
# MAGIC   para almacenar las fechas con la hora local, evitando desfases por UTC.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧭 Recomendación
# MAGIC
# MAGIC Si la tabla crece con el tiempo, se recomienda periódicamente optimizarla:
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.CONTROL_ARCHIVOS_PANCIN
# MAGIC ZORDER BY (SR_FOLIO, SR_SUBPROCESO, SR_SUBETAPA);

# COMMAND ----------

# DBTITLE 1,NC_0180_GEN_ARC
if params.sr_subetapa == "273":
    if params.sr_reproceso == "2":
        # Solo update
        spark.sql(
            f"""
            UPDATE {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.CONTROL_ARCHIVOS_PANCIN
            SET ESTADO = 'False',
                FECHA_ACTUALIZACION = from_utc_timestamp(current_timestamp(), 'America/Mexico_City')
            WHERE SR_FOLIO = '{params.sr_folio}'
              AND SR_SUBPROCESO = '{params.sr_subproceso}'
              AND SR_SUBETAPA = '{params.sr_subetapa}'
        """
        )
        logger.info("Estado actualizado a False")

    elif params.sr_reproceso == "1":
        # Solo inserción
        spark.sql(
            f"""
            INSERT INTO {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.CONTROL_ARCHIVOS_PANCIN
            (SR_FOLIO, SR_SUBPROCESO, SR_SUBETAPA, FECHA_CREACION, ESTADO)
            VALUES (
                '{params.sr_folio}',
                '{params.sr_subproceso}',
                '{params.sr_subetapa}',
                from_utc_timestamp(current_timestamp(), 'America/Mexico_City'),
                'True'
            )
        """
        )
        logger.info("Registro insertado con estado True")

    else:
        logger.info("Nothing to do")

else:
    logger.info("Nothing to do")

# COMMAND ----------

# DBTITLE 1,Notificacion de finalizado
Notify.send_notification("INFO", params)

# COMMAND ----------

# DBTITLE 1,Borrado de deltas
# Definimos la lista de tablas con sufijo dinámico organizadas por notebook

# ==================================================================================
# NB_PANCIN_MOV_0003_EXT_PREMOVS
# ==================================================================================
tables_0003 = [
    f"RESULTADO_MATRIZ_CONVIVENCIA_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0006_EXT_PREMOVS
# ==================================================================================
tables_0006 = [
    f"TEMP_PREMOVIMIENTOS_{params.sr_folio}",
    f"TEMP_FLUJO_1_{params.sr_folio}",
    f"TEMP_FLUJO_2_{params.sr_folio}",
    f"RESULTADO_PREMOVIMIENTOS_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0010_EXT_PREMOVS_CONCEP_MOV
# ==================================================================================
tables_0010_concep_mov = [
    f"TEMP_CONCEPTO_MOV_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0010_EXT_PREMOVS_MOV_SUBCTA
# ==================================================================================
tables_0010_mov_subcta = [
    f"TEMP_MOV_SUBCTA_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0010_EXT_PREMOVS_TIPO_SUBCTA
# ==================================================================================
tables_0010_tipo_subcta = [
    f"TEMP_TIPO_SUBCTA_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0010_EXT_PREMOVS_VAL_ACCION
# ==================================================================================
tables_0010_val_accion = [
    f"TEMP_VALOR_ACCION_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0010_EXT_PREMOVS
# ==================================================================================
tables_0010 = [
    f"TEMP_DATOS_CON_MARCA_{params.sr_folio}",
    f"DATOS_SIN_MARCA_{params.sr_folio}",
    f"TEMP_JOIN_TIPO_SUBCTA_{params.sr_folio}",
    f"TEMP_JOIN_MOV_SUBCTA_{params.sr_folio}",
    f"TEMP_JOIN_CONCEPTO_MOV_{params.sr_folio}",
    f"RESULTADO_MOVIMIENTOS_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0016_SUF_SALDO
# ==================================================================================
tables_0016 = [
    f"TEMP_PREMOVIMIENTOS_{params.sr_folio}",
    f"TEMP_TRANSFORMACION_{params.sr_folio}",
    f"TEMP_FILTRADO_MARCA_{params.sr_folio}",
    f"TEMP_JOIN_DATOS_{params.sr_folio}",
    f"RESULTADO_PREMOVIMIENTOS_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0020_SUF_SALDO
# ==================================================================================
tables_0020 = [
    f"TEMP_BALANCE_MOVS_{params.sr_folio}",
    f"RESULTADO_SUF_SALDO_FINAL_0020_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0030_MOVS_BALANCE
# ==================================================================================
tables_0030_balance = [
    f"TEMP_JOIN_MOVS_BALANCE_{params.sr_folio}",
    f"TEMP_MAXIMOS_ORACLE_{params.sr_folio}",
    f"TEMP_JOIN_MAXIMOS_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0030_MOVS_BALANCE_DATASET
# ==================================================================================
tables_0030_dataset = [
    f"RESULTADO_MOVS_BALANCE_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0030_MOVS_BALANCE_SEMAFOROS
# ==================================================================================
tables_0030_semaforos = [
    f"SEMAFOROS_ROJOS_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0050_CAN_BALANCE
# ==================================================================================
tables_0050 = [
    f"TEMP_MOVIMIENTOS_ETL_{params.sr_folio}",
    f"TEMP_MAX_ID_BALANCE_{params.sr_folio}",
    f"TEMP_JOIN_COMPLETO_{params.sr_folio}",
]

# ==================================================================================
# NB_PANCIN_MOV_0060_CIFRAS_CONTROL
# ==================================================================================
tables_0060 = [
    f"TEMP_CIFRAS_CONTROL_{params.sr_folio}",
]

# ==================================================================================
# CONSOLIDAR TODAS LAS TABLAS
# ==================================================================================
all_tables = (
    tables_0003
    + tables_0006
    + tables_0010_concep_mov
    + tables_0010_mov_subcta
    + tables_0010_tipo_subcta
    + tables_0010_val_accion
    + tables_0010
    + tables_0016
    + tables_0020
    + tables_0030_balance
    + tables_0030_dataset
    + tables_0050
    + tables_0060
)

# Recorremos la lista y aplicamos drop
for table in all_tables:
    db.drop_delta(table)


# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
