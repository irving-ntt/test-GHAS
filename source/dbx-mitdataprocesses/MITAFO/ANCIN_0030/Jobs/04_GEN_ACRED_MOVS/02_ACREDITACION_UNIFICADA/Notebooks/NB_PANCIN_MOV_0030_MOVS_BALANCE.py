# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0030_MOVS_BALANCE
# MAGIC
# MAGIC **Descripción:** Inserción de movimientos en tabla de balance con procesamiento de semáforos
# MAGIC
# MAGIC **Subetapa:** Inserción de movimientos en balance
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - RESULTADO_MOVIMIENTOS_{sr_folio} (Delta - sufijo 02)
# MAGIC - RESULTADO_SUF_SALDO_FINAL_0020_{sr_folio} (Delta - sufijo 03)
# MAGIC - CX_CRN_ESQUEMA.TL_CRN_BALANCE_MOVS
# MAGIC - CX_CRE_ESQUEMA.TL_CRE_MOVIMIENTOS
# MAGIC - CX_CRE_ESQUEMA.TL_CRE_PRE_MOVIMIENTOS
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_JOIN_MOVS_BALANCE_{sr_folio}
# MAGIC - TEMP_MAXIMOS_ORACLE_{sr_folio}
# MAGIC - TEMP_JOIN_MAXIMOS_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_MOV_0030_MOVS_BALANCE_001.sql
# MAGIC - NB_PANCIN_MOV_0030_MOVS_BALANCE_002.sql
# MAGIC - NB_PANCIN_MOV_0030_MOVS_BALANCE_003.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DS_100**: Lee datos de movimientos (sufijo 02)
# MAGIC 2. **DS_110**: Lee datos de balance (sufijo 03)
# MAGIC 3. **JO_120**: Left Join entre DS_100 y DS_110
# MAGIC 4. **CG_130**: Column Generator (agrega LLAVE)
# MAGIC 5. **DB_200**: Extrae máximos desde Oracle
# MAGIC 6. **JO_140**: Inner Join entre CG_130 y DB_200
# MAGIC 7. Este notebook finaliza aquí; las 3 salidas del transformer se calculan en notebooks paralelos
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332290?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_image.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
# Solo incluir parámetros que realmente se usan + obligatorios del framework
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
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332288?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_image (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 2,JO_120 - LEFT JOIN ENTRE MOVIMIENTOS Y BALANCE
# Realizar Left Join entre datos de movimientos y datos de balance
# Equivalente al stage JO_120

logger.info("Realizando Left Join entre movimientos y balance")

# Cargar query para realizar join entre tabla Delta de movimientos y tabla Delta de balance
statement_001 = query.get_statement(
    "NB_PANCIN_MOV_0030_MOVS_BALANCE_001.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# Procesar join y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_JOIN_MOVS_BALANCE_{params.sr_folio}",
    db.sql_delta(statement_001),
    "overwrite",
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_JOIN_MOVS_BALANCE_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332287?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_image (4).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 3,DB_200_CRE_BALANCE_MOVS - EXTRACCIÓN DE MÁXIMOS DESDE ORACLE
# Extraer máximos desde Oracle para balance de movimientos
# Equivalente al stage DB_200_CRE_BALANCE_MOVS

logger.info("Ejecutando consulta Oracle para extraer máximos")

# Cargar query para extraer máximos desde Oracle
statement_002 = query.get_statement(
    "NB_PANCIN_MOV_0030_MOVS_BALANCE_002.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRN_BALANCE_MOVS=conf.TL_CRN_BALANCE_MOVS,
    TL_CRE_MOVIMIENTOS=conf.TL_CRE_MOVIMIENTOS,
)

# COMMAND ----------

# DBTITLE 4,GUARDAR MÁXIMOS ORACLE EN TABLA DELTA TEMPORAL
# Leer datos de Oracle y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_MAXIMOS_ORACLE_{params.sr_folio}",
    db.read_data("default", statement_002),
    "overwrite",
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_MAXIMOS_ORACLE_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 📘 Reglas de Derivación – df_join
# MAGIC
# MAGIC | **Derivación (Regla de negocio)** | **Variable de la etapa** |
# MAGIC |----------------------------------|---------------------------|
# MAGIC | if (FCN_ID_TIPO_MONTO = 184) then (FTN_DISP_ACCIONES * FCN_VALOR_ACCION) else 0 | **CALCPESOS** |
# MAGIC | if (FCN_ID_TIPO_MONTO = 184 and CALCPESOS >= FTF_MONTO_PESOS_SUM) then 'S' else 'N' | **SALSUFPES** |
# MAGIC | if ((FCN_ID_TIPO_MONTO = 199 or FCN_ID_TIPO_MONTO = 185 or FCN_ID_TIPO_MONTO = 200) and FTN_DISP_ACCIONES >= FTF_MONTO_ACCIONES_SUM) then 'S' else 'N' | **SALSUFACC** |
# MAGIC | if ((FCN_ID_TIPO_MOV = 181) OR (FCN_ID_TIPO_MOV = 180 and SALSUFPES = 'S' and FTF_MONTO_PESOS = FTF_MONTO_PESOS_SOL) OR (FCN_ID_TIPO_MOV = 180 and SALSUFACC = 'S' and FTF_MONTO_ACCIONES = FTF_MONTO_ACCIONES_SOL)) then 190 else if ((FCN_ID_TIPO_MOV = 180 and SALSUFPES = 'S' and FTF_MONTO_PESOS < FTF_MONTO_PESOS_SOL and FCN_ID_SALDO_OPERA = 11) OR (FCN_ID_TIPO_MOV = 180 and SALSUFACC = 'S' and FTF_MONTO_ACCIONES < FTF_MONTO_ACCIONES_SOL and FCN_ID_SALDO_OPERA = 11)) then 192 else 191 | **SEMAFORO** |
# MAGIC | if SEMAFORO in (190,192) then **row_number()** (ordenado por FTC_FOLIO) else 0 | **ID** |
# MAGIC | if SEMAFORO in (190,192) then (MAXIMO + ID) else CONTADOR | **CONTADOR** |
# MAGIC | **row_number()** (ordenado por FTC_FOLIO, sin condición) | **ID2** |
# MAGIC | MAXIMO2 + ID2 | **CONTADOR2** |
# MAGIC | if (SEMAFORO = 191) then (SEMROJOS + 1) else SEMROJOS | **SEMROJOS** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧠 Notas
# MAGIC
# MAGIC - **CALCPESOS**: calcula el valor en pesos solo cuando el tipo de monto es 184.  
# MAGIC - **SALSUFPES / SALSUFACC**: indican suficiencia de pesos o acciones disponibles.  
# MAGIC - **SEMAFORO**: clasifica los registros en:
# MAGIC   - 🟢 **190** → condición cumplida  
# MAGIC   - 🟡 **192** → condición parcial  
# MAGIC   - 🔴 **191** → condición no cumplida  
# MAGIC - **ID**: numeración incremental (row_number) solo cuando el semáforo está en verde o amarillo.  
# MAGIC - **ID2**: numeración incremental general, sin filtro.  
# MAGIC - **CONTADOR / CONTADOR2**: valores acumulativos dependientes de los ID.  
# MAGIC - **SEMROJOS**: contador de registros con semáforo rojo.  

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, expr, lit, row_number, when
from pyspark.sql.window import Window
from pyspark.sql.functions import from_utc_timestamp, current_timestamp

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# DBTITLE 5,JO_140 - INNER JOIN CON MÁXIMOS ORACLE
# Realizar Inner Join entre datos del join y máximos de Oracle
# Equivalente al stage JO_140

logger.info("Realizando Inner Join con máximos de Oracle")

# Cargar query para realizar join entre tabla temporal y máximos Oracle
statement_003 = query.get_statement(
    "NB_PANCIN_MOV_0030_MOVS_BALANCE_003.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

df_join = db.sql_delta(statement_003)

# Definir el window para ordenación
w_all = Window.orderBy("FTC_FOLIO")  # row_number general
w_semaforo = Window.orderBy("FTC_FOLIO")

df_tf = (
    df_join
    # Calcular variables base como antes...
    .withColumn(
        "CALCPESOS",
        F.when(F.col("FCN_ID_TIPO_MONTO") == 184,
               F.col("FTN_DISP_ACCIONES") * F.col("FCN_VALOR_ACCION")).otherwise(0)
    )
    .withColumn(
        "SALSUFPES",
        F.when(
            (F.col("FCN_ID_TIPO_MONTO") == 184) &
            (F.col("CALCPESOS") >= F.col("FTF_MONTO_PESOS_SUM")),
            "S"
        ).otherwise("N")
    )
    .withColumn(
        "SALSUFACC",
        F.when(
            (F.col("FCN_ID_TIPO_MONTO").isin(199,185,200)) &
            (F.col("FTN_DISP_ACCIONES") >= F.col("FTF_MONTO_ACCIONES_SUM")),
            "S"
        ).otherwise("N")
    )
    .withColumn(
        "SEMAFORO",
        F.when(
            (F.col("FCN_ID_TIPO_MOV") == 181) |
            ((F.col("FCN_ID_TIPO_MOV") == 180) &
             (F.col("SALSUFPES") == "S") &
             (F.col("FTF_MONTO_PESOS") == F.col("FTF_MONTO_PESOS_SOL"))) |
            ((F.col("FCN_ID_TIPO_MOV") == 180) &
             (F.col("SALSUFACC") == "S") &
             (F.col("FTF_MONTO_ACCIONES") == F.col("FTF_MONTO_ACCIONES_SOL"))),
            190
        ).when(
            ((F.col("FCN_ID_TIPO_MOV") == 180) &
             (F.col("SALSUFPES") == "S") &
             (F.col("FTF_MONTO_PESOS") < F.col("FTF_MONTO_PESOS_SOL")) &
             (F.col("FCN_ID_SALDO_OPERA") == 11)) |
            ((F.col("FCN_ID_TIPO_MOV") == 180) &
             (F.col("SALSUFACC") == "S") &
             (F.col("FTF_MONTO_ACCIONES") < F.col("FTF_MONTO_ACCIONES_SOL")) &
             (F.col("FCN_ID_SALDO_OPERA") == 11)),
            192
        ).otherwise(191)
    )

    # ID2: row_number global
    .withColumn("ID2", F.row_number().over(w_all))

    # ID: row_number solo para SEMAFORO en (190,192)
    .withColumn(
        "ID",
        F.when(
            F.col("SEMAFORO").isin(190,192),
            F.row_number().over(w_semaforo)
        ).otherwise(F.lit(0))
    )

    # CONTADOR y CONTADOR2 dependen de los nuevos IDs
    .withColumn(
        "CONTADOR",
        F.when(F.col("SEMAFORO").isin(190,192),
               F.col("MAXIMO") + F.col("ID")).otherwise(F.lit(0))
    )
    .withColumn("CONTADOR2", F.col("MAXIMO2") + F.col("ID2"))

    # SEMROJOS: acumulador lógico de casos 191 (si se quiere contar, lo hacemos al final)
    .withColumn(
        "SEMROJOS",
        F.when(F.col("SEMAFORO") == 191, F.lit(1)).otherwise(F.lit(0))
    )
)
if conf.debug: 
    display(df_tf)

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332286?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_image (3).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332289?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0030_MOVS_BALANCE_image (2).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# Procesar join y guardar en tabla Delta temporal
db.write_delta(
    f"TEMP_JOIN_MAXIMOS_{params.sr_folio}",
    df_tf,
    "overwrite",
)

# Mostrar resultados en modo debug
if conf.debug:
    display(db.read_delta(f"TEMP_JOIN_MAXIMOS_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 6,FINALIZACIÓN PRE-TRANSFORMER
logger.info(
    "Joins completados. Finalizando notebook principal previo al transformer. Salidas se calcularán en notebooks paralelos."
)
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())
