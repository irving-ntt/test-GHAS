# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_GMO_0100_DEV_FOV
# MAGIC
# MAGIC **Descripción:** Job paralelo que genera insumo de generación de movimientos para devolución de saldos excedentes de FOVISSSTE.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Devolución de Saldo Excedente FOVISSSTE
# MAGIC
# MAGIC **Trámite:** Devolución de Saldos Excedentes FOVISSSTE
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_FOV (Oracle - Datos de devolución de saldos excedentes FOVISSSTE)
# MAGIC - CIERREN.TCAFOGRAL_VALOR_ACCION (Oracle - Valores de acción para lookup)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_DEV_SALDOS_FOV_{sr_folio}
# MAGIC - TEMP_VALOR_ACCION_{sr_folio}
# MAGIC - NB_PATRIF_GMO_0100_DEV_FOV_RESULTADO_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_GMO_0100_DEV_FOV_001_OCI_DEV_SALDOS.sql
# MAGIC - NB_PATRIF_GMO_0100_DEV_FOV_002_OCI_VALOR_ACCION.sql
# MAGIC - NB_PATRIF_GMO_0100_DEV_FOV_003_DELTA_TRANSFORM.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **BD_100_DEV_SALDOS**: Extraer datos de devolución de saldos excedentes FOVISSSTE desde Oracle a Delta
# MAGIC 2. **DB_200_VALOR_ACCION**: Extraer valores de acción desde Oracle a Delta
# MAGIC 3. **LO_300_ID_VALOR**: Hacer lookup de valores de acción por fecha
# MAGIC 4. **CG_400_GEN_COLUMNAS**: Generar columnas de tipos de subcuenta (17 y 18)
# MAGIC 5. **MO_401_STRING**: Convertir FTC_FOLIO a string (implícito en Spark)
# MAGIC 6. **PI_600_COLUMNAS**: Pivotar columnas VIV08/VIV92 en columnas consolidadas
# MAGIC 7. **TF_800_GEN_REGLAS**: Aplicar reglas de negocio finales y cálculos
# MAGIC 8. **DS_500_SALDO**: Guardar resultado final en tabla Delta (equivalente al dataset)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332193?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (9).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams(
    {
        "sr_actualiza": str,
        "sr_etapa": str,
        "sr_fec_acc": str,
        "sr_fec_liq": str,
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_paso": str,
        "sr_proceso": str,
        "sr_reproceso": str,
        "sr_subetapa": str,
        "sr_subproceso": str,
        "sr_usuario": str,
    }
)
params.validate()

# Cargar configuración de entorno
conf = ConfManager()

# Inicializar managers
query = QueryManager()
db = DBXConnectionManager()

# Mostrar lista de archivos SQL disponibles
display(query.get_sql_list())

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332185?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,PASO 1: Extraer Devolución de Saldos Excedentes FOVISSSTE desde Oracle (BD_100_DEV_SALDOS)
statement_001 = query.get_statement(
    "NB_PATRIF_GMO_0100_DEV_FOV_001_OCI_DEV_SALDOS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_DEV_SLD_ECX_FOV=conf.TL_PRO_DEV_SLD_ECX_FOV,
    SR_FOLIO=params.sr_folio,
    ESTATUS=conf.ESTATUS,
)

# Escribir datos de Oracle a tabla Delta temporal
db.write_delta(
    f"TEMP_DEV_SALDOS_FOV_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_DEV_SALDOS_FOV_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332186?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (2).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,PASO 2: Extraer Valores de Acción desde Oracle (DB_200_VALOR_ACCION)
statement_002 = query.get_statement(
    "NB_PATRIF_GMO_0100_DEV_FOV_002_OCI_VALOR_ACCION.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRN_VALOR_ACCION=conf.TL_CRN_VALOR_ACCION,
    FCN_ID_SIEFORE=conf.FCN_ID_SIEFORE,
    FCN_ID_REGIMEN=conf.FCN_ID_REGIMEN,
)

# Escribir datos de Oracle a tabla Delta temporal
db.write_delta(
    f"TEMP_VALOR_ACCION_{params.sr_folio}",
    db.read_data("default", statement_002),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_VALOR_ACCION_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332186?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (3).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332186?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (4).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332186?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (5).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332186?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (6).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332186?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (7).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332186?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/FOVISSSTE/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_FOVimage (8).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC
# MAGIC # Reglas del transformer
# MAGIC
# MAGIC `NullToZero(FTN_MONTO_PESOS) <> 0
# MAGIC OR NullToZero(FTN_SDO_AIVS) <> 0
# MAGIC OR NullToZero(FTN_NUM_APLI_INTE_VIV) <> 0`

# COMMAND ----------

# DBTITLE 1,PASO 3: Aplicar Transformaciones Completas (LO_300 + CG_400 + MO_401 + PI_600 + TF_800)
statement_003 = query.get_statement(
    "NB_PATRIF_GMO_0100_DEV_FOV_003_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    TIPO_SUBCTA_VIV92_FOV=conf.TIPO_SUBCTA_VIV92_FOV,
    TIPO_SUBCTA_VIV08_FOV=conf.TIPO_SUBCTA_VIV08_FOV,
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO,
)

# Escribir resultado final a tabla Delta (equivalente a DS_500_SALDO)
db.write_delta(
    f"DELTA_500_SALDO_{params.sr_folio}",
    db.sql_delta(statement_003),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"DELTA_500_SALDO_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
