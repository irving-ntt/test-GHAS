# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_GMO_0100_DEV_SALD_EXC
# MAGIC
# MAGIC **Descripción:** Job paralelo que genera insumo de generación de movimientos para devolución de saldos excedentes de INFONAVIT.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Devolución de Saldo Excedente
# MAGIC
# MAGIC **Trámite:** Devolución de Saldos Excedentes INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO (Oracle - Datos de devolución de saldos excedentes)
# MAGIC - CIERREN.TCAFOGRAL_VALOR_ACCION (Oracle - Valores de acción para lookup)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_DEV_SALDOS_{sr_folio}
# MAGIC - TEMP_VALOR_ACCION_{sr_folio}
# MAGIC - NB_PATRIF_GMO_0100_DEV_SALD_EXC_RESULTADO_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_GMO_0100_DEV_SALD_EXC_001_OCI_DEV_SALDOS.sql
# MAGIC - NB_PATRIF_GMO_0100_DEV_SALD_EXC_002_OCI_VALOR_ACCION.sql
# MAGIC - NB_PATRIF_GMO_0100_DEV_SALD_EXC_003_DELTA_TRANSFORM.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **BD_100_DEV_SALDOS**: Extraer datos de devolución de saldos excedentes desde Oracle a Delta
# MAGIC 2. **DB_200_VALOR_ACCION**: Extraer valores de acción desde Oracle a Delta
# MAGIC 3. **LO_300_ID_VALOR**: Hacer lookup de valores de acción por fecha
# MAGIC 4. **CG_400_GEN_COLUMNAS**: Generar columnas de tipos de subcuenta
# MAGIC 5. **MO_401_STRING**: Convertir FTC_FOLIO a string (implícito en Spark)
# MAGIC 6. **FI_500_FILTRA_SUBP**: Filtrar y separar datos por subproceso (348 y 349)
# MAGIC 7. **PI_600_COLUMNAS**: Pivotar columnas VIV97/VIV92 (solo para subproceso 349)
# MAGIC 8. **FU_700_COMPLEMENTO**: Combinar ambos flujos con funnel (UNION ALL)
# MAGIC 9. **TF_800_GEN_REGLAS**: Aplicar reglas de negocio finales y cálculos
# MAGIC 10. **DS_500_SALDO**: Guardar resultado final en tabla Delta (equivalente al dataset)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332157?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC.png" style="max-width: 100%; height: auto;"/>
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
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332158?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_01.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,PASO 1: Extraer Devolución de Saldos Excedentes desde Oracle (BD_100_DEV_SALDOS)
statement_001 = query.get_statement(
    "NB_PATRIF_GMO_0100_DEV_SALD_EXC_001_OCI_DEV_SALDOS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_CRE_DEVO_SALD_EXC=conf.TL_CRE_DEVO_SALD_EXC,
    SR_FOLIO=params.sr_folio,
    ESTATUS=conf.ESTATUS,
)

# Escribir datos de Oracle a tabla Delta temporal
db.write_delta(
    f"TEMP_DEV_SALDOS_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_DEV_SALDOS_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332159?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_02.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,PASO 2: Extraer Valores de Acción desde Oracle (DB_200_VALOR_ACCION)
statement_002 = query.get_statement(
    "NB_PATRIF_GMO_0100_DEV_SALD_EXC_002_OCI_VALOR_ACCION.sql",
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
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332167?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_image (9).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332160?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_image (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332162?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_image (2).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332161?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_image (3).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332163?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_image (4).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332166?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_image (5).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332165?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_image (6).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332164?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/03_DEV_SALDO_EXCED/INFONAVIT/Notebooks/assets/NB_PATRIF_GMO_0100_DEV_SALD_EXC_image (7).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC
# MAGIC # Regla del transformer:
# MAGIC
# MAGIC `NullToZero(FTN_MONTO_PESOS) <> 0
# MAGIC OR NullToZero(FTN_SDO_AIVS) <> 0
# MAGIC OR NullToZero(FTN_NUM_APLI_INTE_VIV) <> 0`
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,PASO 3: Aplicar Transformaciones Completas (LO_300 + CG_400 + MO_401 + FI_500 + PI_600 + FU_700 + TF_800)
statement_003 = query.get_statement(
    "NB_PATRIF_GMO_0100_DEV_SALD_EXC_003_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    TIPO_SUBCTA_VIV97=conf.TIPO_SUBCTA_VIV97,
    TIPO_SUBCTA_VIV92=conf.TIPO_SUBCTA_VIV92,
    SUBPROUG=conf.SUBPROUG,
    SUBPROUG_TA=conf.SUBPROUG_TA,
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO,
)

# Escribir resultado final a tabla Delta (equivalente a DS_500_SALDO)
db.write_delta(
    f"DELTA_500_SALDO_{params.sr_folio}",
    db.sql_delta(statement_003),
    "overwrite",
)

if conf.debug:
    display(
        db.read_delta(f"DELTA_500_SALDO_{params.sr_folio}")
    )

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
