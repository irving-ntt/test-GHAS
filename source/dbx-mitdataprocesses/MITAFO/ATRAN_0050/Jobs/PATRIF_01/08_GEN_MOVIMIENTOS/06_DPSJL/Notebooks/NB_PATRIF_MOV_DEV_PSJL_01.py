# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_MOV_DEV_PSJL_01
# MAGIC
# MAGIC **Descripción:** Job paralelo que lee la información de suficiencia de saldos y devolución de saldos SJL (Sin Jornada Laboral), hace unpivot de conceptos de movimiento y genera dataset con movimientos individuales.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Devolución de Pagos Sin Jornada Laboral
# MAGIC
# MAGIC **Trámite:** Devolución de Pagos SJL (DPSJL)
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_DEV_PAG_SJL (Oracle - Datos de devolución de pagos SJL)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_DEV_PAG_SJL_{sr_folio} (temporal de extracción Oracle)
# MAGIC - DELTA_600_GEN_MOV_{sr_folio} (resultado final unpivoteado)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_01_001_OCI_DEV_PAG_SJL.sql
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_01_002_DELTA_UNPIVOT.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100_TL_PRO_DEV_PAG_SJL**: Extraer datos de devolución pagos SJL desde Oracle a Delta
# MAGIC 2. **CG_GEN_CAMPOS**: Generar 11 columnas con IDs de conceptos de movimiento
# MAGIC 3. **PI_300_GEN_REGISTROS**: Hacer unpivot de 11 columnas a 11 filas por registro
# MAGIC 4. **FI_400_ELIMINA_CEROS**: Filtrar registros con IMP_SOL <> 0
# MAGIC 5. **CG_GEN_FOLIO**: Agregar FTC_FOLIO y renombrar CON_MOV a FFN_ID_CONCEPTO_MOV
# MAGIC 6. **DS_600_GEN_MOV**: Guardar resultado en tabla Delta (equivalente al dataset)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332233?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_01image.png" style="max-width: 100%; height: auto;"/>
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
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332234?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_01image (1).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,PASO 1: Extraer Devolución de Pagos SJL desde Oracle (DB_100_TL_PRO_DEV_PAG_SJL)
statement_001 = query.get_statement(
    "NB_PATRIF_MOV_DEV_PSJL_01_001_OCI_DEV_PAG_SJL.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_DEV_PAG_SJL=conf.TL_PRO_DEV_PAG_SJL,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

# Escribir datos de Oracle a tabla Delta temporal
db.write_delta(
    f"TEMP_DEV_PAG_SJL_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_DEV_PAG_SJL_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332236?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_01image (2).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332231?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_01image (3).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332237?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_01image (4).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332235?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_01image (5).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332230?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_01image (6).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332232?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_01image (7).png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,PASO 2: Aplicar Unpivot y Transformaciones (CG_GEN_CAMPOS + PI_300 + FI_400 + CG_GEN_FOLIO)
statement_002 = query.get_statement(
    "NB_PATRIF_MOV_DEV_PSJL_01_002_DELTA_UNPIVOT.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    CON_MOV_IMP_PESOS_RET=conf.CON_MOV_IMP_PESOS_RET,
    CON_MOV_ACT_PESOS_RET=conf.CON_MOV_ACT_PESOS_RET,
    CON_MOV_REN_PESOS_RET=conf.CON_MOV_REN_PESOS_RET,
    CON_MOV_IMP_PESOS_CES_VEJ_PAT=conf.CON_MOV_IMP_PESOS_CES_VEJ_PAT,
    CON_MOV_IMP_PESOS_CES_VEJ_TRA=conf.CON_MOV_IMP_PESOS_CES_VEJ_TRA,
    CON_MOV_ACT_PESOS_CES_VEJ_PAT=conf.CON_MOV_ACT_PESOS_CES_VEJ_PAT,
    CON_MOV_ACT_PESOS_CES_VEJ_TRA=conf.CON_MOV_ACT_PESOS_CES_VEJ_TRA,
    CON_MOV_REN_PESOS_CES_VEJ_PAT=conf.CON_MOV_REN_PESOS_CES_VEJ_PAT,
    CON_MOV_REN_PESOS_CES_VEJ_TRA=conf.CON_MOV_REN_PESOS_CES_VEJ_TRA,
    CON_MOV_CS_IMP=conf.CON_MOV_CS_IMP,
    CON_MOV_VIV97=conf.CON_MOV_VIV97,
)

# Escribir resultado final a tabla Delta (equivalente a DS_600_GEN_MOV)
db.write_delta(
    f"DELTA_600_GEN_MOV_{params.sr_folio}",
    db.sql_delta(statement_002),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"DELTA_600_GEN_MOV_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
