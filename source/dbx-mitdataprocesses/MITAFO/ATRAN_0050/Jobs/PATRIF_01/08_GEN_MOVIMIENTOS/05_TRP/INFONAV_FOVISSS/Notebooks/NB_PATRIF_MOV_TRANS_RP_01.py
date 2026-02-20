# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_MOV_TRANS_RP_01
# MAGIC
# MAGIC **Descripción:** Job paralelo que lee la información que la subetapa de Suficiencia de Saldos dejó preparada, filtra por tipo subcuenta, homologa nombres de campos y guarda en dataset para generación de movimientos de Transferencias y Retiro Parcial.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Transferencias y Retiro Parcial
# MAGIC
# MAGIC **Trámite:** Transferencias y Retiro Parcial (TRP) - INFONAVIT y FOVISSSTE
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - RESULTADO_SUF_SALDOS_10_{sr_folio} (Delta - Datos de suficiencia de saldos previamente procesados)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - DELTA_TRA_DB_ATRIF_DS_700_SALDOS_PROC_FOV (lectura - tabla fija generada por job previo)
# MAGIC - DELTA_500_SALDO_{sr_folio} (escritura - output final)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_MOV_TRANS_RP_01_001_DELTA_TRANSFORM.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DS_700_SALDOS_PROC**: Leer dataset de entrada (tabla Delta con sufijo 10)
# MAGIC 2. **FI_100_TIPO_SUBCTA**: Filtrar por tipo subcuenta y homologar campos (2 outputs)
# MAGIC    - Output 0: VIV97 (15) o VIV08 (18) - con indicador VIV97=1
# MAGIC    - Output 1: VIV92 (16) o VIV92 FOV (17) - con indicador VIV92=1
# MAGIC 3. **FU_200_REG**: Combinar ambos flujos con funnel (UNION ALL)
# MAGIC 4. **MO_300_NONULL**: Convertir campos críticos de nullable a NOT NULL
# MAGIC 5. **DS_500_SALDO**: Guardar resultado en tabla Delta (equivalente al dataset con sufijo 01)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332210?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/05_TRP/INFONAV_FOVISSS/Notebooks/assets/NB_PATRIF_MOV_TRANS_RP_01.png" style="max-width: 100%; height: auto;"/>
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

# DBTITLE 1,PASO 1: Aplicar Transformaciones Completas (FI_100 + FU_200 + MO_300)
statement_001 = query.get_statement(
    "NB_PATRIF_MOV_TRANS_RP_01_001_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    DELTA_700_SALDOS_PROC_FOV=f"{conf.DELTA_700_SALDOS_PROC_FOV}_{params.sr_folio}",
    TIPO_SUBCTA_VIV97=conf.TIPO_SUBCTA_VIV97,
    TIPO_SUBCTA_VIV92=conf.TIPO_SUBCTA_VIV92,
    TIPO_SUBCTA_VIV08_FOV=conf.TIPO_SUBCTA_VIV08_FOV,
    TIPO_SUBCTA_VIV92_FOV=conf.TIPO_SUBCTA_VIV92_FOV,
)

# Escribir resultado final a tabla Delta (equivalente a DS_500_SALDO)
db.write_delta(
    f"DELTA_500_SALDO_{params.sr_folio}",
    db.sql_delta(statement_001),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"DELTA_500_SALDO_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
