# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_IDC_0200_BUC_CTE
# MAGIC
# MAGIC **Descripción:** Job de Transformación de Identificación de Cliente para Validación
# MAGIC
# MAGIC **Subetapa:** Identificación de Cliente
# MAGIC
# MAGIC **Trámite:** 7 - IDC
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - DS_IMSS_202509111456082467_IDC_09.ds (Delta - datos de identificación)
# MAGIC - DS_IMSS_202509111456082467_IDC_BUC_01.ds (Delta - datos de dispersión)
# MAGIC - DS_IMSS_202509111456082467_IDC_BUC_02.ds (Delta - datos de BUC)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_IMSS_ISSSTE_{sr_folio}
# MAGIC - TEMP_MAPEO_DATOS_{sr_folio}
# MAGIC - TEMP_JOIN_SUBCTA_{sr_folio}
# MAGIC - TEMP_REMOVE_DUPLICATES_{sr_folio}
# MAGIC - TEMP_MAPEO_FINAL_{sr_folio}
# MAGIC - DS_IMSS_202509111456082467_IDC_01.ds (Delta - resultado final)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_IDC_0200_BUC_CTE_001_JOIN_DATOS.sql
# MAGIC - NB_PANCIN_IDC_0200_BUC_CTE_002_MAPEO_CAMPOS.sql
# MAGIC - NB_PANCIN_IDC_0200_BUC_CTE_003_JOIN_SUBCTA.sql
# MAGIC - NB_PANCIN_IDC_0200_BUC_CTE_004_REMOVE_DUPLICATES.sql
# MAGIC - NB_PANCIN_IDC_0200_BUC_CTE_005_MAPEO_FINAL.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DS_106_IMSS_ISSSTE**: Leer datos de identificación desde dataset
# MAGIC 2. **CP_107_MAPEO**: Mapear campos de identificación (NSS/CURP)
# MAGIC 3. **DS_400_BUC + DS_400_DISPERSION**: Leer datos de BUC y dispersión
# MAGIC 4. **Join_96**: Hacer join entre datos de BUC y dispersión
# MAGIC 5. **JO_109_SUBCTA**: Join con datos de subcuenta
# MAGIC 6. **RD_600_SUBCTA**: Remover duplicados por NSS_CURP y CTA_INVDUAL
# MAGIC 7. **CP_700_MAPP_CAMPOS**: Mapear campos finales
# MAGIC 8. **DS_111_TRAMITE**: Escribir resultado final en dataset

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams({
    "sr_folio": str,
    "tipo_identificador": str,
    "ds_nm_dataset": str,
    "clv_unid_neg": str,
    "clv_sufijo_01": str,
    "clv_sufijo_09": str,
    "ext_arc_ds": str,
    "rt_path_temp": str,
    "tl_cuo_identificador": str,
    "tl_val_ident_cte": str,
    "tl_cre_pre_dispersion": str,
    "tl_cre_etl_buc": str,
    "tl_cre_etl_disp_cta": str
})

# Cargar configuración de entorno
conf = ConfManager()
# Validar parámetros
params.validate()

# Inicializar managers
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE DATOS DE IDENTIFICACIÓN
# Leer datos de identificación desde dataset
statement_imss_issste = query.get_statement(
    "NB_PANCIN_IDC_0200_BUC_CTE_001_JOIN_DATOS.sql",
    DS_NM_DATASET=params.ds_nm_dataset,
    SR_FOLIO=params.sr_folio,
    CLV_UNID_NEG=params.clv_unid_neg,
    CLV_SUFIJO_09=params.clv_sufijo_09,
    EXT_ARC_DS=params.ext_arc_ds
)

# Escribir datos de identificación en Delta
db.write_delta(
    f"TEMP_IMSS_ISSSTE_{params.sr_folio}",
    db.read_delta(statement_imss_issste),
    "overwrite"
)

logger.info("Datos de identificación extraídos y guardados en Delta")

# COMMAND ----------

# DBTITLE 2,MAPEO DE CAMPOS DE IDENTIFICACIÓN
# Mapear campos de identificación (NSS/CURP)
statement_mapeo = query.get_statement(
    "NB_PANCIN_IDC_0200_BUC_CTE_002_MAPEO_CAMPOS.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)

# Escribir datos mapeados en Delta
db.write_delta(
    f"TEMP_MAPEO_DATOS_{params.sr_folio}",
    db.sql_delta(statement_mapeo),
    "overwrite"
)

logger.info("Campos de identificación mapeados exitosamente")

# COMMAND ----------

# DBTITLE 3,JOIN CON DATOS DE SUBCUENTA
# Hacer join con datos de subcuenta
statement_join_subcta = query.get_statement(
    "NB_PANCIN_IDC_0200_BUC_CTE_003_JOIN_SUBCTA.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)

# Escribir datos de join en Delta
db.write_delta(
    f"TEMP_JOIN_SUBCTA_{params.sr_folio}",
    db.sql_delta(statement_join_subcta),
    "overwrite"
)

logger.info("Join con datos de subcuenta completado")

# COMMAND ----------

# DBTITLE 4,REMOVER DUPLICADOS
# Remover duplicados por NSS_CURP y CTA_INVDUAL
statement_remove_dup = query.get_statement(
    "NB_PANCIN_IDC_0200_BUC_CTE_004_REMOVE_DUPLICATES.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)

# Escribir datos sin duplicados en Delta
db.write_delta(
    f"TEMP_REMOVE_DUPLICATES_{params.sr_folio}",
    db.sql_delta(statement_remove_dup),
    "overwrite"
)

logger.info("Duplicados removidos exitosamente")

# COMMAND ----------

# DBTITLE 5,MAPEO FINAL DE CAMPOS
# Mapear campos finales
statement_mapeo_final = query.get_statement(
    "NB_PANCIN_IDC_0200_BUC_CTE_005_MAPEO_FINAL.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)

# Escribir resultado final en Delta
db.write_delta(
    f"TEMP_MAPEO_FINAL_{params.sr_folio}",
    db.sql_delta(statement_mapeo_final),
    "overwrite"
)

logger.info("Mapeo final de campos completado")

# COMMAND ----------

# DBTITLE 6,ESCRITURA DE RESULTADO FINAL
# Escribir resultado final en dataset
db.write_delta(
    f"DS_{params.ds_nm_dataset}_{params.sr_folio}_{params.clv_unid_neg}_{params.clv_sufijo_01}{params.ext_arc_ds}",
    db.read_delta(f"TEMP_MAPEO_FINAL_{params.sr_folio}"),
    "overwrite"
)

logger.info("Resultado final escrito en dataset exitosamente")

# COMMAND ----------

# DBTITLE 7,LIMPIEZA DE DATOS TEMPORALES
# Limpiar datos temporales
db.sql_delta(f"DROP TABLE IF EXISTS {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_IMSS_ISSSTE_{params.sr_folio}")
db.sql_delta(f"DROP TABLE IF EXISTS {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_MAPEO_DATOS_{params.sr_folio}")
db.sql_delta(f"DROP TABLE IF EXISTS {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_JOIN_SUBCTA_{params.sr_folio}")
db.sql_delta(f"DROP TABLE IF EXISTS {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_REMOVE_DUPLICATES_{params.sr_folio}")
db.sql_delta(f"DROP TABLE IF EXISTS {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_MAPEO_FINAL_{params.sr_folio}")

logger.info("Datos temporales limpiados exitosamente")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
