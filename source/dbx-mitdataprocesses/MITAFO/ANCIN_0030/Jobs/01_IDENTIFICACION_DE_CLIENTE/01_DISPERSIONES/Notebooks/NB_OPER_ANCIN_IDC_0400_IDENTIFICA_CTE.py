# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_IDC_0400_IDENTIFICA_CTE
# MAGIC
# MAGIC **Descripción:** Job de Transformación de Identificación de Cliente para Identificados
# MAGIC
# MAGIC **Subetapa:** Identificación de Clientes
# MAGIC
# MAGIC **Trámite:** IDC - Identificación de Clientes
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - temp_vigentes_#SR_ID_ARCHIVO# (Delta - clientes vigentes)
# MAGIC - temp_novigentes_#SR_ID_ARCHIVO# (Delta - clientes no vigentes)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_NO_VIGENTES_RECHAZADOS_{sr_id_archivo}
# MAGIC - TEMP_NO_VIGENTES_FILTRADOS_{sr_id_archivo}
# MAGIC - TEMP_UNION_IDENTIFICADOS_{sr_id_archivo}
# MAGIC - TEMP_MARCA_0_{sr_id_archivo}
# MAGIC - TEMP_PROCESA_IDENTIFICADOS_{sr_id_archivo}
# MAGIC - TEMP_UNION_FINAL_{sr_id_archivo}
# MAGIC - TEMP_PROCESO_FINAL_{sr_id_archivo}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PANCIN_IDC_0400_IDENTIFICA_CTE_001_NO_VIGENTES_FILTRADOS.sql
# MAGIC - NB_PANCIN_IDC_0400_IDENTIFICA_CTE_002_NO_VIGENTES_RECHAZADOS.sql
# MAGIC - NB_PANCIN_IDC_0400_IDENTIFICA_CTE_003_UNION_IDENTIFICADOS.sql
# MAGIC - NB_PANCIN_IDC_0400_IDENTIFICA_CTE_004_MARCA_0.sql
# MAGIC - NB_PANCIN_IDC_0400_IDENTIFICA_CTE_005_PROCESA_IDENTIFICADOS.sql
# MAGIC - NB_PANCIN_IDC_0400_IDENTIFICA_CTE_006_UNION_FINAL.sql
# MAGIC - NB_PANCIN_IDC_0400_IDENTIFICA_CTE_007_PROCESO_FINAL.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DS_100_VIGENTES + CO_110_DIVIDE**: Leer temp_vigentes_02 completa
# MAGIC 2. **DS_200_NO_VIGENTES_0 + SO_210_IZQ + FI_300_FILTRO**: Leer temp_novigentes con filtro y ordenamiento
# MAGIC    - **Salida 0**: Datos que pasan filtro → FU_400_UNION_IDENTIFICADOS
# MAGIC    - **Salida 1**: Datos rechazados → CG_310_MARCA_0
# MAGIC 3. **FU_400_UNION_IDENTIFICADOS**: Unir vigentes + no vigentes filtrados
# MAGIC 4. **CG_310_MARCA_0**: Procesar datos rechazados del filtro
# MAGIC 5. **SO_410 + RM_420 + TF_430**: Ordenar + Eliminar duplicados + Aplicar transformaciones
# MAGIC 6. **FU_440_UNION_F**: Unir datos procesados + datos rechazados
# MAGIC 7. **CP_600 + AG_700 + JO_800 + FL_810 + CL_820/830 + FU_900**: Conteo + Join + Filtro + Marcas + Unión
# MAGIC 4. **CG_310_MARCA_0**: Agregar marcas de tipo 0 con SQL desde Delta
# MAGIC 5. **SO_410_ORDENA**: Ordenar registros con SQL desde Delta
# MAGIC 6. **RM_420_DUPLICADOS**: Eliminar duplicados con SQL desde Delta
# MAGIC 7. **TF_430_MARCA_1**: Aplicar transformaciones y marcas de tipo 1 con SQL desde Delta
# MAGIC 8. **FU_440_UNION_F**: Unir datos finales con SQL desde Delta
# MAGIC 9. **DS_500_ENCONTRADOS**: Escribir clientes encontrados en Delta
# MAGIC 10. **CP_600_VIG**: Procesar lógica de vigencia con SQL desde Delta
# MAGIC 11. **AG_700_COUNT**: Generar conteos y estadísticas con SQL desde Delta
# MAGIC 12. **JO_800_VIGENCIA**: Aplicar joins de vigencia con SQL desde Delta
# MAGIC 13. **FL_810_VIGENCIA**: Aplicar filtros de vigencia con SQL desde Delta
# MAGIC 14. **CL_820_MARCADUP**: Marcar duplicados con SQL desde Delta
# MAGIC 15. **CL_830_MARCADUPN**: Marcar duplicados no vigentes con SQL desde Delta
# MAGIC 16. **FU_900_CTAS**: Escribir resultado final en Delta
# MAGIC 17. **Notebook finaliza**: Procesamiento de identificación de clientes completado

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams(
    {
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_tipo_layout": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
        "sr_paso": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("NB_PANCIN_IDC_0400")))

# COMMAND ----------

# DBTITLE: 1. Leer datos vigentes (temp_vigentes_#SR_ID_ARCHIVO#)
# Las tablas temp_vigentes_#SR_ID_ARCHIVO# y temp_novigentes_#SR_ID_ARCHIVO# ya existen
# No necesitamos generar tablas Delta temporales para ellas

if conf.debug:
    display(db.read_delta(f"temp_vigentes_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 2. Leer datos no vigentes con filtro y ordenamiento (temp_novigentes_#SR_ID_ARCHIVO#)
statement_no_vigentes = query.get_statement(
    "NB_PANCIN_IDC_0400_IDENTIFICA_CTE_001_NO_VIGENTES_FILTRADOS.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# Crear tabla Delta con datos no vigentes filtrados
db.write_delta(
    f"TEMP_NO_VIGENTES_FILTRADOS_{params.sr_id_archivo}",
    db.sql_delta(statement_no_vigentes),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_NO_VIGENTES_FILTRADOS_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 2B. Procesar datos no vigentes rechazados del filtro
statement_no_vigentes_rechazados = query.get_statement(
    "NB_PANCIN_IDC_0400_IDENTIFICA_CTE_002_NO_VIGENTES_RECHAZADOS.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# Crear tabla Delta con datos rechazados
db.write_delta(
    f"TEMP_NO_VIGENTES_RECHAZADOS_{params.sr_id_archivo}",
    db.sql_delta(statement_no_vigentes_rechazados),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_NO_VIGENTES_RECHAZADOS_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 3. Unir datos de identificados (vigentes + no vigentes filtrados)
statement_union_identificados = query.get_statement(
    "NB_PANCIN_IDC_0400_IDENTIFICA_CTE_003_UNION_IDENTIFICADOS.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# Crear tabla Delta con datos unidos
db.write_delta(
    f"TEMP_UNION_IDENTIFICADOS_{params.sr_id_archivo}",
    db.sql_delta(statement_union_identificados),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_UNION_IDENTIFICADOS_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 4. Agregar marcas de tipo 0
statement_marca_0 = query.get_statement(
    "NB_PANCIN_IDC_0400_IDENTIFICA_CTE_004_MARCA_0.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# Crear tabla Delta con marcas de tipo 0
db.write_delta(
    f"TEMP_MARCA_0_{params.sr_id_archivo}", db.sql_delta(statement_marca_0), "overwrite"
)

if conf.debug:
    display(db.read_delta(f"TEMP_MARCA_0_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 5. Procesar datos identificados (Ordenar + Eliminar duplicados + Transformar)
statement_procesa_identificados = query.get_statement(
    "NB_PANCIN_IDC_0400_IDENTIFICA_CTE_005_PROCESA_IDENTIFICADOS.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# Crear tabla Delta con datos procesados
db.write_delta(
    f"TEMP_PROCESA_IDENTIFICADOS_{params.sr_id_archivo}",
    db.sql_delta(statement_procesa_identificados),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_PROCESA_IDENTIFICADOS_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 6. Unir datos finales (Procesados + Rechazados)
statement_union_final = query.get_statement(
    "NB_PANCIN_IDC_0400_IDENTIFICA_CTE_006_UNION_FINAL.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# Crear tabla Delta con datos finales unidos
db.write_delta(
    f"TEMP_UNION_FINAL_{params.sr_id_archivo}",
    db.sql_delta(statement_union_final),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_UNION_FINAL_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 8. Proceso final (Conteo + Join + Filtro + Marcas + Unión)
statement_proceso_final = query.get_statement(
    "NB_PANCIN_IDC_0400_IDENTIFICA_CTE_007_PROCESO_FINAL.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# Crear tabla Delta con resultado final
db.write_delta(
    f"temp_encontrados_06_{params.sr_id_archivo}",
    db.sql_delta(statement_proceso_final),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"temp_encontrados_06_{params.sr_id_archivo}"))

# COMMAND ----------

# DBTITLE: 18. Limpieza final
# Limpiar datos del notebook y liberar memoria
CleanUpManager.cleanup_notebook(locals())
