# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_id_archivo": str,
    "sr_fec_arc": str,
    "sr_paso": str,
    "sr_id_archivo_06": str,
    "sr_id_archivo_09": str
})
params.validate()
conf = ConfManager()
#query = QueryManager()
db = DBXConnectionManager()


# COMMAND ----------

Notify.send_notification("INFO", params)

# COMMAND ----------

sr_folio = params.sr_folio
sr_subproceso = params.sr_subproceso
query_str = f"""
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE table_name LIKE '%_arc_acep_%'
OR table_name LIKE '%_ARC_REC_%'
OR table_name LIKE '%DELTA_0200_INS%'
OR table_name LIKE '%DELTA_0500_INS%'
OR table_name LIKE '%DELTA_0200_ARCH_ACEP_ENCAB_AG_UG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0300_ARCH_ACEP_DETA_AG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_0400_ARCH_ACEP_SUM_AG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_0200_ARCH_ACEP_ENCAB_TA_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0300_ARCH_ACEP_DETA_TA_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_0400_ARCH_ACEP_SUM_TA_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_0300_ARCH_ACEP_DETA_UG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_0400_ARCH_ACEP_SUM_UG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_0400_ARCH_ACEP_SUM_UG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_0200_ARCH_RECH_ENCAB_TA_AG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0300_ARCH_RECH_DETA_AG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0300_ARCH_RECH_DETA_TA_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_0400_ARCH_RECH_SUM_AG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0400_ARCH_RECH_SUM_TA_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0200_ARCH_RECH_ENCAB_UG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0300_ARCH_RECH_DETA_TA_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0300_ARCH_RECH_DETA_UG_{sr_subproceso}_{sr_folio}%'
OR table_name LIKE '%DELTA_GAR_0100_ENCABEZADO%'
OR table_name LIKE '%DELTA_GAR_0200_DETALLE%'
OR table_name LIKE '%DELTA_GAR_0300_SUMARIO%'
OR table_name LIKE '%_DPSJL_%'
"""
tablas_delta = spark.sql(query_str)
display(tablas_delta)

# COMMAND ----------

# Itera sobre las filas del DataFrame que contiene las tablas encontradas
for row in tablas_delta.collect():
    table_name = row['table_name']
    try:
        # Elimina la tabla Delta usando el método personalizado
        db.drop_delta(table_name)
    except Exception as e:
        # Captura y muestra cualquier error ocurrido durante la eliminación
        print(f"Error al eliminar la tabla {table_name}: {str(e)}")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
