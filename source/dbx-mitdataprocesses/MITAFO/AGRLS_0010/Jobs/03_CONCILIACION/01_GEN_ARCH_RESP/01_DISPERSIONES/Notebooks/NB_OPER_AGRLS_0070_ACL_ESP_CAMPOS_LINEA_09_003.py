# Databricks notebook source
'''
Descripcion:
    Inserción de los registros rechazados y aceptados hacia la tabla CIFRAS_CTRL_RESP
Subetapa:
    20 - ARCHIVO RESPUESTA
Trámite:
    118 - DAEIMSS
    120 - DOISSSTE
    122 - DITISSSTE
Tablas INPUT:
    N/A
Tablas OUTPUT:
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP
Tablas INPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_500_#SR_ID_ARCHIVO#
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    ACLARACIONES_ESPECIALES_011.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio" : str,
    "sr_id_archivo": str,
    "sr_fec_arc" : str,
    "sr_fec_liq" : str,
    "sr_dt_org_arc" : str,
    "sr_origen_arc" : str,
    "sr_tipo_layout": str,
    "sr_tipo_reporte" : str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_etapa": str,
    "sr_id_snapshot": str
})
# Validar widgets
params.validate()

# COMMAND ----------

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

file_manager = FileManager(err_repo_path=conf.err_repo_path)

# COMMAND ----------

DELTA_TABLE_500 = "DELTA_ACLARACIONES_ESPECIALES_500_" + params.sr_id_archivo

schema = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}"

# COMMAND ----------

# DBTITLE 1,VALIDACION 01
#Ejecuta consulta para el 01
statement_001 = query.get_statement(
    "ACLARACIONES_ESPECIALES_011.sql",
    DELTA_ACL_ESP_500=f"{schema}.{DELTA_TABLE_500}",
    SR_PROCESO = params.sr_proceso,
    SR_SUBPROCESO = params.sr_subproceso,
    CRE_USUARIO = SETTINGS.GENERAL.PROCESS_USER
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

# COMMAND ----------

if conf.debug:
    display(df)

# COMMAND ----------

table_001 = "CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP"
statement_001 = f"""
SELECT FTC_FOLIO, FCN_ID_SUBPROCESO, FLD_FEC_REG
FROM {table_001} WHERE FTC_FOLIO = {params.sr_folio} AND FCN_ID_SUBPROCESO = {params.sr_subproceso}"""

existing_records = db.read_data("default", statement_001)

# COMMAND ----------

if conf.debug:
    display(existing_records)

# COMMAND ----------

existing_records = df.join(existing_records, 
                           on=["FTC_FOLIO", "FCN_ID_SUBPROCESO"], 
                           how="inner").select(df["*"])

# COMMAND ----------

if existing_records.collect():  # Verifica si hay registros
    delete_statement = f"""
    DELETE FROM {table_001} 
    WHERE FTC_FOLIO = {params.sr_folio} 
    AND FCN_ID_SUBPROCESO = {params.sr_subproceso}
    """
    execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

# for row in existing_records.collect():
#     delete_statement = f"""
#     DELETE FROM {table_001} 
#     WHERE FTC_FOLIO = '{row['FTC_FOLIO']}' 
#     AND FCN_ID_SUBPROCESO = '{row['FCN_ID_SUBPROCESO']}'
#     """
#     execution = db.execute_oci_dml(
#         statement=delete_statement, async_mode=False
#     )

# COMMAND ----------

# Escribir solo los nuevos registros en la tabla
db.write_data(df, f"{table_001}", "default", "append")
