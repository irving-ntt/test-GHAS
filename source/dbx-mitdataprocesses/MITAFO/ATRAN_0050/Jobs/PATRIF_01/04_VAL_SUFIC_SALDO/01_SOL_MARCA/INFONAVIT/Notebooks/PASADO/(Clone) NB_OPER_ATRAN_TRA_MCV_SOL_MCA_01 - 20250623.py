# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta, finalmente inserta en OCI el resultado
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    354 - Solicitud de marca de cuentas por 43 bis
Tablas input:
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
    PROCESOS.TTSISGRAL_SUF_SALDOS
    CIERREN.TCAFOGRAL_VALOR_ACCION
    CIERREN.TRAFOGRAL_MOV_SUBCTA
    CIERREN.TFAFOGRAL_CONFIG_SUBPROCESO
Tablas output:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
Tablas Delta:
    DELTA_TRA_MCV_SOL_MCA_01_{params.sr_folio}
    
Archivos SQL:
    TRA_MCV_SOL_MARCA_0100_EXT_INFO.sql
    TRA_MCV_SOL_MARCA_0200_DEL_PRE_MATRIZ.sql
  

"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_tipo_mov": str,
    "sr_conv_ingty": str,
    "sr_path_arch": str,
    "sr_tipo_ejecucion": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_DB_100_MARCA_DESMARCA_INFO.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_100_MARCA_DESMARCA_INFO_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_100_MARCA_DESMARCA_INFO_{params.sr_folio}"))

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_DB_402_VALOR_ACCION.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_402_VALOR_ACCION_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_402_VALOR_ACCION_{params.sr_folio}"))

# COMMAND ----------

DELTA_TABLE_DES_INF = "DELTA_TRA_DB_100_MARCA_DESMARCA_INFO_" + params.sr_folio
DELTA_TABLE_VALOR_ACCION = "DELTA_TRA_DB_402_VALOR_ACCION_" + params.sr_folio


# COMMAND ----------

#Ejecuta consulta para el 01
statement_001 = query.get_statement(
    "TRA_DELTA_DES_INF_VAL_ACC.sql",
    DELTA_TABLE_DES_INF=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DES_INF}",
    DELTA_TABLE_VALOR_ACCION=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_VALOR_ACCION}",
   
   # SR_SUBPROCESO=params.sr_subproceso
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

display(df)

# COMMAND ----------

#Ejecuta consulta para el 01
statement_002 = query.get_statement(
    "TRA_DELTA_DES_INF_SALDO_MENOR.sql",
    DELTA_TABLE_DES_INF=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DES_INF}",
    DELTA_TABLE_VALOR_ACCION=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_VALOR_ACCION}",
   
   # SR_SUBPROCESO=params.sr_subproceso
)

#Ejecuta la consulta sobre la delta
df_2 = db.sql_delta(query=statement_002)

display(df_2)

# COMMAND ----------

#query de join :


