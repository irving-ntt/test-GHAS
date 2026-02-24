# Databricks notebook source
# DBTITLE 1,VERSION  DEV
"""
Descripcion:
    VAL_SALDO
    Lee la información de tablas delta, aplica las transformaciones y reglas necesarias de la validacion de saldo y guarda la información resultante en la tabla delta de no convivencia.
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
Tablas input:
    CIERREN.TTAFOGRAL_BALANCE_MOVS
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
    CIERREN.TCAFOGRAL_VALOR_ACCION        
Tablas output:
    N/A 
Tablas Delta:
    TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_04_BAL_MOVS_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_03_VAL_CARGO_{params.sr_folio}
Archivos SQL:
    COMUN_MCV_115_DB_100_EXT_INFO_BALANCE_MOVS.sql
    """

# COMMAND ----------

# DBTITLE 1,CARGA FRAMEWORK
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,VALIDA PARAMETROS
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

# DBTITLE 1,INFORMACION DE BALANCE  MOVS
##Query Extrae informacion de los resultados de BALANCE MOVS a DELTA_COMUN_MCV_DELTA_04_BAL_MOVS_
#   Informacion de Balance Movs agrupada por Cuenta individual, tipo sub cuenta y siefore
#   cuando la sumatoria de FTN_DISP_ACCIONES sea >=0 del universo de cuentas individuales de Pre Matriz

statement = query.get_statement(
    "COMUN_MCV_115_DB_100_EXT_INFO_BALANCE_MOVS.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_04_BAL_MOVS_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_04_BAL_MOVS_{params.sr_folio}"))
#  -------------------------------------------------------------------------------------


# COMMAND ----------

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}"))

# COMMAND ----------

# Query Extrae informacion de los resultados de VAL_SUF y los valida con la el monto de balance movs en caso de no cumplir las condiciones se clasifica como NO CONVIVENCIA (APPEND) a VAL_NO_CONVIV  --> TD_200_02_NO_CONVIV APPEND
#   115 VAL SALDO
# 
statement = query.get_statement(
    "COMUN_MCV_115_TD_01_APP_DELTA_VAL_SUF_A_NO_CONVIV.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}",
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_04_BAL_MOVS_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}", db.sql_delta(statement), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}"))
