# Databricks notebook source
"""
Descripcion:
    Lee la información de tablas delta, aplica las transformaciones y reglas necesarias y guarda la información intermedia en tablas delta
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
Tablas input:
    N/A   
Tablas output:
    N/A 
Tablas Delta:
    DELTA_COMUN_MCV_DELTA_01_VAL_CARGO_{params.sr_folio} (input)
    DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}
    DELTA_COMUN_MCV_DELTA_03_VAL_SUF_{params.sr_folio}
Archivos SQL:
    TEMP_DELTA_COMUN_MCV_DELTA_03_VAL_CARGO_{params.sr_folio}
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
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
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

# DBTITLE 1,APPEND A DELTA VAL_SUF
# #Query Extrae informacion de los resultados de VAL_CARGO y los clasifica (APPEND) a VAL_SUF  
#   Regla --> TD_100_01_VAL_SUF APPEND
#   D.FTN_ID_TIPO_SUBCTA <> FCN_ID_TIPO_SUBCTA_CON
#   O (D.FTN_ID_TIPO_SUBCTA = FCN_ID_TIPO_SUBCTA_CON) Y sean validas cualquiera de las siguientes condiciones (AND)
#   (D.FCN_ID_TIPO_MONTO_CON = 185  AND  D.FTN_ID_TIPO_MONTO = 185) 
#   OR (D.FCN_ID_TIPO_MONTO_CON = 199  AND  D.FTN_ID_TIPO_MONTO = 199) 
#   OR (D.FCN_ID_TIPO_MONTO_CON = 200  AND  D.FTN_ID_TIPO_MONTO = 200)

statement = query.get_statement(
    "COMUN_MCV_110_TD_01_APP_DELTA_VAL_CARGO_A_VAL_SUF.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_03_VAL_CARGO_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}", db.sql_delta(statement), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,APPEND A DELTA NO_CONVIV
##Query Extrae informacion de los resultados de VAL_CARGO y los clasifica (APPEND) a VAL_NO_CONVIV  --> TD_200_02_NO_CONVIV APPEND
#   
#   (D.FTN_ID_TIPO_SUBCTA = FCN_ID_TIPO_SUBCTA_CON) Y NO sean validas cualquiera de las siguientes condiciones (AND)
#   (D.FCN_ID_TIPO_MONTO_CON = 185  AND  D.FTN_ID_TIPO_MONTO = 185) 
#   OR (D.FCN_ID_TIPO_MONTO_CON = 199  AND  D.FTN_ID_TIPO_MONTO = 199) 
#   OR (D.FCN_ID_TIPO_MONTO_CON = 200  AND  D.FTN_ID_TIPO_MONTO = 200)

statement = query.get_statement(
    "COMUN_MCV_110_TD_02_APP_DELTA_VAL_CARGO_A_NO_CONVIV.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_03_VAL_CARGO_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}", db.sql_delta(statement), "append")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}"))

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
