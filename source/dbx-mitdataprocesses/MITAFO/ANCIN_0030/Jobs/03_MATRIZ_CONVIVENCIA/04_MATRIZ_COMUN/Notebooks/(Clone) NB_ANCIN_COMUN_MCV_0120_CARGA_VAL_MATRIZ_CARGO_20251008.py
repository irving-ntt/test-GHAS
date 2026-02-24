# Databricks notebook source
# DBTITLE 1,DOCUMENTACION
"""
Descripcion:
   120 MATRIZ_CARGO
    Validacion de informacion previa, sumariza por cuenta y valida con cuentas previamente marcadas, inserta los resultados en ETL_VAL_MATRIZ
Subetapa: 
    25 - Matriz de Convivencia
Trámite:
    COMUN - 
Tablas input:
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ    
Tablas output:
    CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX
Tablas Delta:
    TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_05_SUM_PRE_MATRIZ_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_06_NO_CONVIV_RD_{params.sr_folio} 
    TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}
Archivos SQL:
    COMUN_MCV_120_DB_100_EXT_INFO_SUM_PRE_MATRIZ.sql
    COMUN_MCV_120_TD_01_REMOVE_DUP.sql
    COMUN_MCV_120_TD_02_GEN_INFO_PRE_VAL_MATRIZ.sql
    COMUN_MCV_120_TD_03_DEL_VAL_MATRIZ_AUX.sql
    COMUN_MCV_120_TD_04_DB_MERGE_VAL_MATRIZ.sql

   
    """

# COMMAND ----------

# DBTITLE 1,FRAMEWORK
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,VALIDACION PARAMETROS
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

# DBTITLE 1,EXTRACCION DE DATOS SUMARIZADOS
## Query extrae datos de prematriz sumarizados 
#   Informacion de Balance Movs agrupada por Cuenta individual, tipo sub cuenta y siefore
#   cuando la sumatoria de FTN_DISP_ACCIONES sea >=0 del universo de cuentas individuales de Pre Matriz

statement = query.get_statement(
    "COMUN_MCV_120_DB_100_EXT_INFO_SUM_PRE_MATRIZ.sql",
    sr_folio=params.sr_folio,
    sr_tipo_mov=params.sr_tipo_mov,
 #   DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_05_SUM_PRE_MATRIZ_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_05_SUM_PRE_MATRIZ_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_05_SUM_PRE_MATRIZ_{params.sr_folio}"))
#  -------------------------------------------------------------------------------------


# COMMAND ----------

# DBTITLE 1,VALIDAMOS RESULTADOS NO CONVIV
if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,REMUEVE DUPLICADOS NO CONVIV
# Query Remueve los duplicados de los que no conviven (NO_CONVIV) 
#   120 MATRIZ CARGO
# 
statement = query.get_statement(
    "COMUN_MCV_120_TD_01_REMOVE_DUP.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_06_NO_CONVIV_RD_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_06_NO_CONVIV_RD_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,VERIFICA PRE MATRIZ Y NO CONVIV
# Query Extrae informacion de los resultados sumarizados de PRE MATRIZ y valida con el resultado de los que no conviven (NO_CONVIV)
#   120 MATRIZ CARGO
# 
statement = query.get_statement(
    "COMUN_MCV_120_TD_02_GEN_INFO_PRE_VAL_MATRIZ.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_05_SUM_PRE_MATRIZ_{params.sr_folio}",
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_06_NO_CONVIV_RD_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,ELIMINA REGISTROS DEL FOLIO EN VAL_MATRIZ_AUX
# ELIMINA REGISTROS DEL FOLIO EN VAL_MATRIZ_AUX
statement = query.get_statement(
    "COMUN_MCV_120_TD_03_DEL_VAL_MATRIZ_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,INSERTA EN VAL_MATRIZ_AUX
#INSERTA EN VAL_MATRIZ_AUX

table_name = "CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}"), table_name, "default", "append")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge de VAL_MATRIZ_AUX Y VAL_MATRIZ

# COMMAND ----------

# REALIZA MERGE VAL_MATRIZ_AUX Y VAL_MATRIZ
statement = query.get_statement(
    "COMUN_MCV_120_TD_04_DB_MERGE_VAL_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
