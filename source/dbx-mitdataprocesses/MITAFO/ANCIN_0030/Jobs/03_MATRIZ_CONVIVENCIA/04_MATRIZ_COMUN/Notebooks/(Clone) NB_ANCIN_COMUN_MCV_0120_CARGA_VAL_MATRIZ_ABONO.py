# Databricks notebook source
"""
Descripcion:
   120 Carga tabla de VAL_MATRIZ para el abono
    lee la informacion de PRE MATRIZ y la ccomplementa con la delta de no convivencia e inserta en ETL_VAL_MATRIZ
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
Tablas input:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
    DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}        
Tablas output:
    CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV
Tablas Delta:
    N/A
Archivos SQL:
   
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

## Query extrae datos de prematriz sumarizados 
#   Informacion de Balance Movs agrupada por Cuenta individual, tipo sub cuenta y siefore
#   cuando la sumatoria de FTN_DISP_ACCIONES sea >=0 del universo de cuentas individuales de Pre Matriz

statement = query.get_statement(
    "COMUN_MCV_120_ABONO_DB_100_EXT_INFO_SUM_PRE_MATRIZ.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_COMUN_MCV_DELTA_05_SUM_PRE_MATRIZ_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_ABONO_01_SUM_PRE_MATRIZ_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_ABONO_01_SUM_PRE_MATRIZ_{params.sr_folio}"))
#  -------------------------------------------------------------------------------------


# COMMAND ----------

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}"))

# COMMAND ----------

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

# Query Extrae informacion de los resultados sumarizados de PRE MATRIZ y valida con el resultado de los que no conviven (NO_CONVIV)
#   120 MATRIZ CARGO
# 
statement = query.get_statement(
    "COMUN_MCV_120_ABONO_TD_02_GEN_INFO_PRE_VAL_MATRIZ.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_ABONO_01_SUM_PRE_MATRIZ_{params.sr_folio}",
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_06_NO_CONVIV_RD_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_ABONO_02_PRE_VAL_MATRIZ_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_ABONO_02_PRE_VAL_MATRIZ_{params.sr_folio}"))

# COMMAND ----------

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

#INSERTA EN VAL_MATRIZ_AUX
# 

table_name = "CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_ABONO_02_PRE_VAL_MATRIZ_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge de VAL_MATRIZ_AUX Y VAL_MATRIZ

# COMMAND ----------

# REALIZA MERGE VAL_MATRIZ_AUX Y VAL_MATRIZ
# llave FTC_FOLIO, FCN_ID_PROCESO, FCN_ID_SUBPROCESO, FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA
statement = query.get_statement(
    "COMUN_MCV_120_TD_04_DB_MERGE_VAL_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,ELIMINA LA TABLA AUXILIAR DESPUES DEL MERGE
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

CleanUpManager.cleanup_notebook(locals())
