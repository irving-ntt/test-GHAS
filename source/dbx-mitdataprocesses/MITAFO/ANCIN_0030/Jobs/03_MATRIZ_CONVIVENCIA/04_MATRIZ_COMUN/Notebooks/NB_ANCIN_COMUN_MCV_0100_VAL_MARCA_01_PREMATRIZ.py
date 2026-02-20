# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
    Tablas input:
    CIERREN.TFAFOGRAL_CONFIG_CONVIV
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
    CIERREN.TTCRXGRAL_FOLIO
    CIERREN.TRAFOGRAL_MOV_SUBCTA
    CIERREN.TFAFOGRAL_CONFIG_CONCEP_MOV
    CIERREN.TFAFOGRAL_CONFIG_SUBPROCESO
Tablas output:
    N/A 
Tablas Delta:
    TEMP_DELTA_COMUN_MCV_DELTA_01_PRE_MAT_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_02_PRE_MAT_MATRIZ_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_03_CONFIG_CONVIV_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_01_{params.sr_folio}   --> delta PRINCIPAL
    TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_03_VAL_CARGO_{params.sr_folio}

Archivos SQL:
    COMUN_MCV_100_DB_0200_EXT_INFO_MATRIZ_CON_MARCA.sql
    COMUN_MCV_100_DB_0300_EXT_INFO_CONFIG_CONVIV.sql
    COMUN_MCV_100_TD_0100_GEN_MATRIZ_DELTA_01.sql
    COMUN_MCV_100_TD_0200_GEN_MATRIZ_DELTA_01_VAL_SUF.sql
    COMUN_MCV_100_TD_0300_GEN_MATRIZ_DELTA_02_NO_CONVIV.sql
    COMUN_MCV_100_TD_0400_GEN_MATRIZ_DELTA_03_VAL_CARGO.sql
    COMUN_MCV_110_TD_01_APP_DELTA_VAL_CARGO_A_VAL_SUF.sql

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

# DBTITLE 1,EXTRAE INFO DE PRE MATRIZ
#Query Extrae informacion de pre matriz por el folio o el folio rel
# Stage DB_100_TTSISGRAL_ETL_PRE_MATRIZ 

statement = query.get_statement(
    "COMUN_MCV_100_DB_0100_EXT_INFO_ETL_PRE_MATRIZ_02.sql",
    sr_folio=params.sr_folio,
    sr_tipo_mov=params.sr_tipo_mov
)

# db.write_delta(f"TEMP_DELTA_COMUN_MCV_01_PRE_MAT_{params.sr_folio}", db.read_data("default", statement), "overwrite")
db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_PRE_MAT_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_PRE_MAT_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,BUSCAMOS AL MENOS  1 REGISTRO
statement_cnt = query.get_statement(
    "COMUN_MCV_100_TD_0100_CUANTOS_DELTA_DELTA_PRE_MATRIZ.sql",
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_01_PRE_MAT_{params.sr_folio}",
)


# COMMAND ----------

# DBTITLE 1,VERIFICAMOS CON PYTHON
var_datos_a_validar = None
rows = spark.sql(statement_cnt).take(1)   # devuelve [] o [Row(...)]
if rows:
    row = rows[0]
    # ejemplo: acceder a la columna TOTAL
    total = row["FTN_NUM_CTA_INVDUAL"] if "FTN_NUM_CTA_INVDUAL" in row.asDict() else None
    var_datos_a_validar = "CON_DATOS_A_VALIDAR"
    print("Hay datos. FTN_NUM_CTA_INVDUAL =", total, " ", var_datos_a_validar)
else:
    row = None
    total = None
    var_datos_a_validar = "SIN_DATOS_A_VALIDAR"
    print("No hay registros.")

# COMMAND ----------

# DBTITLE 1,EXTRAE TOTAL EN BASE A UN SQL COUNT
# SE COMENTA PORQUE EL COUNT(*) ES MAS COSTOSO QUE EXTRAER 1 REGISTRO DE LA TABLA DELTA
# SIRVE CON LA PRIMER PARTE DEL SQL 

#df_from_delta=db.sql_delta(statement_cnt)

#if conf.debug:
#    display(df_from_delta)

#rows = df_from_delta.select("TOTAL").take(1)   # devuelve [] o [Row(TOTAL=X)]
#if rows:
#    total = int(rows[0]["TOTAL"])              # o rows[0][0]
#else:
#    total = None

#print(total)  # 7 o None

# COMMAND ----------

# DBTITLE 1,VERIFICAMOS CON DATAFRAME
# sql_delta --> para consultar tablas delta con un query
# read_delta --> para hacerle un select * from a una tabla delta sin necesidad de usar query
var_existen_datos = None
df_from_delta=db.sql_delta(statement_cnt)

if conf.debug:
    display(df_from_delta)

rows = df_from_delta.select("FTN_NUM_CTA_INVDUAL", "FTN_ID_TIPO_MOV").limit(1).collect()
if rows:
    first = rows[0]
    print("Existe fila:", first["FTN_NUM_CTA_INVDUAL"], first["FTN_ID_TIPO_MOV"],  " ", var_datos_a_validar)
else:
    print("DataFrame vacío")

# COMMAND ----------

dbutils.jobs.taskValues.set(key='var_datos_a_validar', value=var_datos_a_validar)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
