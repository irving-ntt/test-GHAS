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
    TEMP_DELTA_COMUN_MCV_01_PRE_MAT_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_02_PRE_MAT_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_03_CONF_CONV_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_01_{params.sr_folio}
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

#Query Extrae informacion de pre matriz por el folio o el folio rel

statement = query.get_statement(
    "COMUN_MCV_100_DB_0100_EXT_INFO_ETL_PRE_MATRIZ.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_01_PRE_MAT_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_01_PRE_MAT_{params.sr_folio}"))

# COMMAND ----------

#Query Extrae informacion de matriz 

statement = query.get_statement(
    "COMUN_MCV_100_DB_0200_EXT_INFO_MATRIZ_CON_MARCA.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_02_PRE_MAT_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_02_PRE_MAT_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Extraccion Config Conviv
#Query Extrae informacion de config conviv 

statement = query.get_statement(
    "COMUN_MCV_100_DB_0300_EXT_INFO_CONFIG_CONVIV.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_03_CONF_CONV_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_03_CONF_CONV_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query que une las tablas delta 01 y 02 del comun de matriz 
# MAGIC --  
# MAGIC SELECT 
# MAGIC    -- D.* 
# MAGIC          D.FTN_NUM_CTA_INVDUAL
# MAGIC         ,D.FTN_ID_PROCESO     
# MAGIC         ,D.FTN_ID_SUBPROCESO  
# MAGIC         ,D.FTN_ID_SIEFORE     
# MAGIC         ,D.FTN_ID_TIPO_SUBCTA 
# MAGIC         ,D.FTN_ID_TIPO_MONTO  
# MAGIC         ,D.FTN_ID_SALDO_OPERA 
# MAGIC         ,D.FTN_ID_TIPO_MOV    
# MAGIC         ,D.FTN_MONTO_ACCIONES 
# MAGIC         ,D.FTN_MONTO_PESOS  
# MAGIC         ,D.FCN_ID_PROCESO_CON    
# MAGIC         ,D.FCN_ID_SUBPROCESO_CON 
# MAGIC         ,D.FCN_ID_TIPO_SUBCTA_CON
# MAGIC         ,D.FCN_ID_TIPO_MONTO_CON 
# MAGIC         ,D.FTD_FECHA_BLOQ        
# MAGIC         ,D.B_MATRIZ      
# MAGIC         ,CC.FFB_CONVIVENCIA       
# MAGIC         ,CC.B_CONVIV             
# MAGIC FROM  (
# MAGIC     SELECT 
# MAGIC         PRE.FTN_NUM_CTA_INVDUAL
# MAGIC         ,PRE.FTN_ID_PROCESO     
# MAGIC         ,PRE.FTN_ID_SUBPROCESO  
# MAGIC         ,PRE.FTN_ID_SIEFORE     
# MAGIC         ,PRE.FTN_ID_TIPO_SUBCTA 
# MAGIC         ,PRE.FTN_ID_TIPO_MONTO  
# MAGIC         ,PRE.FTN_ID_SALDO_OPERA 
# MAGIC         ,PRE.FTN_ID_TIPO_MOV    
# MAGIC         ,PRE.FTN_MONTO_ACCIONES 
# MAGIC         ,PRE.FTN_MONTO_PESOS 
# MAGIC     --   FTN_NUM_CTA_INVDUAL   
# MAGIC         ,M.FCN_ID_PROCESO_CON    
# MAGIC         ,M.FCN_ID_SUBPROCESO_CON 
# MAGIC         ,M.FCN_ID_TIPO_SUBCTA_CON
# MAGIC         ,M.FCN_ID_TIPO_MONTO_CON 
# MAGIC         ,M.FTD_FECHA_BLOQ        
# MAGIC         ,M.B_MATRIZ                 
# MAGIC     from dbx_mit_dev_1udbvf_workspace.default.TEMP_DELTA_COMUN_MCV_01_PRE_MAT_201906251956390001  PRE
# MAGIC     LEFT JOIN dbx_mit_dev_1udbvf_workspace.default.TEMP_DELTA_COMUN_MCV_02_PRE_MAT_201906251956390001 M 
# MAGIC     ON PRE.FTN_NUM_CTA_INVDUAL = M.FTN_NUM_CTA_INVDUAL 
# MAGIC ) D
# MAGIC LEFT JOIN dbx_mit_dev_1udbvf_workspace.default.TEMP_DELTA_COMUN_MCV_03_CONF_CONV_201906251956390001 CC
# MAGIC ON  D.FCN_ID_PROCESO_CON = CC.FCN_ID_PROCESO_CON
# MAGIC AND D.FCN_ID_SUBPROCESO_CON = CC.FCN_ID_SUBPROCESO_CON
# MAGIC AND D.FTN_ID_SUBPROCESO = CC.FTN_ID_SUBPROCESO

# COMMAND ----------

# DBTITLE 1,GENERA DELTA 01
#Query 1 para generar la tabla delta primaria 1 

statement = query.get_statement(
    "COMUN_MCV_100_TD_0100_GEN_MATRIZ_DELTA_01.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_01_PRE_MAT_{params.sr_folio}",
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_02_PRE_MAT_{params.sr_folio}",
    DELTA_TABLA_NAME3 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_03_CONF_CONV_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_{params.sr_folio}"))



# COMMAND ----------

# DBTITLE 1,GENERA DELTA VALIDA SUFICIENCIA
#Query  para generar la tabla delta DS100_VAL_SUF DATASET 01 --> TD_100_01_VAL_SUF 
#   (D.B_MATRIZ <> 1 AND D.FTN_ID_TIPO_MOV = 180)  OR  (D.FFB_CONVIVENCIA  IS NULL )

statement = query.get_statement(
    "COMUN_MCV_100_TD_0200_GEN_MATRIZ_DELTA_01_VAL_SUF.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_01_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,GENERA DELTA NO CONVIVE
# Query  para generar la tabla delta D200_VAL_SUF DATASET 02 --> TD_200_02_NO_CONVIV 
# D.B_MATRIZ = 1 AND D.FFB_CONVIVENCIA = '0'

statement = query.get_statement(
    "COMUN_MCV_100_TD_0300_GEN_MATRIZ_DELTA_02_NO_CONVIV.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_01_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,GENERA DELTA VALIDA CARGO
# Query  para generar la tabla delta D400_VAL_CARGO DATASET 02 --> TD_400_03_VAL_CARGO 
# D.B_MATRIZ = 1 AND D.FTN_ID_TIPO_MOV = '180'

statement = query.get_statement(
    "COMUN_MCV_100_TD_0400_GEN_MATRIZ_DELTA_03_VAL_CARGO.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_01_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_03_VAL_CARGO_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_03_VAL_CARGO_{params.sr_folio}"))
