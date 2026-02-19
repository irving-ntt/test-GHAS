# Databricks notebook source
# MAGIC %md
# MAGIC NB_ANCIN_COMUN_MCV_0155_RESPUESTA_INTEGRITY 20251013

# COMMAND ----------

""" 
Descripcion:
     desmarca y actualiza los registros que no convivieron en Integrity.
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
Tablas input:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
    DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}        
Tablas output:
    TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
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

# 🚀 **Ejemplo de Uso**
conf = ConfManager()
#Archivos SQL
query = QueryManager()
#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Delta para Actualizar Val Matriz  y Matriz
# Query para extraer informacion de THAFOGRAL_MAR_DEs_ITGY y MAP_NIC_ITGY
statement_mdi_pro = query.get_statement(
    "COMUN_MCV_155_DB_100_EXT_INFO_MAR_DES_ITGY.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_THAFOGRAL_MAR_DES_ITGY_{params.sr_folio}", db.read_data("default", statement_mdi_pro), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_16_THAFOGRAL_MAR_DES_ITGY_{params.sr_folio}"))

# COMMAND ----------

# Query para actualizar VAL_MATRIZ con los datos ya guardadoS de la op65 en TEMP_DELTA_COMUN_MCV_DELTA_16_THAFOGRAL_MAR_DES_ITGY_
statement_R65_val_matriz = query.get_statement(
    "COMUN_MCV_155_TD_01_INFO_PARA_UPD_VAL_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_16_THAFOGRAL_MAR_DES_ITGY_{params.sr_folio}",
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_17_VAL_MATRIZ_UPD_{params.sr_folio}", db.sql_delta(statement_R65_val_matriz), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_17_VAL_MATRIZ_UPD_{params.sr_folio}"))


# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN VAL_MATRIZ_AUX
statement = query.get_statement(
    "COMUN_MCV_120_TD_03_DEL_VAL_MATRIZ_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) INDEX(TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

#INSERTA EN VAL_MATRIZ_AUX
table_name = "CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_17_VAL_MATRIZ_UPD_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# DBTITLE 1,UPDATE USANDO MERGE A VAL MATRIZ

# -----------------------------------------------------------------
# REALIZA MERGE/UPDATE TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX Y TLSISGRAL_ETL_VAL_MATRIZ_CONV
statement = query.get_statement(
    "COMUN_MCV_155_TD_04_DB_MERGE_VAL_MATRIZ_UPD.sql",
    SR_USUARIO = params.sr_usuario,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) INDEX(TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)



# COMMAND ----------

# DBTITLE 1,Elimina registros de tabla aux por si hay reprocesos
# ELIMINA REGISTROS DEL FOLIO EN VAL_MATRIZ_AUX PARA DEJAR LA TABLA VACIA
statement = query.get_statement(
    "COMUN_MCV_120_TD_03_DEL_VAL_MATRIZ_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) INDEX(TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## UPDATE A MATRIZ

# COMMAND ----------

# Query para ctualizar MATRIZ con los datos ya guardadoS de la op65 en TEMP_DELTA_COMUN_MCV_DELTA_16_THAFOGRAL_MAR_DES_ITGY_
statement_R65_matriz_conv = query.get_statement(
    "COMUN_MCV_155_TD_02_INFO_PARA_UPD_MATRIZ_CONV.sql",
    SR_FOLIO=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_16_THAFOGRAL_MAR_DES_ITGY_{params.sr_folio}",
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_18_MATRIZ_CONV_UPD_{params.sr_folio}", db.sql_delta(statement_R65_matriz_conv), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_18_MATRIZ_CONV_UPD_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN MATRIZ_AUX
statement = query.get_statement(
    "COMUN_MCV_130_TD_06_DEL_MATRIZ_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) INDEX(TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

#INSERTA EN MATRIZ_AUX
table_name = "CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_18_MATRIZ_CONV_UPD_{params.sr_folio}"), table_name, "default", "append")


# COMMAND ----------

# DBTITLE 1,UPDATE A MATRIZ


# REALIZA MERGE/UPDATE TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX Y TLSISGRAL_ETL_VAL_MATRIZ_CONV
statement = query.get_statement(
    "COMUN_MCV_155_TD_05_DB_MERGE_MATRIZ_CONV_UPD.sql",
    SR_FOLIO=params.sr_folio,
    SR_USUARIO = params.sr_usuario,
    hints="/*+ PARALLEL(8) INDEX(TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# ELIMINA REGISTROS DEL FOLIO EN MATRIZ_AUX PARA DEJAR LA TABLA VACIA
statement = query.get_statement(
    "COMUN_MCV_130_TD_06_DEL_MATRIZ_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) INDEX(TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
