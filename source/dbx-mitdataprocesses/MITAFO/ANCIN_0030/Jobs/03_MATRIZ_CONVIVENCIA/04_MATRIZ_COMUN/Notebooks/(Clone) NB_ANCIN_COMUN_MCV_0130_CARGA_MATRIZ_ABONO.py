# Databricks notebook source
"""
Descripcion:
    130 MATRIZ_CARGO
    lee la informacion de VAL MATRIZ y la inserta en MATRIZ
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
Tablas input:
    CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV  
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ 
    CIERREN.TRAFOGRAL_MOV_SUBCTA
    DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}        
Tablas output:
    TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
Tablas Delta:
    TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}
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

# MAGIC %md
# MAGIC ## Genera Delta previo para carga en MATRIZ_AUX

# COMMAND ----------

## Query extrae datos de val matriz y prepara el previo para la carga/actualizacion de MATRIZ
#  
statement = query.get_statement(
    "COMUN_MCV_130_TD_05_EXT_INFO_A_DELTA_MATRIZ.sql",
    sr_folio=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}",
)

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}"))
#  -------------------------------------------------------------------------------------


# COMMAND ----------

# MAGIC %md
# MAGIC ## Elimina registros en tabla auxiliar de matriz

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN MATRIZ_AUX
statement = query.get_statement(
    "COMUN_MCV_130_TD_06_DEL_MATRIZ_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inserta en MATRIZ_AUX

# COMMAND ----------

#INSERTA EN MATRIZ_AUX

table_name = "CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_07_PRE_VAL_MATRIZ_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge de MATRIZ_AUX y MATRIZ

# COMMAND ----------

# REALIZA MERGE MATRIZ_AUX Y MATRIZ
statement = query.get_statement(
    "COMUN_MCV_130_TD_07_DB_MERGE_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)
