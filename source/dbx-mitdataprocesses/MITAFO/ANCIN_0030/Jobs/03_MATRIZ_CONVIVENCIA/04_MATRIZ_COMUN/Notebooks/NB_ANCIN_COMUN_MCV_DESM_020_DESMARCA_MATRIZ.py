# Databricks notebook source
""" 
Descripcion:
    NB_ANCIN_COMUN_MCV_DESM_020_DESMARCA_MATRIZ
    ACTUALIZA MATRIZ DE CONVIVENCIA (DESMARCA) CON LOS DATOS ALMACENADOS EN LA TABLA DELTA TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01{params.sr_folio}
Subetapa: 
    POR DEFINIR
Trámite:
    COMUN - 
Tablas input:
    TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01{params.sr_folio}        
Tablas output:
    TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
Tablas Delta:
    N/A
Archivos SQL:
   
    """

# COMMAND ----------

# DBTITLE 1,Cargar Framework 🚀
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Definir y validar parametros
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_bandera": str,
    "sr_path_arch": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

# DBTITLE 1,Cargar configuraciones locales
conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,PREPARA INFORMACION PARA MATRIZ AUX
# Extrae unicamente DATOS para el update a Matriz de Convivencia en Datastage es solo por el ID, verificar
# Datos almacenados en TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01 
statement_desmc_matriz_conv = query.get_statement(
    "ATRAN_DESM_020_0100_INFO_PARA_DESMARCA_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}",
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}", db.sql_delta(statement_desmc_matriz_conv), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}"))


# COMMAND ----------

# DBTITLE 1,ELIMINA REGISTROS POR FOLIO EN MATRIZ AUX
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

# DBTITLE 1,INSERTA EN MATRIZ AUX
#INSERTA EN MATRIZ_AUX
table_name = "CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# DBTITLE 1,MERGE CON UPDATE PARA DESMARCAR
# REALIZA MERGE/UPDATE 
statement = query.get_statement(
    "ATRAN_DESM_020_0200_MERGE_UPD_MATRIZ_CONVIVENCIA.sql",
    SR_FOLIO = params.sr_folio,
    SR_USUARIO = params.sr_usuario,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)


# COMMAND ----------

# DBTITLE 1,DESPUES DEL MERGE (UPDATE) ELIMINA EN MATRIZ AUX POR FOLIO
# ELIMINA REGISTROS DEL FOLIO EN MATRIZ_AUX PARA DEJAR LA TABLA VACIA
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
# MAGIC ## el siguiente paso es la generacion del archivo 69 en otro notebook
