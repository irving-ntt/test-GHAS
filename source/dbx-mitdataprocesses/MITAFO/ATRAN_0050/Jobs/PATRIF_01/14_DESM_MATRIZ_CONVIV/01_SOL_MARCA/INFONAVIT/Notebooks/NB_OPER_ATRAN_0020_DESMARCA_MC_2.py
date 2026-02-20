# Databricks notebook source
'''
Descripcion:
    Desmarca Matriz de Convivencia
Subetapa:
    Desmarca Matriz de Convivencia
Tramite:
    
Tablas Input:
    TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01{params.sr_folio}
Tablas Output:
    TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
Tablas DELTA:
    DELTA_DESMARCA_{params.sr_folio}

Archivos SQL:
    0010_DESMARCA.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_path_arch": str,
    "sr_conv_ingty": str,
    #valores obligatorios
    "sr_etapa": str,
    "sr_instancia_proceso": str,    
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# Validar widgets
params.validate()

# 🚀 **Ejemplo de Uso**
conf = ConfManager()
db = DBXConnectionManager()
query = QueryManager()

# COMMAND ----------

# Extrae unicamente DATOS para el update a Matriz de Convivencia en Datastage es solo por el ID, verificar
# Datos almacenados en TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01 
statement_desmc_matriz_conv = query.get_statement(
    "020_0100_INFO_PARA_DESMARCA_MATRIZ.sql",
    SR_FOLIO=params.sr_folio,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}",
 )

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}", db.sql_delta(statement_desmc_matriz_conv), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}"))


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

#INSERTA EN MATRIZ_AUX
table_name = "CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

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


db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}")

# COMMAND ----------

Notify.send_notification("DESMARCA", params)
