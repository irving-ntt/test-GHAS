# Databricks notebook source
'''
Descripcion:
    
Subetapa:
    
Tramite:
    
Tablas Input:
   
Tablas Output:
    
Tablas DELTA:
    

Archivos SQL:
 
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

params.validate()
conf = ConfManager()
db = DBXConnectionManager()
query = QueryManager()

# COMMAND ----------

db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_03_PROCESO_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_04_SUB_PROCESO_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_ENC_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_DET_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_SUM_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_ARCHIVO_69_FINAL_ORD_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_05_RESP_ITGY_{params.sr_folio}")
db.drop_delta(f"TEMP_DELTA_COMUN_VAL_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}")

# COMMAND ----------

#NOTIFICACION
Notify.send_notification("DESMARCA", params)
