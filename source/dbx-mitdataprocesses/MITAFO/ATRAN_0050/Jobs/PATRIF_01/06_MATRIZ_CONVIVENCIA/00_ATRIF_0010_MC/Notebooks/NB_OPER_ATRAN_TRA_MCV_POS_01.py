# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta, finalmente inserta en OCI el resultado
Subetapa: 
    25 - Matriz de Convivencia-PosMatriz
Trámite:

Tablas input:
  CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV
Tablas output:
  
Tablas Delta:
    DELTA_TRA_MCV_SOL_DM_01_{params.sr_folio}
    
Archivos SQL:
    TRA_MCV_TRANSFERENCIAS_0200_DEL_PRE_MATRIZ.sql
  

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
    "sr_tipo_ejecucion":int,
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_id_snapshot":str,
    "sr_paso":str,
})
# Validar widgets
params.validate()

# COMMAND ----------

conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

#Query Extrae informacion para pre matriz
if params.sr_subproceso == "354":
    statement = query.get_statement(
        "TRA_MCV_0100_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "347" and (params.sr_tipo_ejecucion==0 or params.sr_tipo_ejecucion==2):
    statement = query.get_statement(
        "TRA_MCV_0200_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
                )
elif params.sr_subproceso == "347" and params.sr_tipo_ejecucion==1:
    statement = query.get_statement(
        "TRA_MCV_0300_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "364" or params.sr_subproceso == "365" or params.sr_subproceso == "368":
    statement = query.get_statement(
        "TRA_MCV_0400_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "348" or params.sr_subproceso == "349":
    statement = query.get_statement(
        "TRA_MCV_0500_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "3286":
    statement = query.get_statement(
        "TRA_MCV_0600_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "3832":
    statement = query.get_statement(
        "TRA_MCV_0700_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "3832":
    statement = query.get_statement(
        "TRA_MCV_0700_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "350":
    statement = query.get_statement(
        "TRA_MCV_0800_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "3283":
    statement = query.get_statement(
        "TRA_MCV_0900_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
elif params.sr_subproceso == "363":
    statement = query.get_statement(
        "TRA_MCV_1000_POS_MATRIZ.sql",
        sr_folio=params.sr_folio,
        sr_subproceso=params.sr_subproceso,
        sr_proceso=params.sr_proceso,
        sr_usuario=params.sr_usuario,
        )
else:
    raise Exception("No se encuentra el subproceso")

execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

Notify.send_notification("INFO", params)

# COMMAND ----------


