# Databricks notebook source
'''
Descripcion:
    Desmarca Matriz de Convivencia
Subetapa:
    Desmarca Matriz de Convivencia
Tramite:
    348 Devolución de Excedentes por 43 BIS
    349 Devolución de Saldos Excedentes por T.A.A.G.
Tablas Input:
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA 
    PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO 
Tablas Output:
    
Tablas DELTA:
    DELTA_DESMARCA_{params.sr_folio}

Archivos SQL:
    0010_DEV_SLDOS_EXC.sql
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

statement_001 = query.get_statement(
    "0010_DEV_SLDOS_EXC.sql",
    SR_FOLIO=params.sr_folio,
    TABLE_01=conf.conn_schema_01 + '.' + conf.table_001,
    TABLE_06=conf.conn_schema_02 + '.' + conf.table_006   
)



# COMMAND ----------

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}", db.read_data("default", statement_001), "overwrite")
if conf.debug:
        display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}"))

# COMMAND ----------

Notify.send_notification("DESMARCA", params)
