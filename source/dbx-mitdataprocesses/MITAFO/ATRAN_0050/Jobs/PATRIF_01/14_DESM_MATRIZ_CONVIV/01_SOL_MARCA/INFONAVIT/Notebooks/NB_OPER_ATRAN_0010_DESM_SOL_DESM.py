# Databricks notebook source
'''
Descripcion:
    Desmarca Matriz de Convivencia
Subetapa:
    Desmarca Matriz de Convivencia
Tramite:
    347 Desmarca de crédito de vivienda por 43 BIS
Tablas Input:
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA 
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO 
Tablas Output:
    
Tablas DELTA:
    DELTA_DESMARCA_{params.sr_folio}

Archivos SQL:
    0010_DESM_SOL_DESM.sql
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
    "0010_DESM_SOL_DESM.sql",
    SR_FOLIO=params.sr_folio,
    TABLE_01=conf.conn_schema_01 + '.' + conf.table_001,
    TABLE_02=conf.conn_schema_02 + '.' + conf.table_002   
)


# COMMAND ----------

db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}", db.read_data("default", statement_001), "overwrite")
if conf.debug:
        display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}"))

# COMMAND ----------

Notify.send_notification("DESMARCA", params)
