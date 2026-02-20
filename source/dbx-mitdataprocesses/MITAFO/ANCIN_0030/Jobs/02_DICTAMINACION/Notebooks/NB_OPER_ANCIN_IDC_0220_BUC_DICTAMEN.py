# Databricks notebook source
# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0220_BUC_DICTAMEN")))

# COMMAND ----------

# DBTITLE 1,Construcción de Consulta Principal
statement_001 = query.get_statement(
    "IDC_0220_BUC_DICTAMEN_001.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecución de Consulta Principal
df_principal = db.read_data("default", statement_001)
if conf.debug:
    display(df_principal)
    display(str(df_principal.count()) + " Registros Totales")

# COMMAND ----------

# DBTITLE 1,Variable de fecha y hora
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, to_timestamp

fecha_actual = to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))

display(fecha_actual)

# COMMAND ----------

# DBTITLE 1,Estatus Histórico
from pyspark.sql.functions import lit

df_historico = df_principal.filter(df_principal.FTN_ESTATUS_REG_HIST.isNotNull()).select(
    'FTC_NOMBRE_COMPLETO_ARCH',
    'FTN_NSS_ARCH',
    'FTC_RFC_ARCH',
    'FNC_REG_PATRONAL_IMSS', 
    (df_principal.FTN_ESTATUS_REG_HIST * -1).alias('FTN_ESTATUS_REG'), 
    lit(None).alias('FTN_MOTIVO'), 
    fecha_actual.alias('FTD_FEH_ULTIMO_USO'),
    lit(params.sr_folio).alias('FTC_FOLIO')
)

df_avanza = df_principal.filter(df_principal.FTN_ESTATUS_REG_HIST.isNull()).select(
    'FTC_NOMBRE_COMPLETO_ARCH', 'FTN_NSS_ARCH', 'FTC_RFC_ARCH', 'FTC_RFC_BUC', 
    'FNC_REG_PATRONAL_IMSS', 'FTN_ESTATUS_REG_HIST', 'FTN_MOTIVO_HIST', 
    'COMP_NOMBRES', 'COMP_FECHAS', 'CRITERIO', 'FTC_NOMBRE_COMPLETO_SIN_PESOS', 
    'FTC_NOMBRE_COMPLETO_BUC'
)

if conf.debug:
    display(df_historico)
    display(str(df_historico.count()) + " Registros historicos")
    display(str(df_avanza.count()) +  " Registros No historicos")
    display(df_avanza)

# COMMAND ----------

# DBTITLE 1,Estatus Aceptado
from pyspark.sql.functions import lit
df_aceptado = df_avanza.filter(
    (df_avanza.FTC_NOMBRE_COMPLETO_SIN_PESOS == df_avanza.FTC_NOMBRE_COMPLETO_BUC) & 
    (df_avanza.FTC_RFC_ARCH == df_avanza.FTC_RFC_BUC)
).select(
    'FTC_NOMBRE_COMPLETO_ARCH',
    'FTN_NSS_ARCH',
    'FTC_RFC_ARCH',
    'FNC_REG_PATRONAL_IMSS',
    lit(558).alias('FTN_ESTATUS_REG'), 
    lit(561).alias('FTN_MOTIVO'), 
    fecha_actual.alias('FTD_FEH_ULTIMO_USO'),
    lit(params.sr_folio).alias('FTC_FOLIO')
) 

df_sigueavanzando = df_avanza.filter(
      (df_avanza.FTC_NOMBRE_COMPLETO_SIN_PESOS != df_avanza.FTC_NOMBRE_COMPLETO_BUC) | 
    (df_avanza.FTC_RFC_ARCH != df_avanza.FTC_RFC_BUC)

#df_sigueavanzando = df_avanza.filter(
#~ ((df_avanza.FTC_NOMBRE_COMPLETO_SIN_PESOS == df_avanza.FTC_NOMBRE_COMPLETO_BUC) & 
# (df_avanza.FTC_RFC_ARCH == df_avanza.FTC_RFC_BUC))
).select(
    'FTC_NOMBRE_COMPLETO_ARCH', 'FTN_NSS_ARCH', 'FTC_RFC_ARCH', 'FTC_RFC_BUC', 'FNC_REG_PATRONAL_IMSS', 'FTN_ESTATUS_REG_HIST', 'FTN_MOTIVO_HIST','COMP_NOMBRES','COMP_FECHAS','CRITERIO','FTC_NOMBRE_COMPLETO_SIN_PESOS', 'FTC_NOMBRE_COMPLETO_BUC'
)

if conf.debug:
    display(df_aceptado)
    display(str(df_aceptado.count()) + " Registros Aceptados")
    display(df_sigueavanzando)
    display(str(df_sigueavanzando.count()) +  " Avanzan a evaluación por algoritmo")

# COMMAND ----------

# DBTITLE 1,Estatus Aceptado Algoritmo
from pyspark.sql.functions import lit

df_acep_algoritmo = df_sigueavanzando.filter(
(df_principal.COMP_FECHAS == 1) & (df_principal.COMP_NOMBRES >= df_principal.CRITERIO)
).select(
    'FTC_NOMBRE_COMPLETO_ARCH',
    'FTN_NSS_ARCH',
    'FTC_RFC_ARCH',
    'FNC_REG_PATRONAL_IMSS',
    lit(558).alias('FTN_ESTATUS_REG'), 
    lit(561).alias('FTN_MOTIVO'), 
    fecha_actual.alias('FTD_FEH_ULTIMO_USO'),
    lit(params.sr_folio).alias('FTC_FOLIO')
)
if conf.debug:
    display(df_acep_algoritmo)
    display(str(df_acep_algoritmo.count()) + " Registros Aceptados Algoritmo")

# COMMAND ----------

# DBTITLE 1,Estatus Pendiente Algoritmo
from pyspark.sql.functions import lit

df_rech_algoritmo = df_sigueavanzando.filter(
    (df_principal.COMP_FECHAS == 1) & (df_principal.COMP_NOMBRES < df_principal.CRITERIO)
).select(
    'FTC_NOMBRE_COMPLETO_ARCH',
    'FTN_NSS_ARCH',
    'FTC_RFC_ARCH',
    'FNC_REG_PATRONAL_IMSS',
    lit(560).alias('FTN_ESTATUS_REG'), 
    lit(821).alias('FTN_MOTIVO'), 
    fecha_actual.alias('FTD_FEH_ULTIMO_USO'),
    lit(params.sr_folio).alias('FTC_FOLIO')
)
if conf.debug:
    display(df_rech_algoritmo)
    display(str(df_rech_algoritmo.count()) + " Registros Rechazados Algoritmo")

# COMMAND ----------

# DBTITLE 1,Estatus Rechazado Algoritmo
from pyspark.sql.functions import lit

df_pend_algoritmo = df_sigueavanzando.filter(
    (df_principal.COMP_FECHAS == 0)
).select(
    'FTC_NOMBRE_COMPLETO_ARCH',
    'FTN_NSS_ARCH',
    'FTC_RFC_ARCH',
    'FNC_REG_PATRONAL_IMSS',
    lit(559).alias('FTN_ESTATUS_REG'), 
    lit(562).alias('FTN_MOTIVO'), 
    fecha_actual.alias('FTD_FEH_ULTIMO_USO'),
    lit(params.sr_folio).alias('FTC_FOLIO')
)
if conf.debug:
    display(df_pend_algoritmo)
    display(str(df_pend_algoritmo.count()) + " Registros Pendientes Algoritmo")
    

# COMMAND ----------

# DBTITLE 1,Unión de los 5 Dataframes
df_unido = df_pend_algoritmo.union(df_rech_algoritmo).union(df_acep_algoritmo).union(df_aceptado).union(df_historico)
if conf.debug:
    display(df_unido)
    display(str(df_unido.count()) + " Registros Unidos")

# COMMAND ----------

# DBTITLE 1,Llenado de Tabla Auxiliar
table_name_aux = "CIERREN_DATAUX.TTSISGRAL_ETL_AEIM_AUX"
db.write_data(df_unido, table_name_aux, "default", "append")

# COMMAND ----------

# DBTITLE 1,Construcción de Sentencia para Merge
statement_002 = query.get_statement(
    "IDC_0230_ACT_ESTATUS_002.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecución de Merge
execution = db.execute_oci_dml(
    statement=statement_002, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Construcción de Sentencia Delete Aux
statement_003 = query.get_statement(
    "IDC_0230_ACT_ESTATUS_003.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecución de Delete Aux
#La ejecucion se envia en segundo plano con async_mode=True
execution = db.execute_oci_dml(
    statement=statement_003, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Depuración de Dataframes usados
CleanUpManager.cleanup_notebook(locals())