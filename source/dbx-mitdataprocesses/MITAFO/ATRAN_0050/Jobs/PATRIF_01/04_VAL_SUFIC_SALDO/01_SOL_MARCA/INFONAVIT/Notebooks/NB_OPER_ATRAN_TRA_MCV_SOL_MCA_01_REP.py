# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta, finalmente inserta en OCI el resultado
Subetapa: 
    3354 - Matriz de Convivnecia
Trámite:
    354 - SOLICITUD DE MARCA DE CUENTAS POR 43 BIS
Tablas input:
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
    CIERREN.TCAFOGRAL_VALOR_ACCION
Tablas output:
    PROCESOS.TTSISGRAL_SUF_SALDOS
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
    CIERREN.TCAFOGRAL_VALOR_ACCION
"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_folio": str,
    "sr_proceso": str,
    "sr_reproceso": str,
    "sr_subetapa": str,     
    "sr_subproceso": str,
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

#Query Extrae informacion de MARCA_DESMARCA

statement = query.get_statement(
    "TRA_DB_100_MARCA_DESMARCA_INFO_REP.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_100_MARCA_DESMARCA_INFO_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_100_MARCA_DESMARCA_INFO_{params.sr_folio}"))

# COMMAND ----------

#Query Extrae informacion de Valor accion

statement = query.get_statement(
    "TRA_DB_402_VALOR_ACCION.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_402_VALOR_ACCION_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_402_VALOR_ACCION_{params.sr_folio}"))

# COMMAND ----------

DELTA_TABLE_DES_INF = "DELTA_TRA_DB_100_MARCA_DESMARCA_INFO_" + params.sr_folio
DELTA_TABLE_VALOR_ACCION = "DELTA_TRA_DB_402_VALOR_ACCION_" + params.sr_folio


# COMMAND ----------

statement_001 = query.get_statement(
"TRA_DELTA_DES_INF_VAL_ACC.sql",
 DELTA_TABLE_DES_INF=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DES_INF}",
 DELTA_TABLE_VALOR_ACCION=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_VALOR_ACCION}",
)

DLTstatement_001 = df = db.sql_delta(query=statement_001)

db.write_delta(f"DELTA_TRA_DELTA_DES_INF_VAL_ACC_{params.sr_folio}", DLTstatement_001, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_DES_INF_VAL_ACC_{params.sr_folio}"))


statement_002 = query.get_statement(
    "TRA_DELTA_DES_INF_SALDO_MENOR.sql",
    DELTA_TABLE_DES_INF=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DES_INF}"
    #sr_folio=params.sr_folio,
)

DLTstatement_002 = df = db.sql_delta(query=statement_002)

db.write_delta(f"DELTA_TRA_DELTA_DES_INF_SALDO_MENOR_{params.sr_folio}", DLTstatement_002, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_DES_INF_SALDO_MENOR_{params.sr_folio}"))


#Ejecuta la consulta sobre la delta
#df = db.sql_delta(query=statement_001)

#display(df)

# COMMAND ----------

#query de join :
DELTA_TABLE_SALDO_MAYOR = "DELTA_TRA_DELTA_DES_INF_VAL_ACC_" + params.sr_folio
DELTA_TABLE_SALDO_MENOR = "DELTA_TRA_DELTA_DES_INF_SALDO_MENOR_" + params.sr_folio

statement_003 = query.get_statement(
    "TRA_DELTA_UNION_SALDOS.sql",
 DELTA_TABLE_SALDO_MAYOR=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_SALDO_MAYOR}",
 DELTA_TABLE_SALDO_MENOR=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_SALDO_MENOR}",
    sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DELTA_UNION_SALDOS_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_UNION_SALDOS_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO BITACORA EN SUFICENCIA DE SALDOS AUXILIAR
statement = query.get_statement(
    "TRA_DB_DEL_SUF_SALDOS_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

#query de INSERT DE SUF SALDOS  :
DELTA_TABLE_INSERT = "DELTA_TRA_DELTA_UNION_SALDOS_" + params.sr_folio


statement_004 = query.get_statement(
    "TRA_DB_INSERT_SUF_SALDOS_REP.sql",
 DELTA_TABLE_INSERT=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_INSERT}",
    sr_folio=params.sr_folio,

)
DLTstatement_004 = df = db.sql_delta(query=statement_004)

db.write_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_REP_{params.sr_folio}", DLTstatement_004, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_REP_{params.sr_folio}"))



# COMMAND ----------


#INSERTA EN PRE SUF_SALDOS

table_name = "CIERREN_DATAUX.TTSISGRAL_SUF_SALDOS_AUX"

db.write_data(db.read_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_REP_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# REALIZA MERGE SUF SALDOS
statement = query.get_statement(
    "TRA_DB_MERGE_SUF_SALDOS.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

#UPDATE EN MARCA DESMARCA

DELTA_TABLE_UPDATE = "DELTA_TRA_DELTA_UNION_SALDOS_" + params.sr_folio


statement_005 = query.get_statement(
    "TRA_DB_UPDATE_SUF_SALDOS_REP.sql",
 DELTA_TABLE_UPDATE=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_UPDATE}",
    sr_folio=params.sr_folio,

)
DLTstatement_005 = df = db.sql_delta(query=statement_005)

db.write_delta(f"DELTA_TRA_DELTA_UPDATE_SALDOS_REP_{params.sr_folio}", DLTstatement_005, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_UPDATE_SALDOS_REP_{params.sr_folio}"))



# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO BITACORA EN TABLA DESMARA MARCA AUXILIAR
statement = query.get_statement(
    "TRA_DB_DEL_DESMARCA_MARCA_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

    #UPDATE EN MARCA_DESMARCA AUX

table_name = "CIERREN_DATAUX.TTCRXGRAL_MARCA_DESMARCA_INFO_AUX"

db.write_data(db.read_delta(f"DELTA_TRA_DELTA_UPDATE_SALDOS_REP_{params.sr_folio}"), table_name, "default", "append")



# COMMAND ----------

# REALIZA MERGE DESMARCA_MARCA_AUX Y DESMARCA_MARCA
statement = query.get_statement(
    "TRA_DB_MERGE_DESMARCA_MARCA.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)
