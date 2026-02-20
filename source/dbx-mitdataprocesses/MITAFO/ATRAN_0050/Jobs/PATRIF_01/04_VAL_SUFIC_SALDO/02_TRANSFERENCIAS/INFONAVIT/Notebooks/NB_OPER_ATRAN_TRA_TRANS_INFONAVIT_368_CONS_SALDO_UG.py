# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta, finalmente inserta en OCI el resultado
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    354 - Solicitud de marca de cuentas por 43 bis
Tablas input:
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
    PROCESOS.TTSISGRAL_SUF_SALDOS
    CIERREN.TCAFOGRAL_VALOR_ACCION
    CIERREN.TRAFOGRAL_MOV_SUBCTA
    CIERREN.TFAFOGRAL_CONFIG_SUBPROCESO
Tablas output:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
Tablas Delta:
    DELTA_TRA_MCV_SOL_MCA_01_{params.sr_folio}
    
Archivos SQL:
    TRA_MCV_SOL_MARCA_0100_EXT_INFO.sql
    TRA_MCV_SOL_MARCA_0200_DEL_PRE_MATRIZ.sql
  

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

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_900_NSS_MULTIPLES.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_900_NSS_MULTIPLES_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_900_NSS_MULTIPLES_{params.sr_folio}")) 

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_NSS_UNICO.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_NSS_UNICO_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_NSS_UNICO_{params.sr_folio}"))

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_600_RejInsSaldos.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_600_RejInsSaldos_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_600_RejInsSaldos_{params.sr_folio}"))

# COMMAND ----------

DS_900_NSS_MULTIPLES = "DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_900_NSS_MULTIPLES_" + params.sr_folio
DS_NSS_UNICO = "DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_NSS_UNICO_" + params.sr_folio
DS_600_RejInsSaldos = "DELTA_TRA_DB_ATRIF_VSS_0100_INFO_TRANS_368_DS_600_RejInsSaldos_" + params.sr_folio

# COMMAND ----------

statement_001 = query.get_statement(
"TRA_DB_ATRIF_VSS_0100_INFO_TRANSFER_368_UNION.sql",
 DS_900_NSS_MULTIPLES=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_900_NSS_MULTIPLES}",
 DS_NSS_UNICO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_NSS_UNICO}",
 DS_600_RejInsSaldos=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_600_RejInsSaldos}",
  sr_folio=params.sr_folio,
)

DLTstatement_001 = df = db.sql_delta(query=statement_001)

db.write_delta(f"DELTA_TRA_INFONAVIT_TRANSFER_368_UNION_{params.sr_folio}", DLTstatement_001, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_INFONAVIT_TRANSFER_368_UNION_{params.sr_folio}"))

# COMMAND ----------

#query para DS_700_SALDOS_PROC :
DELTA_TABLE_DS_700_SALDOS_PROC_368 = "DELTA_TRA_INFONAVIT_TRANSFER_368_UNION_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DB_ATRIF_368_DS_700_SALDOS_PROC.sql",
 DELTA_TABLE_DS_700_SALDOS_PROC_368=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DS_700_SALDOS_PROC_368}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DB_ATRIF_DS_700_SALDOS_PROC_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_DS_700_SALDOS_PROC_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO BITACORA EN SUFICENCIA DE SALDOS
statement = query.get_statement(
    "TRA_DB_DEL_SUF_SALDOS.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------


#query de INSERT DE SUF SALDOS  :
#DELTA_TABLE_INSERT = "DELTA_TRA_DELTA_UNION_SALDOS_" + params.sr_folio
FU_603_UNION_TOTAL = "DELTA_TRA_INFONAVIT_TRANSFER_368_UNION_" + params.sr_folio


statement_004 = query.get_statement(
    "TRA_DB_ATRIF_368_BD_606_TL_PRO_SUF_SALDOS.sql",
 FU_603_UNION_TOTAL=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{FU_603_UNION_TOTAL}",
    sr_folio=params.sr_folio,

)
DLTstatement_004 = df = db.sql_delta(query=statement_004)

db.write_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_368_{params.sr_folio}", DLTstatement_004, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_368_{params.sr_folio}"))

# COMMAND ----------

#INSERTA EN PRE SUF_SALDOS

table_name = "PROCESOS.TTSISGRAL_SUF_SALDOS"

db.write_data(db.read_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_368_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

#query de INSERT DE TRANS_INFONA :

FU_603_UNION_TOTAL = "DELTA_TRA_INFONAVIT_TRANSFER_368_UNION_" + params.sr_folio


statement_004 = query.get_statement(
    "TRA_DB_ATRIF_368_BD_605_TL_PRO_TRANS_INFONA.sql",
 FU_603_UNION_TOTAL=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{FU_603_UNION_TOTAL}",
    sr_folio=params.sr_folio,

)
DLTstatement_004 = df = db.sql_delta(query=statement_004)

db.write_delta(f"DELTA_TRA_DELTA_TRANS_INFONA_SALDOS_368_{params.sr_folio}", DLTstatement_004, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_TRANS_INFONA_SALDOS_368_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO BITACORA EN TRANS_INFONA_AUX
statement = query.get_statement(
    "TRA_DB_DEL_TRANS_INFONA_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

table_name = "CIERREN_DATAUX.TTCRXGRAL_TRANS_INFONA_AUX"

db.write_data(db.read_delta(f"DELTA_TRA_DELTA_TRANS_INFONA_SALDOS_368_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

statement = query.get_statement(
    "TRA_DB_ATRIF_368_MERGE_TRANS_INFONA.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)
