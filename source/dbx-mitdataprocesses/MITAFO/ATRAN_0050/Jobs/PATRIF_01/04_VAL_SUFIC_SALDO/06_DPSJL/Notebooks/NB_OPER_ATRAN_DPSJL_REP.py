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


statement = query.get_statement(
    "TRA_DS_300_SDO_DISP_VIV97.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DS_300_SDO_DISP_VIV97_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_300_SDO_DISP_VIV97_{params.sr_folio}")) 


# COMMAND ----------


statement = query.get_statement(
    "TRA_DS_101_SDO_DISP_SUBCTAS_IMSS.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_{params.sr_folio}")) 

# COMMAND ----------


statement = query.get_statement(
    "TRA_DS_400_SDO_SOL_IMSS.sql", 
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DS_400_SDO_SOL_IMSS_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_400_SDO_SOL_IMSS_{params.sr_folio}")) 

# COMMAND ----------

statement = query.get_statement(
    "TRA_DS_500_SDO_SOL_INFON.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DS_500_SDO_SOL_INFON_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_500_SDO_SOL_INFON_{params.sr_folio}")) 

# COMMAND ----------

DS_101_SDO_DISP_SUBCTAS_IMSS = "DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_" + params.sr_folio
DS_400_SDO_SOL_IMSS = "DELTA_TRA_DS_400_SDO_SOL_IMSS_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_700_CTAS_SIN_DISP_IMSS.sql",
 DS_101_SDO_DISP_SUBCTAS_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_101_SDO_DISP_SUBCTAS_IMSS}",
 DS_400_SDO_SOL_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_SDO_SOL_IMSS}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_700_CTAS_SIN_DISP_IMSS_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_700_CTAS_SIN_DISP_IMSS_{params.sr_folio}"))

# COMMAND ----------

DS_101_SDO_DISP_SUBCTAS_IMSS = "DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_" + params.sr_folio
DS_400_SDO_SOL_IMSS = "DELTA_TRA_DS_400_SDO_SOL_IMSS_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_600_SDOS_IMSS.sql",
 DS_101_SDO_DISP_SUBCTAS_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_101_SDO_DISP_SUBCTAS_IMSS}",
 DS_400_SDO_SOL_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_SDO_SOL_IMSS}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_600_SDOS_IMSS_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_600_SDOS_IMSS_{params.sr_folio}"))

# COMMAND ----------


DS_500_SDO_SOL_INFON = "DELTA_TRA_DS_500_SDO_SOL_INFON_" + params.sr_folio
DS_300_SDO_DISP_VIV97 = "DELTA_TRA_DS_300_SDO_DISP_VIV97_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_300_CTAS_SIN_DISP_INFO.sql",
 DS_500_SDO_SOL_INFON=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_500_SDO_SOL_INFON}",
 DS_300_SDO_DISP_VIV97=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_300_SDO_DISP_VIV97}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_300_CTAS_SIN_DISP_INFO_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_300_CTAS_SIN_DISP_INFO_{params.sr_folio}"))

# COMMAND ----------

statement = query.get_statement(
    "TRA_DB_600_CAT_VALOR_ACCION.sql", 
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_600_CAT_VALOR_ACCION_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_600_CAT_VALOR_ACCION_{params.sr_folio}")) 

# COMMAND ----------

DS_500_SDO_SOL_INFON = "DELTA_TRA_DS_500_SDO_SOL_INFON_" + params.sr_folio
DS_300_SDO_DISP_VIV97 = "DELTA_TRA_DS_300_SDO_DISP_VIV97_" + params.sr_folio
DB_600_CAT_VALOR_ACCION = "DELTA_TRA_DB_600_CAT_VALOR_ACCION_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_1000_SDOS_1NSS_INFO.sql",
 DS_500_SDO_SOL_INFON=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_500_SDO_SOL_INFON}",
 DS_300_SDO_DISP_VIV97=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_300_SDO_DISP_VIV97}",
 DB_600_CAT_VALOR_ACCION=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DB_600_CAT_VALOR_ACCION}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_1000_SDOS_1NSS_INFO_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_1000_SDOS_1NSS_INFO_{params.sr_folio}"))

# COMMAND ----------

DS_500_SDO_SOL_INFON = "DELTA_TRA_DS_500_SDO_SOL_INFON_" + params.sr_folio
DS_300_SDO_DISP_VIV97 = "DELTA_TRA_DS_300_SDO_DISP_VIV97_" + params.sr_folio
DB_600_CAT_VALOR_ACCION = "DELTA_TRA_DB_600_CAT_VALOR_ACCION_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_1002_NSS_MULTIPLES.sql",
 DS_500_SDO_SOL_INFON=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_500_SDO_SOL_INFON}",
 DS_300_SDO_DISP_VIV97=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_300_SDO_DISP_VIV97}",
 DB_600_CAT_VALOR_ACCION=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DB_600_CAT_VALOR_ACCION}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_1002_NSS_MULTIPLES_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_1002_NSS_MULTIPLES_{params.sr_folio}"))

# COMMAND ----------

DS_1002_NSS_MULTIPLES = "DELTA_TRA_DS_1002_NSS_MULTIPLES_" + params.sr_folio
DS_300_CTAS_SIN_DISP_INFO = "DELTA_TRA_DS_300_CTAS_SIN_DISP_INFO_" + params.sr_folio
DS_1000_SDOS_1NSS_INFO = "DELTA_TRA_DS_1000_SDOS_1NSS_INFO_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_13_INS_SDOS_INFO.sql",
 DS_1002_NSS_MULTIPLES=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_1002_NSS_MULTIPLES}",
 DS_300_CTAS_SIN_DISP_INFO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_300_CTAS_SIN_DISP_INFO}",
 DS_1000_SDOS_1NSS_INFO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_1000_SDOS_1NSS_INFO}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_13_INS_SDOS_INFO_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_13_INS_SDOS_INFO_{params.sr_folio}"))

# COMMAND ----------



DS_600_SDOS_IMSS = "DELTA_TRA_DS_600_SDOS_IMSS_" + params.sr_folio
DS_700_CTAS_SIN_DISP_IMSS = "DELTA_TRA_DS_700_CTAS_SIN_DISP_IMSS_" + params.sr_folio
DS_13_INS_SDOS_INFO = "DELTA_TRA_DS_13_INS_SDOS_INFO_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_BD_400_VAL_SDOS_DPSJL.sql",
 DS_600_SDOS_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_600_SDOS_IMSS}",
 DS_700_CTAS_SIN_DISP_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_700_CTAS_SIN_DISP_IMSS}",
 DS_13_INS_SDOS_INFO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_13_INS_SDOS_INFO}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_BD_400_VAL_SDOS_DPSJL_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_BD_400_VAL_SDOS_DPSJL_{params.sr_folio}"))

# COMMAND ----------

statement = query.get_statement(
    "TRA_DB_DEL_PROCESOS.TTCRXGRAL_VAL_SDOS_DPSJL.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

table_name = "PROCESOS.TTCRXGRAL_VAL_SDOS_DPSJL"

db.write_data(db.read_delta(f"DELTA_TRA_BD_400_VAL_SDOS_DPSJL_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

DS_600_SDOS_IMSS = "DELTA_TRA_DS_600_SDOS_IMSS_" + params.sr_folio
DS_700_CTAS_SIN_DISP_IMSS = "DELTA_TRA_DS_700_CTAS_SIN_DISP_IMSS_" + params.sr_folio
DS_13_INS_SDOS_INFO = "DELTA_TRA_DS_13_INS_SDOS_INFO_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_BD_700_PRO_DEV_PAG_SJL.sql",
 DS_600_SDOS_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_600_SDOS_IMSS}",
 DS_700_CTAS_SIN_DISP_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_700_CTAS_SIN_DISP_IMSS}",
 DS_13_INS_SDOS_INFO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_13_INS_SDOS_INFO}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_BD_700_PRO_DEV_PAG_SJL_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_BD_700_PRO_DEV_PAG_SJL_{params.sr_folio}"))
