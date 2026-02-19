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

DS_400_SDO_SOL_IMSS = "DELTA_TRA_DS_400_SDO_SOL_IMSS_" + params.sr_folio

statement_003 = query.get_statement(
    "TRA_DS_300_CTAS_SIN_SDO_SOL.sql",
 DS_400_SDO_SOL_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_SDO_SOL_IMSS}",
 
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_300_CTAS_SIN_SDO_SOL_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_300_CTAS_SIN_SDO_SOL_{params.sr_folio}"))

# COMMAND ----------

DS_101_SDO_DISP_SUBCTAS_IMSS = "DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_" + params.sr_folio
DS_400_SDO_SOL_IMSS = "DELTA_TRA_DS_400_SDO_SOL_IMSS_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_700_CTAS_SIN_DISP_MAS_NSS.sql",
 DS_101_SDO_DISP_SUBCTAS_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_101_SDO_DISP_SUBCTAS_IMSS}",
 DS_400_SDO_SOL_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_SDO_SOL_IMSS}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_700_CTAS_SIN_DISP_MAS_NSS_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_700_CTAS_SIN_DISP_MAS_NSS_{params.sr_folio}"))



# COMMAND ----------

DS_101_SDO_DISP_SUBCTAS_IMSS = "DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_" + params.sr_folio
DS_400_SDO_SOL_IMSS = "DELTA_TRA_DS_400_SDO_SOL_IMSS_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_801_CTAS_NO_ALCN_SDO_DISP.sql",
 DS_101_SDO_DISP_SUBCTAS_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_101_SDO_DISP_SUBCTAS_IMSS}",
 DS_400_SDO_SOL_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_SDO_SOL_IMSS}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_801_CTAS_NO_ALCN_SDO_DISP_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_801_CTAS_NO_ALCN_SDO_DISP_{params.sr_folio}"))


# COMMAND ----------

DS_101_SDO_DISP_SUBCTAS_IMSS = "DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_" + params.sr_folio
DS_400_SDO_SOL_IMSS = "DELTA_TRA_DS_400_SDO_SOL_IMSS_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DS_902_SDOS_IMSS.sql",
 DS_101_SDO_DISP_SUBCTAS_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_101_SDO_DISP_SUBCTAS_IMSS}",
 DS_400_SDO_SOL_IMSS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_SDO_SOL_IMSS}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_902_SDOS_IMSS_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_902_SDOS_IMSS_{params.sr_folio}"))


# COMMAND ----------

#query para Union :
FU_504_UNI_COMPLETA = "DELTA_TRA_DB_ATRIF_TRP_FU_504_UNI_COMPLETA_" + params.sr_folio

statement_003 = query.get_statement(
    "TRA_DB_ATRIF_TRP_DS_700_SALDOS_PROC.sql",
 FU_504_UNI_COMPLETA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{FU_504_UNI_COMPLETA}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_700_SALDOS_PROC_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_700_SALDOS_PROC_{params.sr_folio}"))

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

#INSERTA EN PRE SUF_SALDOS

table_name = "PROCESOS.TTSISGRAL_SUF_SALDOS"

db.write_data(db.read_delta(f"DELTA_TRA_DB_700_PRO_SUF_SALDOS_INS_{params.sr_folio}"), table_name, "default", "append")
