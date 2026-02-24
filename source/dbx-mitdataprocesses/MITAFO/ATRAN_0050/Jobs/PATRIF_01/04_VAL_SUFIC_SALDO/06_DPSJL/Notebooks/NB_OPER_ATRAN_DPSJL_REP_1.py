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

#Query Extrae informacion para TRP

statement = query.get_statement(
    "TRA_DS_300_SDO_DISP_VIV97.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DS_300_SDO_DISP_VIV97_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_300_SDO_DISP_VIV97_{params.sr_folio}")) 


# COMMAND ----------

#Query Extrae informacion para TRP

statement = query.get_statement(
    "TRA_DS_101_SDO_DISP_SUBCTAS_IMSS.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_{params.sr_folio}")) 

# COMMAND ----------

#Query Extrae informacion para TRP

statement = query.get_statement(
    "TRA_DS_101_SDO_DISP_SUBCTAS_IMSS.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_101_SDO_DISP_SUBCTAS_IMSS_{params.sr_folio}")) 

# COMMAND ----------

DS_300_TRANFERENCIAS = "DELTA_TRA_DB_ATRIF_TRP_DS_300_TRANFERENCIAS_" + params.sr_folio


# COMMAND ----------

#query para DS_700_SALDOS_PROC :
DS_300_TRANFERENCIAS = "DELTA_TRA_DB_ATRIF_TRP_DS_300_TRANFERENCIAS_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DB_ATRIF_TRP_DS_400_RejInsSaldos.sql",
 DS_300_TRANFERENCIAS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_300_TRANFERENCIAS}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_400_RejInsSaldos_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_400_RejInsSaldos_{params.sr_folio}"))

# COMMAND ----------

#Query Extrae informacion para VALOR_ACCION DE  TRP

statement = query.get_statement(
    "TRA_DB_ATRIF_TRP_DB_500_VALOR_ACCION.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_ATRIF_TRP_DB_500_VALOR_ACCION_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_TRP_DB_500_VALOR_ACCION_{params.sr_folio}")) 


# COMMAND ----------

#query para DS_400_NNS_UNICO :
DS_300_TRANFERENCIAS = "DELTA_TRA_DB_ATRIF_TRP_DS_300_TRANFERENCIAS_" + params.sr_folio
DS_500_VALOR_ACCION = "DELTA_TRA_DB_ATRIF_TRP_DB_500_VALOR_ACCION_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DB_ATRIF_TRP_DS_400_NNS_UNICO.sql",
 DS_300_TRANFERENCIAS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_300_TRANFERENCIAS}",
 DS_500_VALOR_ACCION=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_500_VALOR_ACCION}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_400_NNS_UNICO_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_400_NNS_UNICO_{params.sr_folio}"))

# COMMAND ----------


#query para TRA_DB_ATRIF_TRP_DS_500_NSS_VAR :
DS_300_TRANFERENCIAS = "DELTA_TRA_DB_ATRIF_TRP_DS_300_TRANFERENCIAS_" + params.sr_folio
DS_500_VALOR_ACCION = "DELTA_TRA_DB_ATRIF_TRP_DB_500_VALOR_ACCION_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DB_ATRIF_TRP_DS_500_NSS_VAR.sql",
 DS_300_TRANFERENCIAS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_300_TRANFERENCIAS}",
 DS_500_VALOR_ACCION=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_500_VALOR_ACCION}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DS_400_NSS_VAR_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DS_400_NSS_VAR_{params.sr_folio}"))


# COMMAND ----------

#query para Union :
DS_500_NSS_VAR = "DELTA_TRA_DS_400_NSS_VAR_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DB_ATRIF_TRP_FU_504_UNI_COMPLETA.sql",
 DS_500_NSS_VAR=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_500_NSS_VAR}",
 DS_400_RejInsSaldos=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_RejInsSaldos}",
  DS_400_NNS_UNICO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_NNS_UNICO}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DB_ATRIF_TRP_FU_504_UNI_COMPLETA_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_TRP_FU_504_UNI_COMPLETA_{params.sr_folio}"))

# COMMAND ----------

#query para Union :
FU_504_UNI_COMPLETA = "DELTA_TRA_DB_ATRIF_TRP_FU_504_UNI_COMPLETA_" + params.sr_folio

statement_003 = query.get_statement(
    "TRA_DB_ATRIF_TRP_DB_700_PRO_SUF_SALDOS_INS.sql",
 FU_504_UNI_COMPLETA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{FU_504_UNI_COMPLETA}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DB_700_PRO_SUF_SALDOS_INS_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_700_PRO_SUF_SALDOS_INS_{params.sr_folio}"))

# COMMAND ----------

#query para Union :
FU_504_UNI_COMPLETA = "DELTA_TRA_DB_ATRIF_TRP_FU_504_UNI_COMPLETA_" + params.sr_folio

statement_003 = query.get_statement(
    "TRA_DB_ATRIF_TRP_DB_600_TRP.sql",
 FU_504_UNI_COMPLETA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{FU_504_UNI_COMPLETA}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DB_600_TRP_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_600_TRP_{params.sr_folio}"))

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
