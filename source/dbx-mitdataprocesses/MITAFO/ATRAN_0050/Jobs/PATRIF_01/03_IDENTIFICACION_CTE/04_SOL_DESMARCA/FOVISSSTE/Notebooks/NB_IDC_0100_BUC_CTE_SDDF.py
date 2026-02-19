# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Realiza la extracción inicial para el subproceso que se va a ejecutar en la identificación del cliente
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC 	364  - Transferencias de Acreditados Infonavit
# MAGIC 	365  - Transferencia por Anualidad Garantizada
# MAGIC 	368  - Uso de Garantía por 43 BIS
# MAGIC 	348  - Devolución de Excedentes por 43 BIS
# MAGIC 	349  - Devolución de Saldos Excedentes por T.A.A.G.
# MAGIC 	347  - Desmarca de Crédito de Vivienda por 43 BIS
# MAGIC 	3286 - Transferencia de Acreditados por Portabilidad
# MAGIC 	363  - Transferencia de Acreditados Fovissste
# MAGIC 	350  - Devolución de Excedentes por T.A. Fovissste
# MAGIC 	3283 - Desmarca Fovissste
# MAGIC 	3832 - Devolución de Pago sin Justificación Legal
# MAGIC Tablas INPUT:
# MAGIC     CIERREN_ETL.TTAFOTRAS_ETL_MARCA_DESMARCA
# MAGIC 	CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_INFONAVIT
# MAGIC 	CIERREN_ETL.TTAFOTRAS_ETL_DEVO_SALDO_EXC DS
# MAGIC     CIERREN_ETL.TLSISGRAL_ETL_BUC
# MAGIC     CIERREN_ETL.TTAFOTRAS_ETL_TRANS_REC_PORTA
# MAGIC 	CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_FOVISSSTE
# MAGIC 	CIERREN_ETL.TTAFOTRAS_ETL_DEV_SLD_EXC_FOV
# MAGIC 	CIERREN_ETL.TTAFOTRAS_ETL_DESMARCA_FOVST
# MAGIC 	CIERREN_ETL.TTAFOTRAS_ETL_DEV_PAG_SJL
# MAGIC Tablas OUTPUT:
# MAGIC     N/A
# MAGIC Tablas INPUT DELTA:
# MAGIC     N/A
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     DELTA_102_MARCA_43BIS
# MAGIC 	DELTA_300_TRANSFERENCIA
# MAGIC 	DELTA_800_TRAMITE
# MAGIC 	DELTA_102_TRP
# MAGIC 	DELTA_300_DPSJL
# MAGIC Archivos SQL:
# MAGIC     SQL_IDC_100_INFONAVIT.sql
# MAGIC 	SQL_IDC_100_TRANF_INFON.sql
# MAGIC 	SQL_IDC_200_BUC_DSE.sql
# MAGIC     SQL_IDC_100_DESM_INFO.sql
# MAGIC     SQL_IDC_100_TRP.sql
# MAGIC     SQL_IDC_100_TRANS_FOV.sql
# MAGIC     SQL_IDC_100_BUC_DSEF.sql
# MAGIC     SQL_IDC_100_BUC_SDDF.sql
# MAGIC     SQL_IDC_100_DPSJL.sql
# MAGIC

# COMMAND ----------

# DBTITLE 1,INICIO - CONFIGURACIONES PRINCIPALES
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,PARAMETROS
# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({

    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_proceso": str,
    "sr_subetapa": str,
    "sr_dt_org_arc": str,
    "sr_origen_arc": str,
    "sr_subproceso": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
})

# Validar widgets
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()


# COMMAND ----------

# DBTITLE 1,VALIDA EXISTENCIA SQL

queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
if conf.debug:
    display(
        queries_df.filter(
            col("Archivo SQL").startswith("SQL_IDC_100")
        )
    )

# COMMAND ----------

# DBTITLE 1,VERIFICA SUBPROCESO ACTIVO
SUBPROCESO = dbutils.widgets.get("sr_subproceso")

if SUBPROCESO == '3283':
    Query = "SQL_IDC_100_BUC_SDDF.sql"
    Delta = "DELTA_111_TRAMITE"
    
display("Q: " + Query + '  D: ' + Delta)



# COMMAND ----------

# DBTITLE 1,CONSTRUCCION QUERY SEGUN SUBPROCESO
statement_001 = query.get_statement(
    #Query DEFINICION DEPENDE DEL SUBPROCESO,
    Query,
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_001)

if conf.debug:
    display(df)


# COMMAND ----------

# DBTITLE 1,LLENADO DE TABLA DELTA DEL PROCESO 100
#Delta DEFINICION DEPENDE DEL SUBPROCESO
temp_view = Delta + '_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
