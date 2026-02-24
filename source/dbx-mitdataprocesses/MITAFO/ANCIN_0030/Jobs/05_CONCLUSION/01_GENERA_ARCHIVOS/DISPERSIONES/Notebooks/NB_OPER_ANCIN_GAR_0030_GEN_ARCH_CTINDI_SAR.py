# Databricks notebook source
# DBTITLE 1,Descripcion
'''
Descripcion:
    Generacion de el archvio de el subproceso 120 - Dispersión Ordinaria ISSSTE SAR. 
    CTISDI1_20221114_123.DAT  - Archivo de dispersión Ordinaria ISSSTE SAR
Subetapa:
     GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      120 - Dispersión Ordinaria ISSSTE SAR
Tablas INPUT:
    TTSISGRAL_ETL_GEN_ARCHIVO
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas DELTA:
    TTSISGRAL_ETL_CTINDI_SAR001_[sr_folio]
    TTSISGRAL_ETL_CTINDI_SAR002_[sr_folio]
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    ANCIN_GAR_0030_GEN_ARCH_CTINDI_SAR_001.sql
    ANCIN_GAR_0030_GEN_ARCH_CTINDI_SAR_002.sql
    ANCIN_GAR_0030_GEN_ARCH_CTINDI_SAR_003.sql
'''

# COMMAND ----------

# DBTITLE 1,Cargar framework
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Definir y validar parametros
params = WidgetParams(
    {
        "sr_folio": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_subetapa": str,
        "sr_sec_lote": str,
        "sr_fecha_lote": str,
        "sr_fecha_acc": str,
        "sr_tipo_archivo": str,
        "sr_estatus_mov": str,
        "sr_tipo_mov": str,
        "sr_accion": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str, # es obligatorio para servicios
    }
)

# Validar widgets
params.validate()

conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(
    queries_df.filter(
        col("Archivo SQL").startswith("ANCIN_GAR_0030_GEN_ARCH_CTINDI_SAR")
    )
)


# COMMAND ----------

# DBTITLE 1,1. Construcción de Query 001
statement_001 = query.get_statement(
    "ANCIN_GAR_0030_GEN_ARCH_CTINDI_SAR_001.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

# DBTITLE 1,2. Extraccion de datos 001
df_001 = db.read_data("default", statement_001)
if conf.debug:
    display(df_001)

# COMMAND ----------

# DBTITLE 1,3. Crea tabla delta 001
temp_view = 'TTSISGRAL_ETL_CTINDI_SAR' + '001' + '_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")

if conf.debug:
    display(db.read_delta(temp_view))


# COMMAND ----------

# DBTITLE 1,1. Construcción de Query 002
statement_002 = query.get_statement(
    "ANCIN_GAR_0030_GEN_ARCH_CTINDI_SAR_002.sql",
    tabla_delta=temp_view,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

# DBTITLE 1,2. Extraccion de datos 002
df_002 = db.sql_delta(statement_002)
if conf.debug:
    display(df_002)

# COMMAND ----------

# DBTITLE 1,3. Crea tabla delta 002
temp_view_002 = 'TTSISGRAL_ETL_CTINDI_SAR' + '002' + '_' + params.sr_folio
db.write_delta(temp_view_002, db.sql_delta( statement_002), "overwrite")

if conf.debug:
   display(db.read_delta(temp_view))

# COMMAND ----------

# DBTITLE 1,1. Construcción de Query 003
statement_003 = query.get_statement(
    "ANCIN_GAR_0030_GEN_ARCH_CTINDI_SAR_003.sql",
    tabla_delta=temp_view_002,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    hints="/*+ PARALLEL(4) */")

# COMMAND ----------

# DBTITLE 1,2. Extraccion de datos 003
df_003 = db.sql_delta(statement_003)
if conf.debug:
    display(df_003)

# COMMAND ----------

# DBTITLE 1,Configuraciones del archivo
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")



full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + "/CTISDI1_"
        + fecha_actual
        + "_123.DAT"
    )

if conf.debug:
    display(full_file_name)


# COMMAND ----------

# DBTITLE 1,Creación de Archivo CTINDI
# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df_003,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=conf.header,
    calcular_md5=True  # Cambia a False si no quieres calcular el MD5
)

# COMMAND ----------

# DBTITLE 1,Limpiar datos
CleanUpManager.cleanup_notebook(locals())
