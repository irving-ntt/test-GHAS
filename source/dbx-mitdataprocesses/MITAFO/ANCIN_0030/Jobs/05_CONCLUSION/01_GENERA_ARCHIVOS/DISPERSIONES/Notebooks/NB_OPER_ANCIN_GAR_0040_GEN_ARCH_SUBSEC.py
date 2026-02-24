# Databricks notebook source
'''
Descripcion:
    Creación de archivo CTINDI para Subsecuente 92 y 98. 
Subetapa:
    GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      105 - Subsecuente 92 y 98
Tablas INPUT:
    CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    GAR_0040_GEN_ARCH_SUBSEC_EXT_001.sql
    GAR_0040_GEN_ARCH_SUBSEC_GEN_002.sql
'''

# COMMAND ----------

# DBTITLE 1,Inicio - Configuraciones Principales
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
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
        'p_TIPO_SUBCTA': str , #-- This parameter is defined or given at notebook level, IS NOT a paremeter requested to services, this because this query must be used in diferent notebooks just changin this filter
        'p_CODMOV': str,# -- This parameter is defined or given at notebook level, IS NOT a paremeter requested to services, this because this query must be used in diferent notebooks just changin this filter
    }
)
# Validar widgets
params.validate()

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Extraccion de datos
statement = query.get_statement(
    "GAR_0040_GEN_ARCH_SUBSEC_EXT_001.sql",
    sr_folio=params.sr_folio,
    p_TIPO_SUBCTA = params.p_TIPO_SUBCTA,
)

db.write_delta(f"DELTA_CTINDI_SUBSEC_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_SUBSEC_01_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Transformaciones y concatenamiento
statement = query.get_statement(
    "GAR_0040_GEN_ARCH_SUBSEC_GEN_002.sql",
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_SUBSEC_01_{params.sr_folio}",
    p_CODMOV = params.p_CODMOV
)

db.write_delta(f"DELTA_CTINDI_SUBSEC_02_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_SUBSEC_02_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Validación de Archivo Vacío
if db.read_delta(f"DELTA_CTINDI_SUBSEC_02_{params.sr_folio}").limit(1).count() == 0 and params.p_CODMOV == '831':
    Notify.send_notification("INFO", params)
    dbutils.notebook.exit("No hay información para generar el archivo.")

# COMMAND ----------

# DBTITLE 1,Configuraciones del archivo
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
if params.p_CODMOV == '855':
    nombre_ini = "/SUBIS92_"
    nombre_fin = "_125.DAT"
else:
    nombre_ini = "/SUBIS08_"
    nombre_fin = '_126.DAT'

full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + nombre_ini
        + fecha_actual
        + nombre_fin
    )
full_file_name_md5 = (
        conf.external_location
        + conf.err_repo_path
        + nombre_ini
        + fecha_actual
        + nombre_fin + ".md5"
    )
if conf.debug:
    display(full_file_name)

# COMMAND ----------

# DBTITLE 1,Creación de Archivo CTINDI
# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=db.read_delta(f"DELTA_CTINDI_SUBSEC_02_{params.sr_folio}"),  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=conf.header,
    calcular_md5=False  # Cambiar a False si no se requiere calcular el MD5
)

# COMMAND ----------

# DBTITLE 1,Copia del Archivo
#En caso de que se requiera validar el archivo en la maquina local
#dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/CTINDI1_20250603_001.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')

# COMMAND ----------

# DBTITLE 1,Listado de archivos
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')

# COMMAND ----------

# DBTITLE 1,Notificación de proceso finalizado
if params.p_CODMOV == '831':
    Notify.send_notification("INFO", params)

# COMMAND ----------

# DBTITLE 1,Fin - Limpieza
CleanUpManager.cleanup_notebook(locals())
