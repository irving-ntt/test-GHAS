# Databricks notebook source
# DBTITLE 1,Descripción
'''
Descripcion:
    Creación de archivo CTINDI para Transferencia de Acreditados Fovissste | Esta notebook corresponde al Job: JP_PATRIF_0020_GEN_TRANSF_FOV que genera el archivo
Subetapa:
    GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      363 - Transferencia de Acreditados Fovissste
Tablas INPUT:
    CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    CTINDI_FOV_EXT_0300_OCI_GEN_ARCHIVO.sql
    CTINDI_FOV_EXT_0400_OCI_TTAFOTRAS_TRANS_FOVISSSTE.sql
    CTINDI_FOV_TRN_0500_DBK.sql
    CTINDI_FOV_TRN_0600_DBK_TRAN_GEN_ARCHIVO.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    #"sr_mask_rec_trp": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
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

#Query DITIMSS

statement = query.get_statement(
    "CTINDI_FOV_EXT_0300_OCI_GEN_ARCHIVO.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_CTINDI_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_01_{params.sr_folio}"))

# COMMAND ----------

#Query TF --> Mapeo Pre Dispersión

statement = query.get_statement(
    "CTINDI_FOV_EXT_0400_OCI_TTAFOTRAS_TRANS_FOVISSSTE.sql",
    sr_subproceso=params.sr_subproceso,
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_CTINDI_02_{params.sr_folio}",  db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_02_{params.sr_folio}"))

# COMMAND ----------

#Query TF --> Mapeo Pre Dispersión

statement = query.get_statement(
    "CTINDI_FOV_TRN_0500_DBK.sql",
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_01_{params.sr_folio}",
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_02_{params.sr_folio}",
)

db.write_delta(f"DELTA_CTINDI_03_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_03_{params.sr_folio}"))

# COMMAND ----------

#Query TF --> Mapeo Pre Dispersión

statement = query.get_statement(
    "CTINDI_FOV_TRN_0600_DBK_TRAN_GEN_ARCHIVO.sql",
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_03_{params.sr_folio}"
)

db.write_delta(f"DELTA_CTINDI_04_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_04_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Validación de archivo vacío
if db.read_delta(f"DELTA_CTINDI_02_{params.sr_folio}").limit(1).count() > 0:

    from datetime import datetime
    import pytz

    # Inicializa la clase para subir los archivos
    file_manager = FileManager(err_repo_path=conf.err_repo_path)


    fecha_actual = dbutils.jobs.taskValues.get(taskKey="NB_OPER_ATRAN_GAR_COND", key="sr_fecha")

    # Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
    # Configuramos el nombre de acuerdo al subproceso ejecutado
    nombre_ctindi = '/TRAFO_'
    sec_lote = '859'

    full_file_name = (
            conf.external_location
            + conf.err_repo_path
            + nombre_ctindi
            + fecha_actual
            + "_"
            + sec_lote
            + ".DAT"
        )

    if conf.debug:
        display(full_file_name)

    # Creación del archivo y decide si calcular MD5 o no
    file_manager.generar_archivo_ctindi(
        df_final=db.read_delta(f"DELTA_CTINDI_04_{params.sr_folio}"),  # DataFrame que se va a guardar
        full_file_name=full_file_name,
        header=conf.header,
        calcular_md5=True  # Cambiar a False si no se requiere calcular el MD5
    )
