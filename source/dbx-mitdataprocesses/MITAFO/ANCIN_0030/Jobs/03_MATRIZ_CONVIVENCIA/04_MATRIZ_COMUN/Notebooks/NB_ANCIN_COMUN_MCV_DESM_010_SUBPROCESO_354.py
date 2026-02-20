# Databricks notebook source
""" 
Descripcion:
    NB_ANCIN_COMUN_MCV_DESM_010_SUBPROCESO_354
    lee la informacion deL SUB PROCESO 354 y genera tabla delta para EL UPDATE a la tabla de Matriz de convivencia (Desmarca)
Subetapa: 
    25 - Matriz de Convivencia
Trámite:
    COMUN - 
Tablas input:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
    DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}        
Tablas output:
    TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
Tablas Delta:
    N/A
Archivos SQL:
    ATRAN_DESM_0100_SUBPROCESO_354.sql
    """

# COMMAND ----------

# DBTITLE 1,Cargar Framework 🚀
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Definir y validar parametros
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_tipo_mov": str,
    "sr_conv_ingty": str,
    "sr_path_arch": str,
    "sr_tipo_ejecucion": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

# DBTITLE 1,Cargar configuraciones locales
conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,QUERY DATOS
# Query para generar delta para la desmarca y generacion del archivo 69
statement_001 = query.get_statement(
    "ATRAN_DESM_0100_SUBPROCESO_354.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

# DBTITLE 1,Guardamos resultado en un DF
df = db.read_data("default", statement_001)

if conf.debug:
    display(df)


# COMMAND ----------

# DBTITLE 1,dividimos el DF en sub proceso e Info para archivo 69
# Filtrar los datos para SUBPROCESO e INFORMACION para archivo 69
df_subproceso = df.filter(df["FTC_TIPO_ARCH"] == "09")
if conf.debug:
    display(df_subproceso)
df_informacion = df.filter(df["FTC_FOLIO"] == f"{params.sr_folio}")
if conf.debug:
    display(df_informacion)

# COMMAND ----------

# DBTITLE 1,GUARDAMOS DF's A DELTAS

row_count = df_subproceso.count()
print(f"El número de renglones en el DataFrame es: {row_count}")
row_count = df_informacion.count()
print(f"El número de renglones en el DataFrame es: {row_count}")

if df_subproceso.count() > 0:  # Verificar si el DataFrame tiene uno o más registros
    db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_SUB_354_{params.sr_folio}", df_subproceso, "overwrite")
    if conf.debug:
        display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}"))

if df_informacion.count() > 0:  # Verificar si el DataFrame tiene uno o más registros
    db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}", df, "overwrite")
    if conf.debug:
        display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}"))




