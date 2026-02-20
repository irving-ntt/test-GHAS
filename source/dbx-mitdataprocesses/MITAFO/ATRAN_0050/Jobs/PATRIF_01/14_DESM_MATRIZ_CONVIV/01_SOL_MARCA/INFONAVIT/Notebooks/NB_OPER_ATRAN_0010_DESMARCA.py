# Databricks notebook source
'''
Descripcion:
    Desmarca Matriz de Convivencia
Subetapa:
    Desmarca Matriz de Convivencia
Tramite:
    354 Solicitud de Marca de Cuentas por 43 BIS
Tablas Input:
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA 
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO 
Tablas Output:
    TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}
Tablas DELTA:
    TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}

Archivos SQL:
    0010_DESMARCA.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_path_arch": str,
    "sr_conv_ingty": str,
    #valores obligatorios
    "sr_etapa": str,
    "sr_instancia_proceso": str,    
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# Validar widgets
params.validate()

# 🚀 **Ejemplo de Uso**
conf = ConfManager()
db = DBXConnectionManager()
query = QueryManager()

# COMMAND ----------

statement_001 = query.get_statement(
    "0010_DESMARCA.sql",
    SR_FOLIO=params.sr_folio,
    TABLE_01=conf.conn_schema_01 + '.' + conf.table_001,
    TABLE_02=conf.conn_schema_02 + '.' + conf.table_002   
)



# COMMAND ----------

df = db.read_data("default", statement_001)

if conf.debug:
    display(df)


# COMMAND ----------

# Filtrar los datos para SUBPROCESO e INFORMACION para archivo 69
df_subproceso = df.filter(df["FTC_TIPO_ARCH"] == "09")
if conf.debug:
    display(df_subproceso)
df_informacion = df.filter(df["FTC_FOLIO"] == f"{params.sr_folio}")
if conf.debug:
    display(df_informacion)

# COMMAND ----------

row_count = df_subproceso.count()
print(f"El número de renglones en el DataFrame es: {row_count}")
row_count = df_informacion.count()
print(f"El número de renglones en el DataFrame es: {row_count}")

if df_subproceso.count() > 0:  # Verificar si el DataFrame tiene uno o más registros
    db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_SUB_354_{params.sr_folio}", df_subproceso, "overwrite")
    if conf.debug:
        display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_SUB_354_{params.sr_folio}"))

if df_informacion.count() > 0:  # Verificar si el DataFrame tiene uno o más registros
    db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}", df, "overwrite")
    if conf.debug:
        display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}"))




