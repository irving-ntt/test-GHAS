# Databricks notebook source
'''
Descripcion:
    Desmarca cuentas por folio
Subetapa:
    Desmarca cuentas
Tramite:
    8 - DO IMSS
    118 - ACLARACIONES ESPECIALES
    439 - GUBER IMSS
    121 - INTERESES IMSS
    120 - DO ISSSTE
    210 - GUBER ISSSTE
    122 - INTERESES ISSSTE    
Tablas Input:
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
    PROFAPOVOL.TTAFOAVOL_PRINCIPAL
Tablas Output:
    CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
Tablas DELTA:
    NA

Archivos SQL:
    DESMARCA_0010_MOVTOS_001.sql
    DESMARCA_0010_MOVTOS_002.sql
    DESMARCA_0010_MOVTOS_003.sql
    DESMARCA_0010_MOVTOS_004.sql
    DESMARCA_0010_MOVTOS_005.sql

'''

# COMMAND ----------

# DBTITLE 1,FRAMEWORK UPLOAD
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,WIDGETS
# Crear la instancia con los parámetros esperados
params = WidgetParams({    
    "sr_folio" : str ,
    "sr_proceso" : str ,
    "sr_subproceso" : str ,
    "sr_subetapa" : str ,
    "sr_tipo_mov" : str ,
    "sr_accion" : str ,
    "sr_instancia_proceso" : str ,
    "sr_usuario" : str ,
    "sr_etapa" : str ,
    "sr_id_snapshot" : str 
})

# Validar widgets
params.validate()

# 🚀 **Ejemplo de Uso**
conf = ConfManager()
db = DBXConnectionManager()
query = QueryManager()

# COMMAND ----------

# DBTITLE 1,VALIDACIÓN DE LOGITUD FOLIO
from pyspark.sql.functions import col, length, substring, when

# Crea un DataFrame con el valor de sr_folio
val_folio = spark.createDataFrame([(params.sr_folio,)], ["sr_folio"])

# Evalúa la condición y crea la columna val_folio
val_folio = val_folio.withColumn("val_folio", when(
    (length(col("sr_folio")) == 18) &  # Verifica si la longitud de sr_folio es 18
    (substring(col("sr_folio"), 1, 4) > '2016') &  # Verifica si los primeros 4 caracteres de sr_folio son mayores a '2016'
    (substring(col("sr_folio"), 1, 4) < '3017'),  # Verifica si los primeros 4 caracteres de sr_folio son menores a '3017'
    1  # Si todas las condiciones se cumplen, asigna 1 a val_folio
).otherwise(0))  # Si alguna condición no se cumple, asigna 0 a val_folio

# Muestra el DataFrame
if conf.debug:
    display(val_folio)

# COMMAND ----------

# DBTITLE 1,CONSULTA DESMARCA FOLIO O INSTANCIA
import sys

valor_folio = int(val_folio.select('val_folio').first()['val_folio'])

#SR_ACCION 0 = Desmarca matriz

# Arma la consulta para desmarcar el folio
if valor_folio == 1 and params.sr_accion == "0":
     statement_001 = query.get_statement(
    "DESMARCA_0010_MOVTOS_001.sql",
    SR_ACCION = params.sr_accion,
    SR_FOLIO = params.sr_folio
)
     df_desmarca = db.read_data("default",statement_001)
     if conf.debug:
        display(df_desmarca)


# Arma la consulta para desmarcar la instancia proceso AVOL
elif valor_folio == 0 and params.sr_accion == "0":
     statement_002 = query.get_statement(
    "DESMARCA_0010_MOVTOS_002.sql",
     SR_FOLIO = params.sr_folio
)
     df_desmarca = db.read_data("default",statement_002)
     if conf.debug:
        display(df_desmarca)




# COMMAND ----------

df_desmarca.createOrReplaceTempView(f"temp_view_001_{params.sr_folio}")
if conf.debug:  
    display(df_desmarca)

# COMMAND ----------

# DBTITLE 1,RENOMBRE DE CAMPOS PARA LLENAR AUX
statement_003 = query.get_statement(
    "DESMARCA_0010_MOVTOS_003.sql",
     TEMP_VIEW = f"temp_view_001_{params.sr_folio}"
)

df_output_table = db.sql_delta(statement_003)
if conf.debug:
    display(df_output_table)

# COMMAND ----------

# DBTITLE 1,BORRADO DE AUX
statement_005 = query.get_statement(
    "DESMARCA_0010_MOVTOS_005.sql",
    TABLE_001=conf.conn_schema_default
    +
    '.'
    +
    conf.table_001,
    SR_FOLIO = params.sr_folio
)
execution = db.execute_oci_dml(
   statement=statement_005, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,LLENADO DE TABLA AUXILIAR PARA UPDATE
target_table = conf.conn_schema_default + '.' + conf.table_001
db.write_data(df_output_table, target_table, "default", "append")

# COMMAND ----------

# DBTITLE 1,INVOCACIÓN DE MERGE PARA UPDATE
statement_004 = query.get_statement(
    "DESMARCA_0010_MOVTOS_004.sql" ,
    TABLE_002=conf.conn_schema_default_2
    +
    '.'
    +
    conf.table_002,    
    TABLE_001=conf.conn_schema_default
    +
    '.'
    +
    conf.table_001,
    SR_FOLIO = params.sr_folio
)

execution = db.execute_oci_dml(
    statement=statement_004, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,DELETE TABLA AUXILIAR
statement_005 = query.get_statement(
    "DESMARCA_0010_MOVTOS_005.sql",
    TABLE_001=conf.conn_schema_default
    +
    '.'
    +
    conf.table_001,
    SR_FOLIO = params.sr_folio
)
execution = db.execute_oci_dml(
   statement=statement_005, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,NOTIFICACIÓN DE FINALIZADO
Notify.send_notification("DESMARCA", params)
