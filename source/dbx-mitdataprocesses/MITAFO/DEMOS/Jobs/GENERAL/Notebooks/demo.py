# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Notebook de Procesamiento de Datos
# MAGIC ***
# MAGIC **TODO**: Documentar nuevas funciones y readme de la demo
# MAGIC
# MAGIC *** Test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar framework

# COMMAND ----------

# DBTITLE 1,Cargar framework
# MAGIC %run "./startup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir y validar parametros

# COMMAND ----------

# DBTITLE 1,Definir y validar parametros
# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({
    # Parametros normales
    "sr_folio_rel": str,
    "sr_proceso": str,
    "sr_fecha_liq": (str, "YYYYMMDD"),  # Fecha en formato YYYYMMDD
    "sr_tipo_mov": str,
    "sr_reproceso": str,
    "sr_subproceso": str,
    "sr_origen_arc": str,
    "sr_fecha_acc": (str, "YYYYMMDD"),  # Otra fecha en formato YYYYMMDD
    "sr_folio": str,
    "sr_subetapa": str,
    "sr_tipo_archivo": str,
    "var_tramite": str,
    # valores obligatorios para servicios
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_id_snapshot": str,
    "sr_paso": str,

})
# Validar widgets
params.validate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar configuraciones locales

# COMMAND ----------

# DBTITLE 1,Cargar configuraciones locales
# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
# 🚀 **Ejemplo de Uso**
conf = ConfManager()

# 🔍 Acceder a una variable como atributo
logger.info(conf.debug)  # En lugar de conf.get("debug")

# 🔍 Validar si están presentes variables requeridas
conf.validate(["debug", "err_repo_path", "output_file_name_001"])

# 🔍 Mostrar la documentación
logger.info(conf.help())

# 🔍 Acceder a todas las variables cargadas
logger.info(conf.env_variables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## La clase ConfManager() ya hace conversion de tipo de datos booleanos.

# COMMAND ----------

logger.info(conf.debug) # esto es como un print
type(conf.debug)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instancia para manejador de queries

# COMMAND ----------

# DBTITLE 1,Instancia para manejador de queries
query = QueryManager()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mostrar lista de queries disponibles

# COMMAND ----------

# DBTITLE 1,Mostrar lista de queries disponibles
display(query.get_sql_list())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accesar a configuraciones locales

# COMMAND ----------

table_name_main = f"{conf.schema_001}.{conf.table_name_001}"
table_name_aux = f"{conf.schema_002}.{conf.table_name_002}"
logger.info(f"table_name_main: {table_name_main}")
logger.info(f"table_name_aux: {table_name_aux}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear statement

# COMMAND ----------

# DBTITLE 1,Crear statement
statement_demo_001 = query.get_statement(
    "demo_001.sql",
    TTSISGRAL_ETL_MOVIMIENTOS_AUX_OR_MAIN=table_name_main,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear una instancia para el manejador de conexiones

# COMMAND ----------

# DBTITLE 1,Crear una instancia para el manejador de conexciones
db = DBXConnectionManager()
db.help()

# COMMAND ----------

# En tu notebook:
db = DBXConnectionManager()

# Probar TODO tu flujo completo
from test_my_instance import test_my_dbx_instance
result = test_my_dbx_instance(db)

# COMMAND ----------

from test_concurrent_notebooks import quick_singleton_test
quick_singleton_test()

# COMMAND ----------

from test_concurrent_notebooks import test_concurrent_notebooks
result = test_concurrent_notebooks(num_notebooks=20, max_workers=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear configuraciones para conexiones de OCI personalizadas.

# COMMAND ----------

db.create_or_update_config("default", 20000, 40000, 100)
# 1. Nombre de la configuracion personalizada
# 2. fetch_size
# 3. batch_size
# 4. num_partitions
# NOTA: si tienen mas ideas, puedes agregar mas parametros

# COMMAND ----------

display(db.list_configs())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data (Tabla CIERREN_ETL)

# COMMAND ----------

# DBTITLE 1,Read data (Tabla CIERREN_ETL)
df = db.read_data("default", statement_demo_001)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Guarda datos en delta table (Overwrite)

# COMMAND ----------

# DBTITLE 1,Guarda datos en delta table (Overwrite y con borrado permitido)
# Primera llamada - tabla borrable (por defecto)
db.write_delta(f"demo_delta_001_{params.sr_folio}", df, "overwrite")
# Log: 🆕 Registrando nueva tabla 'demo_delta_001_123' con allow_delete=True

# COMMAND ----------

# DBTITLE 1,Guarda datos en delta table (Overwrite y con borrado NO permitido)
# Segunda llamada - tabla NO borrable
db.write_delta(f"demo_delta_002_{params.sr_folio}", df, "overwrite", allow_delete=False, delete_until=2)
# Log: �� Tabla 'demo_delta_001_123' ya existe en control. Cambiando allow_delete de True a False

# COMMAND ----------

# MAGIC %md
# MAGIC # Listar tablas deltas creadas (Desactivado)

# COMMAND ----------

# DBTITLE 1,Listar tablas del notebook actual (detección automática)
# Listar tablas del notebook actual (detección automática)
db.list_delta_tables_by_notebook().show()
# Log: 🔍 Detectando automáticamente notebook: NB_OPER_AGRLS_CGA_0010_CARGA_ARCH
# Log: 📋 Listando tablas Delta para notebook: NB_OPER_AGRLS_CGA_0010_CARGA_ARCH

# COMMAND ----------

# DBTITLE 1,Listar tablas creadas de un solo notebook
# Listar tablas de un notebook específico
db.list_delta_tables_by_notebook("demo").show()

# COMMAND ----------

# DBTITLE 1,Listar tablas creadas de multiples notebooks
# Listar tablas de multiples notebooks
db.list_delta_tables_by_notebook(["demo", "otro_notebook", "tercer_notebook"]).show()

# COMMAND ----------

# DBTITLE 1,# Limpiar tablas del notebook actual (detección automática)
# Limpiar tablas del notebook actual (detección automática)
db.cleanup_delta_tables_by_notebooks(table_suffix="9123456789")

# COMMAND ----------

# DBTITLE 1,Limpiar tablas de notebooks específicos
# Limpiar tablas de notebooks específicos
db.cleanup_delta_tables_by_notebooks(["demo", "otro_notebook"], table_suffix="123")

# COMMAND ----------

# DBTITLE 1,Guarda datos en delta table (Overwrite)
db.write_delta(f"demo_delta_001_{params.sr_folio}", df, "overwrite")

# COMMAND ----------

# DBTITLE 1,Guardar datos en delta table sin usar un dataframe
db.write_delta(f"demo_delta_002_{params.sr_folio}", db.read_data("prueba", statement_demo_001), "overwrite")
# La línea anterior crea una tabla llamada "demo_delta_002_<sr_folio>" a partir del resultado de la lectura de la tabla descrita en el "statement_demo_001". 
# El overwrite borra la tabla delta si esta ya existe.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer delta de forma standard

# COMMAND ----------

# DBTITLE 1,Leer delta de forma standard
df_from_delta_001=db.read_delta(f"demo_delta_001_{params.sr_folio}")
display(df_from_delta_001)

df_from_delta_002=db.read_delta(f"demo_delta_002_{params.sr_folio}")
display(df_from_delta_002)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer delta con query

# COMMAND ----------

# DBTITLE 1,Leer delta con query
statement_para_delta = f"""
WHERE FTN_NUM_CTA_INVDUAL = 500057958
"""
df_from_delta=db.read_delta(f"demo_delta_001_{params.sr_folio}", query=statement_para_delta)
display(df_from_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guarda datos en delta table (Append)

# COMMAND ----------

# DBTITLE 1,Guarda datos en delta table (Append)
db.write_delta(f"demo_delta_001_{params.sr_folio}", df, "append")
df_from_delta=db.read_delta(f"demo_delta_001_{params.sr_folio}")
display(df_from_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear statement para consultas complejas sobre tablas deltas

# COMMAND ----------

display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,Acceder a configuraciones globales
statement_demo_003 = query.get_statement(
    "demo_003.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.demo_delta_001_{params.sr_folio}",
    ID_REFERENCIA=10883
)

# COMMAND ----------

# DBTITLE 1,Enviar consulta
df_consulta_compleja = db.sql_delta(statement_demo_003)
display(df_consulta_compleja)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Borrar delta

# COMMAND ----------

# DBTITLE 1,Borrar delta
db.drop_delta(f"demo_delta_001_{params.sr_folio}")
db.drop_delta(f"demo_delta_002_{params.sr_folio}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data OCI

# COMMAND ----------

# DBTITLE 1,Write data
db.write_data(df, table_name_aux, "default", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear statement (Tabla CIERREN_DATAUX)

# COMMAND ----------

# DBTITLE 1,Crear statement (Tabla CIERREN_DATAUX)
statement_demo_002 = query.get_statement(
    "demo_001.sql",
    TTSISGRAL_ETL_MOVIMIENTOS_AUX_OR_MAIN=table_name_aux,
    SR_FOLIO=params.sr_folio
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data (Tabla CIERREN_DATAUX)

# COMMAND ----------

# DBTITLE 1,Read data (Tabla CIERREN_DATAUX)
df = db.read_data("default",statement_demo_002)
df.cache() # Insertamos la tabla en cache
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear statement de borrado e invocacion de funcion de borrado Scala (Modo normal)

# COMMAND ----------

statement_demo_003 = query.get_statement(
    "demo_002.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement_demo_003, async_mode=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear statement de borrado e invocacion de funcion de borrado Scala (Async Mode)

# COMMAND ----------

# DBTITLE 1,Crear statement de borrado e invocacion de funcion de borrado Scala
statement_demo_003 = query.get_statement(
    "demo_002.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement_demo_003, async_mode=True
)

# COMMAND ----------

# DBTITLE 1,Verificar estado de ejecucion en segundo plano
# Verificar estado simple
status = db.check_job_execution_status(execution)

# COMMAND ----------

# DBTITLE 1,Verificar estado de ejecucion en segundo plano detallado
# Logging detallado
logger.info(f"Estado: {status['message']}")
logger.info(f"¿Finalizó?: {status['is_finished']}")
logger.info(f"Filas afectadas: {status.get('affected_rows', 'N/A')}")
logger.info(f"Tiempo de ejecución: {status.get('execution_time_ms', 'N/A')}ms")
logger.info(f"URL de ejecución: {status['execution_url']}")
logger.info(f"Statement: {execution.get('statement_preview', 'N/A')}")

# COMMAND ----------

# DBTITLE 1,Esperar hasta que una ejecucion en segundo plano finalice
import time

# Configuración del ciclo
max_wait_time = 7200  # 2 horas en segundos
check_interval = 1    # Verificar cada 30 segundos
start_time = time.time()

logger.info("�� Iniciando monitoreo del job...")

while True:
    status = db.check_job_execution_status(execution)
    
    # Mostrar progreso
    elapsed_time = time.time() - start_time
    logger.info(f"⏰ Tiempo transcurrido: {elapsed_time:.0f}s - {status['message']}")
    
    if status['is_finished']:
        if status['result_state'] == 'SUCCESS':
            logger.info("🎉 ¡Job completado exitosamente!")
        else:
            logger.error(f"❌ Job falló con estado: {status['result_state']}")
        break
    
    # Verificar timeout
    if elapsed_time > max_wait_time:
        logger.error("⏰ Timeout alcanzado - Job no terminó en 2 horas")
        break
    
    # Esperar antes de verificar nuevamente
    logger.info(f"⏳ Esperando {check_interval}s antes de verificar...")
    time.sleep(check_interval)

# COMMAND ----------

# DBTITLE 1,Mandar multiples consultas en segundo plano y monitorearlas
# DBTITLE 1,Ejecutar múltiples consultas en paralelo usando la nueva función
# Lista de statements a ejecutar
statements = [
    statement_demo_003,
    statement_demo_003,
    statement_demo_003,
]

# Ejecutar todos los jobs en paralelo usando la nueva función
result = db.execute_multiple_statements_parallel(
    statements=statements,
    check_interval=30,  # Verificar cada 30 segundos
    max_wait_time=7200,  # Timeout de 2 horas
)

# Mostrar resumen de resultados
logger.info(f"📊 Resumen de ejecución:")
logger.info(f"   - Total de jobs: {result['total_jobs']}")
logger.info(f"   - Jobs exitosos: {result['successful_jobs']}")
logger.info(f"   - Jobs fallidos: {result['failed_jobs']}")
logger.info(f"   - Tiempo total: {result['total_time_seconds']:.0f} segundos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enviar notificacion

# COMMAND ----------

# DBTITLE 1,Enviar notificacion
Notify.send_notification("INFO", params)

# COMMAND ----------

def test():
    var1 = 1
    var2 = 0
    display(var1 / var2)  # Esto generará ZeroDivisionError

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulacion de error

# COMMAND ----------

# DBTITLE 1,Simulacion de error
#test()
temp_tabla_001

# COMMAND ----------

# Verificar que las configuraciones automáticas estén funcionando
status = db.check_adaptive_partitioning_status()

print(status['message'])
print(f"Recomendación: {status['recommendation']}")

# Ejemplo de salida esperada:
# ✅ ÓPTIMO: Databricks decidirá particiones automáticamente para cada notebook
# Recomendación: Configuración ideal para cientos de notebooks concurrentes

# COMMAND ----------

# DBTITLE 1,DF para testear creacion de CTINDI
df_test_ctindi = db.read_delta("DELTA_VALIDA_CONTENIDO_001_999999995")
display(df_test_ctindi)

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Creacion de archivos 

# COMMAND ----------

# DBTITLE 1,Generar archivo CTINDI
# Inicializa la clase
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Llama al método para generar el archivo
full_file_name = "abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/INF/CTINDI1_20250313_001.DAT"

# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df_test_ctindi,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=conf.header,
    calcular_md5=True  # Cambia a False si no quieres calcular el MD5
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Si decides calcular el MD5 en otro momento, puedes llamarlo manualmente:

# COMMAND ----------

# DBTITLE 1,Calcular el MD5
full_file_name_md5 = full_file_name + ".md5"

# Generar el MD5 del archivo después de crearlo
file_manager.generar_md5(full_file_name, full_file_name_md5)

# COMMAND ----------

# DBTITLE 1,Generar .csv
file_manager = FileManager(err_repo_path=conf.err_repo_path)

full_file_name = (
    SETTINGS.GENERAL.EXTERNAL_LOCATION + conf.err_repo_path + "/" + params.sr_id_archivo + "_" + conf.output_file_name_001 + ".csv"
)
 
file_manager.generar_archivo_flexible(
    df=df_test_ctindi,
    full_file_name=full_file_name,
    opciones={ # Aqui pones las opciones que necesites.
        "header": "true",
        "encoding": "ISO-8859-1",
        "delimiter": ",",
        "quote": '"',
        "charset": "latin1",
        "escapeQuotes": False,
        "ignoreLeadingWhiteSpace": "false",   # 🔑 Mantener espacios iniciales
        "ignoreTrailingWhiteSpace": "false"   # 🔑 Mantener espacios finales
    },
    calcular_md5=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpiar datos

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
