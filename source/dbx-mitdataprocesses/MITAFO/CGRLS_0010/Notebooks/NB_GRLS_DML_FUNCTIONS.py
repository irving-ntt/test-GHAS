import inspect
import configparser
import sys
#from pyspark.dbutils import DBUtils
from databricks.sdk.runtime import *
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#mandar a traer como parametro
#config_file = 'C:/apps/proyecto_profuturo/POC_DOIMSS/conf/config.py.properties'

def create_table(config_file, conn, spark, df, table_name, partition_col, table_location):
    if conn == 'cloud_azure':
        return_value = create_table_cloud_azure(config_file, conn, spark, df, table_name, partition_col, table_location)
        return return_value
    elif conn == 'oracle':
        #create_table_oracle()
        logger.info("Not supported")
        return '0'
    else:
        logger.info("Not supported")
        return '0'


def drop_table(config_file, conn, spark, table_name, table_location):
    if conn == 'cloud_azure':
        return_value = drop_table_cloud_azure(config_file, conn, spark, table_name, table_location)
        return return_value
    elif conn == 'oracle':
        #create_table_oracle()
        logger.info("Not supported")
        return '0'
    else:
        logger.info("Not supported")
        return '0'


def read_table(config_file, conn, spark, table_name):
    if conn == 'cloud_azure':
        df, failed_task = read_table_cloud_azure(spark, table_name)
        return df, failed_task
    elif conn == 'oracle':
        df, failed_task = read_table_oracle(spark, table_name,conn_options, user_conn, pass_conn)
        return df, failed_task
    else:
        logger.info("Not supported")
        return '0', '0'


def query_table(conn, spark, statement, conn_options, user_conn, pass_conn):
    if conn == 'cloud_azure':
        df, failed_task = query_table_cloud_azure(spark, statement)
        return df, failed_task
    elif conn == 'jdbc_oracle':
        df, failed_task = query_oracle(spark, statement, conn_options, user_conn, pass_conn)
        return df, failed_task
    else:
        logger.info("Not supported")
        return '0', '0'


def write_into_table(conn, df, mode, table_name, conn_options, conn_aditional_options, conn_user, conn_key):
    if conn == 'cloud_azure':
        return_value = insert_table_cloud_azure(config_file, conn, df, table_name)
        return return_value
    elif conn == 'jdbc_oracle':
        return_value = insert_table_oracle(df, mode, table_name, conn_options, conn_aditional_options, conn_user, conn_key)
        return return_value
    else:
        logger.info("Not supported")
        return '0'


def create_file(df, file_name, sep, header):
    try:
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        file_name = file_name + '_TEMP'
        #df.repartition(1).write.mode('overwrite').option('header', header).option('sep', sep).csv(file_name)
        #df.toPandas().to_csv(file_name, sep=sep, encoding='utf-8', index=False, header=True)
        #dfAux = df.collect()
        df.toPandas().to_csv(file_name, sep=sep, index=False, header=header)
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to create file: " + file_name)
        logger.error("Message: " + str(e))
        return '0'
    return '1'



#########################cloud_azure
#table_name must have this format: catalog_name.schema_name.table_name
def create_table_cloud_azure(config_file, conn, spark, df, table_name, partition_col, table_location):
    #spark.conf.set(get_config_options(config_file, conn, "conn_sec_conf"), dbutils.secrets.get(scope = get_config_options(config_file, conn, 'conn_scope'), key = get_config_options(config_file, conn, 'conn_key')))
    try:
        #spark.sql.sources.partitionOverwriteMode='dynamic'
        #spark.sql('DROP TABLE IF EXISTS ' + table_name + ' PURGE')
        #dbutils.fs.rm(table_location, recurse=True)
        #df.write.format('delta').mode('overwrite').partitionBy(partition_col).option("replaceWhere", partition_col + ' == ' + partition_val).saveAsTable(table_name, path = table_location, overwrite = True)
        df = spark.createDataFrame([], df.schema)
        df.write.format('delta').partitionBy(*partition_col).saveAsTable(table_name, path = table_location)
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to create table: " + table_name)
        logger.error("Message: " + str(e))
        return '0'
    return '1'

def drop_table_cloud_azure(config_file, conn, spark, table_name, table_location):
    #spark.conf.set(get_config_options(config_file, conn, "conn_sec_conf"), dbutils.secrets.get(scope = get_config_options(config_file, conn, 'conn_scope'), key = get_config_options(config_file, conn, 'conn_key')))
    try:
        spark.sql('DROP TABLE IF EXISTS ' + table_name + ' PURGE')
        #dbutils.fs.rm(table_location, recurse=True)
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to drop table: " + table_name)
        logger.error("Message: " + str(e))
        return '0'
    return '1'

#return a dataframe
def read_table_cloud_azure(spark, table_name):
    #spark.conf.set(get_config_options(conn, "conn_sec_conf"), dbutils.secrets.get(scope = get_config_options(conn, "conn_scope"), key = get_config_options(conn, "conn_key")))
    try:
        df = spark.table(table_name)
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to read table: " + table_name)
        logger.error("Message: " + str(e))
        return '0', '0'
    return df, '1'

#return a dataframe
def query_table_cloud_azure(spark, statement):
    #spark.conf.set(get_config_options(config_file, conn, "conn_sec_conf"), dbutils.secrets.get(scope = get_config_options(config_file, conn, 'conn_scope'), key = get_config_options(config_file, conn, 'conn_key')))
    try:
        #query = str(query)
        logger.info("Query to execute:")
        logger.info("'")
        logger.info(statement)
        logger.info("'")
        df = spark.sql(statement)
    except Exception as e:
        query_log = statement[1, 30] + "..." if len(statement) > 30 else statement
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to execute query: " + query_log)
        logger.error("Message: " + str(e))
        return '0', '0'
    return df, '1'

#def insert_table_cloud_azure(config_file, conn, df, table_name, partition_col, partition_val, table_location):
def insert_table_cloud_azure(config_file, conn, df, table_name):
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
        table_location = str(spark.sql('describe formatted ' + table_name).filter('col_name == "Location"').select('data_type').collect()[0][0])
        df.repartition(1).write.format('delta').mode('overwrite').option("parquet.block.size", 256 * 1024 * 1024).save(table_location, overwrite=True)
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to insert into: " + table_name)
        logger.error("Message: " + str(e))
        return '0'
    return '1'



#######################################################################################
#########################ORACLE
def read_table_oracle(spark, table_name, conn_options, conn_user, conn_key):
    try:
        scope = conf_process()
        #conn_options = get_config_options(conn, "conn_options")
        #user_conn = dbutils.secrets.get(scope = get_config_options(conn, "user_conn_scope"), key = get_config_options(conn, "user_conn_key"))
        #conn_key = dbutils.secrets.get(scope = get_config_options(conn, "key_conn_scope"), key = get_config_options(conn, "key_conn_key"))
        #df = spark.read("jdbc").options(**conn_options).option("user", conn_user).option("password", conn_key).option("dbtable", table_name).load()
        df = spark.read("jdbc").options(**conn_options).option("user", dbutils.secrets.get(scope, conn_user)).option("password", dbutils.secrets.get(scope, conn_key)).option("dbtable", table_name).load()
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to read table: " + table_name)
        logger.error("Message: " + str(e))
        return '0', '0'
    return df, '1'

#pushdown query, same table query
def query_table_oracle(spark, query, conn_options, conn_user, conn_key):
    try:
        scope = conf_process()
        #conn_options = get_config_options(conn, "conn_options")
        #user_conn = dbutils.secrets.get(scope = get_config_options(conn, "user_conn_scope"), key = get_config_options(conn, "user_conn_key"))
        #conn_key = dbutils.secrets.get(scope = get_config_options(conn, "key_conn_scope"), key = get_config_options(conn, "key_conn_key"))
        logger.info("Query to execute:")
        logger.info("'")
        logger.info(query)
        logger.info("'")
        #df = spark.read("jdbc").options(**conn_options).option("user", conn_user).option("password", conn_key).option("dbtable", str(query)).load()
        df = spark.read("jdbc").options(**conn_options).option("user", dbutils.secrets.get(scope, conn_user)).option("password", dbutils.secrets.get(scope, conn_key)).option("dbtable", str(query)).load()
    except Exception as e:
        query_log = query[1, 30] + "..." if lenght(query) > 30 else query
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to execute query: " + query_log)
        logger.error("Message: " + str(e))
        return '0', '0'
    return df, '1'

def query_oracle(spark, query, conn_options, conn_user, conn_key):
    try:
        scope = conf_process()
        #conn_options = get_config_options(conn, "conn_options")
        #user_conn = dbutils.secrets.get(scope = get_config_options(conn, "user_conn_scope"), key = get_config_options(conn, "user_conn_key"))
        #conn_key = dbutils.secrets.get(scope = get_config_options(conn, "key_conn_scope"), key = get_config_options(conn, "key_conn_key"))
        logger.info("Query to execute:")
        logger.info("'")
        logger.info(query)
        logger.info("'")
        dict_opts = eval(conn_options)
        #df = spark.read.format("jdbc").options(**dict_opts).option("user", conn_user).option("password", conn_key).option("query", str(query)).option("driver", "oracle.jdbc.driver.OracleDriver").load()
        df = spark.read.format("jdbc").options(**dict_opts).option("user", dbutils.secrets.get(scope, conn_user)).option("password", dbutils.secrets.get(scope, conn_key)).option("query", str(query)).option("driver", "oracle.jdbc.driver.OracleDriver").load()
    except Exception as e:
        query_log = query[:50] + "..." if len(query) > 50 else query
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to execute query: " + query_log)
        logger.error("Message: " + str(e))
        return '0', '0'
    return df, '1'

def insert_table_oracle(df, mode, table_name, conn_options, conn_aditional_options, conn_user, conn_key):
    try:
        scope = conf_process()
        dict_opts = eval(conn_options)
        dict_add_opts = eval(conn_aditional_options)
        #df.repartition(10).write.mode(mode).format("jdbc").options(**dict_opts).options(**dict_add_opts).option("user", conn_user).option("password", conn_key).option("dbtable", table_name).save()
        df.write.mode(mode).format("jdbc").options(**dict_opts).options(**dict_add_opts).option("user", dbutils.secrets.get(scope, conn_user)).option("password", dbutils.secrets.get(scope, conn_key)).option("dbtable", table_name).save()
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to save data to : " + table_name)
        logger.error("Message: " + str(e))
        return '0'
    return '1'

def conf_process():
    """ Retrieve process configuration values from a config file.
    Args:
        config_file (str): Path to the configuration file.
        process_name (str): Name of the process.
    Returns:
        tuple: A tuple containing the configuration values and a status flag.
    """
    try:
        import os
        current_dir = os.getcwd()
        root_repo = current_dir[:current_dir.find('MITAFO') + 6]
        config_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_CONN.py.properties"
        process_name = "jdbc_oracle"
        config = configparser.ConfigParser()
        config.read(config_file)
        #Subprocess configurations
        #
        scope = config.get(process_name, 'scope')
        #conn_user = config.get(process_name, 'conn_user')
        #conn_pass = config.get(process_name, 'conn_key')
    except Exception as e:
        logger.error("Function: %s", inspect.stack()[0][3])
        logger.error("An error was raised: " + str(e))
        return '0'
    return scope


