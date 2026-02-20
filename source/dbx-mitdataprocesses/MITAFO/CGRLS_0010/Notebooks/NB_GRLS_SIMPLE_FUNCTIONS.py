import json
import pandas as pd
import re
import datetime
import os
import configparser
import inspect
import requests
import logging
from databricks.sdk.runtime import *

#Name: replacing_values
#VALUE: STRING of characters
#PARAMS: ARRAY of values to replace, Ej. [VAL1, VAL2, VAL3... VALN]
##Example:
##VALUE: "my name is VAL1 and VAL2 is my dog. Hey VAL2!"
##PARAMS: ['JUAN', 'LUCKY']
##OUTPUT: "my name is JUAN and LUCKY is my dog. Hey LUCKY!"

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def replacing_values(VALUE, PARAMS):
    for x in range(0,len(PARAMS)):
        VALUE = VALUE.replace("#PARAM@{0}#".format(x + 1), str(PARAMS[x]))
    return VALUE


def get_value_from_list(VALUE, QUERY_ID):
    OUTPUT = [j for i, j in VALUE if i == QUERY_ID]
    OUTPUT = ''.join(OUTPUT)
    if len(OUTPUT) == 0:
        return '0'
    return OUTPUT


def getting_statement(CONF_VALUES, QUERY_STATEMENT, PARAMS):
    STATEMENT = get_value_from_list(CONF_VALUES, QUERY_STATEMENT)
    if STATEMENT == '0':
        return '0', '0'
    STATEMENT = replacing_values(STATEMENT, PARAMS)
    return STATEMENT, '1'


def conf_init_values(config_file, process_name, process):
    try:
        config = configparser.ConfigParser()
        config.read(config_file)
        #Root configurations
        #full_path = config.get('root', 'root_folder') + '/' + config.get('root', 'project_folder')
        #full_bin_path = full_path + '/' + config.get('root', 'bin_folder')
        #full_conf_path = full_path + '/' + config.get('root', 'config_folder')
        if process == 'VAL_ESTRUCT':
            webhook_url = config.get('root', 'webhook_url_val_str')
        elif process == 'CARG_ARCH':
            webhook_url = config.get('root', 'webhook_url_carg_arch')
        elif process =='INTER_CONT':
            webhook_url = config.get('root', 'webhook_url_intr_cont')
        else:
            webhook_url = config.get('root', 'webhook_url_default')
        channel = config.get('root', 'channel')
        #Process configurations
        #process_name = config.get('doimss', 'process_name')
        #process_conn_conf_path = full_conf_path + '/' + config.get(process_name, 'conn_conf_file')
        #
        #catalog_default = config.get(process_name, 'conn_catalog_default')
        #schema_default = config.get(process_name, 'conn_schema_default')
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("An error was raised: " + str(e))
        return '0','0','0'
    return webhook_url, channel, '1'


def conf_conn_values(config_file, conn_name):
    conn_config = configparser.ConfigParser()
    conn_config.read(config_file)
    #Conn configurations
    if 'cloud' in conn_name:
        try:
            blob_storage = conn_config.get(conn_name, 'conn_blob_storage')
            storage_account = conn_config.get(conn_name, 'conn_storage_account')
            url_domain = conn_config.get(conn_name, 'conn_url_domain')
            scope = conn_config.get(conn_name, 'conn_scope')
            key = conn_config.get(conn_name, 'conn_key')
            repo_path = conn_config.get(conn_name, 'conn_data_file_repository')
            return blob_storage, storage_account, url_domain, scope, key, repo_path, '1'
        except Exception as e:
            logger.error("Function: " + str(inspect.stack()[0][3]))
            logger.error("An error was raised: " + str(e))
            return '0','0','0','0','0','0'
    elif 'jdbc' in conn_name:
        try:
            conn_options = conn_config.get(conn_name, 'conn_options')
            conn_aditional_options = conn_config.get(conn_name, 'conn_aditional_options')
            conn_user = conn_config.get(conn_name, 'conn_user')
            #user_key = conn_config.get(conn_name, 'conn_user_key')
            conn_key = conn_config.get(conn_name, 'conn_key')
            #key_key = conn_config.get(conn_name, 'conn_key_key')
            conn_url = conn_config.get(conn_name, 'conn_url')
            scope = conn_config.get(conn_name, 'scope')
            return conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, '1'
        except Exception as e:
            logger.error("Function: " + str(inspect.stack()[0][3]))
            logger.error("An error was raised: " + str(e))
            return '0','0','0','0','0','0','0','0','0'
    else:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("An error was raised: " + str(e))
        return '0'


def send_slack_messages(webhook_url, messages):
    data=[]
    
    for datos, valor in messages[3]:
        data.append([messages[0], messages[1], messages[2], datos, valor])
    
    df = pd.DataFrame(data, columns=['code', 'message','source','params', 'value'])

    j = (df.groupby(['code','message','source'])
       .apply(lambda x: dict(zip(x.params, x.value)))
       .reset_index()
       .rename(columns={0:'additionalParameters'})
       .to_json(orient='records'))

    payload = json.dumps(json.loads(j), sort_keys=False)

    payload = re.sub('^\[', '', re.sub('\]$', '', re.sub('\"$', '', re.sub('^\"', '',payload))))

    token = obtener_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    #headers = {
    #    "Content-Type": "application/json"
    #}

    logger.info(webhook_url)
    logger.info(payload)
    #response = requests.post(webhook_url, json=payload, headers=headers)
    response = requests.post(webhook_url, data=payload, headers=headers)
    logger.info("Response: " + str(response.status_code))
    if response.status_code not in [200, 204]:
      raise ValueError(f"Error sending Slack messages: {response.text}")
    return response


def notification_raised(webhook_url, code, message, source, input_parameters):
    logger = logging.getLogger(__name__)
    #messages = [sr_proceso,sr_subproceso,sr_subetapa,sr_id_archivo,sr_path_arch,sr_origen_arc,'FAILED']
    try:
        #send_slack_messages(webhook_url, channel, messages)
        send_slack_messages(webhook_url, [code, message, source, input_parameters])
        #if stop_process.upper() == 'Y':
        #    logger.error("Process ends")
        #    #raise Exception("Process ends")
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("An error was raised: " + str(e))
    #dbutils.notebook.exit("Process finisehd on error")


def obtener_token():
    try:
        import os
        current_dir = os.getcwd()
        root_repo = current_dir[:current_dir.find('MITAFO') + 6]
        config_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_CONN.py.properties"
        process_name = 'jdbc_oracle'
        config = configparser.ConfigParser()
        config.read(config_file)
        scope = config.get(process_name, 'scope')
        username = dbutils.secrets.get(scope=scope, key='dbxServicesUser')
        password = dbutils.secrets.get(scope=scope, key='dbxServicesPassword')
        config_file_2 = root_repo + "/" + "/CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties"
        process_name_2 = 'root'
        config_2 = configparser.ConfigParser()
        config_2.read(config_file_2)
        url = config_2.get(process_name_2, 'scope_notifications')
    except Exception as e:
        logger.error("Function: %s", inspect.stack()[0][3])
        logger.error("An error was raised: " + str(e))
        return '0'
    
    payload = 'grant_type=client_credentials'
    
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        response = requests.post(url, headers=headers, data=payload, auth=(username, password))
        response.raise_for_status()
        token = response.json().get("access_token")  # Bearer token
        return token
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener el token: {e}")
        return None


def get_config_options(conn, config_file, value):
    config = configparser.ConfigParser()
    try:
        config.read(str(config_file))
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to read config file: " + config_file)
        logger.error("Message: " + str(e))
        return '0'
    try:
        value = config.get(conn, value)
    except Exception as e:
        logger.error("Function: " + str(inspect.stack()[0][3]))
        logger.error("Trying to read value: " + value)
        logger.error("Message: " + str(e))
        return '0'


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
        config_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties"
        config = configparser.ConfigParser()
        config.read(config_file)
        #Subprocess configurations
        #
        scope = config.get(process_name, 'scope_notifications')
        #conn_user = config.get(process_name, 'conn_user')
        #conn_pass = config.get(process_name, 'conn_key')
    except Exception as e:
        logger.error("Function: %s", inspect.stack()[0][3])
        logger.error("An error was raised: " + str(e))
        return '0'
    return scope




