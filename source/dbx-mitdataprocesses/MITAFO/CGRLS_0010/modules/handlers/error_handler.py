import sys
import traceback
from logger import logger
from IPython.core.interactiveshell import InteractiveShell
from handlers.notification_handler import Notify
from handlers.widget_handler import WidgetParams
from databricks.sdk.runtime import dbutils  # Asegurar que se usa Databricks
from pyspark.sql.utils import AnalysisException  # Importar excepción de PySpark
from pyspark.sql.connect.client.core import SparkConnectException
import unicodedata

# Diccionario de mapeo para asignar "process" según el nombre del notebook
NOTEBOOK_PROCESS_MAP = {
    "NB_OPER_AGRLS_CGA_0010_CARGA_ARCH": "CARG_ARCH",
    # Agrega más notebooks aquí según sea necesario
}

def get_process_from_notebook_name(notebook_key):
    """Determina el proceso según el nombre del notebook."""
    if "_STRL_" in notebook_key:
        return "VAL_ESTRUCT"
    
    if "_VAL_CONT_" in notebook_key:
        return "VAL_CONT"
    
    return NOTEBOOK_PROCESS_MAP.get(notebook_key, "process")  # Valor por defecto

def custom_exception_handler(shell, exc_type, exc_value, exc_traceback, tb_offset=None):
    """Manejador global de excepciones para notebooks en Databricks y Jupyter."""

    if issubclass(exc_type, KeyboardInterrupt):
        shell.showtraceback()
        return

    # 🔹 Obtener el nombre completo del notebook en Databricks
    try:
        notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        notebook_name = "Notebook_Desconocido"

    # Extraer solo la última parte del notebook después de "Notebooks/"
    notebook_key = notebook_name.split("Notebooks/")[-1] if "Notebooks/" in notebook_name else notebook_name

    # Determinar el valor del parámetro "process" basado en notebook_key
    process_param = get_process_from_notebook_name(notebook_key)
    
    traceback_message = ''.join(char for char in unicodedata.normalize('NFD', str(exc_value)[0:200]) if unicodedata.category(char) != 'Mn')

    # 🔹 Capturar el error con el traceback
    error_message = (
        f"\n⚠️  ERROR DETECTADO ⚠️\n"
        f"Tipo de Error: {exc_type.__name__}\n"
        f"Mensaje: {exc_value}\n"
        f"Traceback:\n{''.join(traceback.format_tb(exc_traceback))}\n"
        f"🔍 Por favor, revisa tu código y corrige el error."
    )

    # Registrar en el logger
    logger.error(error_message)

    # 🔹 Intentar capturar `params` desde `sys.modules`
    error_params = {}

    params_key = f"params_{notebook_name}"  # Clave única para evitar colisiones

    logger.info(f"📌 Buscando `params` en sys.modules con clave `{params_key}`")

    if notebook_name == "Notebook_Desconocido":
        error_params = {"error": "No se pudo obtener el nombre del notebook."}
        logger.warning("⚠️ No se pudo identificar el notebook. Enviando mensaje de error en params.")

    else:
        try:
            if params_key in sys.modules:
                params_instance = sys.modules[params_key]
                logger.info(f"🔍 `params` detectado en `sys.modules`: {params_instance}")
                
                # Convertimos a diccionario
                error_params = params_instance.to_dict()

            else:
                # 🔹 Si `params_key` no está en sys.modules, intentar obtenerlo desde dbutils.widgets
                logger.info("🔄 `params_key` no encontrado en `sys.modules`, obteniendo desde `dbutils.widgets`...")

                try:
                    error_params = {key: value for key, value in dbutils.widgets.getAll().items()}
                    logger.info(f"✅ Parámetros obtenidos desde `dbutils.widgets`: {error_params}")

                except Exception as e:
                    error_params = {"error": f"Error al obtener parámetros de widgets: {str(e)}"}
                    logger.error(f"⚠️ No se pudieron obtener los parámetros desde `dbutils.widgets`: {e}")

            # Verificamos si algún parámetro numérico está vacío
            for key, value in error_params.items():
                if isinstance(value, str) and value.strip() == "":
                    logger.warning(f"⚠️ El parámetro `{key}` está vacío. Se asignará un valor por defecto.")
                    error_params[key] = None  # O asigna un valor válido como 0 si es adecuado para tu caso

        except Exception as e:
            error_params = {"error": f"Error al obtener parámetros: {str(e)}"}
            logger.error(f"⚠️ No se pudieron obtener los parámetros del notebook: {e}")
        
        # 🔹 En este punto ya tenemos los parámetros entonces podemos revaluar sr_subproceso y sr_subetapa y cambiar el process_param
        new_process = evaluate_process_by_subprocess(error_params, process_param)
        if new_process:
            logger.info(f"🔄 Cambiando process_param de '{process_param}' a '{new_process}' basado en subproceso/subetapa")
            process_param = new_process
        else:
            logger.info(f"ℹ️ No se encontró coincidencia, manteniendo process_param original: '{process_param}'")

    try:
        # 🔹 Enviar la notificación de error usando el "process" dinámico
        logger.info(f"🚨 Enviando notificación de error desde {notebook_name} con process: {process_param}...")
        #Notify.send_notification(process_param, error_params, is_error=True, notebook_name=notebook_name)
        Notify.send_notification(process_param, error_params, is_error=True, notebook_name=traceback_message)
    except Exception as e:
        logger.error(f"❌ No se pudo enviar la notificación de error: {e}")

def evaluate_process_by_subprocess(error_params, process_param):
    """
    Evalúa sr_subproceso y sr_subetapa para determinar el process_param correcto
    basado en la lógica de negocio específica.
    """
    try:
        sr_subproceso = error_params.get("sr_subproceso")
        sr_subetapa = error_params.get("sr_subetapa")

        if not sr_subproceso or not sr_subetapa:
            logger.warning(
                "⚠️ sr_subproceso o sr_subetapa no están disponibles en los parámetros"
            )
            return None

        logger.info(
            f"🔍 Evaluando: sr_subproceso={sr_subproceso}, sr_subetapa={sr_subetapa}"
        )

        subetap_tras = {"25", "4030"}
        subproc_tras = {"354","364","365","368","347","363","3286","348","350","349","3283","3832","3356"}

        if process_param == "VAL_STRUCT":
            # Lógica ISSSTE
            if (sr_subproceso in ("122", "120") and sr_subetapa != "832") or (sr_subproceso == "120" and sr_subetapa == "832"):
                return "VAL_ESTRUCT"
            # Lógica TRASPASOS (todos con sr_subetapa == "25")
            elif sr_subetapa in subetap_tras and sr_subproceso in subproc_tras:
                return "VAL_ESTRUCT_TRAS"
        elif process_param == "VAL_CONT":
            # Lógica ISSSTE
            if (sr_subproceso in ("122", "120") and sr_subetapa != "832") or (sr_subproceso == "120" and sr_subetapa == "832"):
                return "VAL_CONT"
            # Lógica TRASPASOS (todos con sr_subetapa == "25")
            elif sr_subetapa in subetap_tras and sr_subproceso in subproc_tras:
                return "VAL_CONT_TRAS"
            
        # NOTA: Hay que registrar los process_params en notification_handler.py y registar su variable de entorno global en settings services.

        logger.info(
            f"ℹ️ No se encontró mapeo específico para sr_subproceso={sr_subproceso}, sr_subetapa={sr_subetapa}"
        )
        return None

    except Exception as e:
        logger.error(f"❌ Error al evaluar subproceso/subetapa: {e}")
        return None

# Asignar el manejador de excepciones a nivel global en IPython
# InteractiveShell.instance().set_custom_exc((Exception, AnalysisException), custom_exception_handler)
InteractiveShell.instance().set_custom_exc((Exception, AnalysisException, SparkConnectException), custom_exception_handler)