import json
import requests
from logger import logger
from settings import SETTINGS
from databricks.sdk.runtime import dbutils
from .widget_handler import WidgetParams

class Notify:

    @staticmethod
    def _get_notification_url(process):
        urls = {
            'VAL_ESTRUCT': SETTINGS.SERVICES.WEBHOOK_URL_VAL_STR,
            'VAL_ESTRUCT_TRAS': SETTINGS.SERVICES.WEBHOOK_URL_VAL_STR_TRAS,
            'CARG_ARCH': SETTINGS.SERVICES.WEBHOOK_URL_CARG_ARCH,
            'INTER_CONT': SETTINGS.SERVICES.WEBHOOK_URL_INTR_CONT,
            'VAL_CONT': SETTINGS.SERVICES.WEBHOOK_URL_VAL_CONT,
            'VAL_CONT_TRAS': SETTINGS.SERVICES.WEBHOOK_URL_VAL_CONT_TRAS,
        }
        return urls.get(process, SETTINGS.SERVICES.WEBHOOK_URL_DEFAULT)

    @staticmethod
    def _obtener_token():
        """Obtiene el token de autenticación"""
        scope = SETTINGS.GENERAL.SCOPE
        username = dbutils.secrets.get(scope=scope, key='dbxServicesUser')
        password = dbutils.secrets.get(scope=scope, key='dbxServicesPassword')
        url = SETTINGS.SERVICES.SCOPE_NOTIFICATIONS.strip().strip('"')  # Asegurar que la URL está limpia
        payload = {'grant_type': 'client_credentials'}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        try:
            response = requests.post(url, headers=headers, data=payload, auth=(username, password))
            response.raise_for_status()
            return response.json().get("access_token")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error obteniendo token: {e}")
            return None

    @staticmethod
    def send_notification(process, messages, is_error=False, notebook_name=""):
        """Envía una notificación de éxito o error según el valor de is_error"""
        webhook_url = Notify._get_notification_url(process)

        # 🔹 Convertir `messages` a diccionario si es una instancia de `WidgetParams`
        if isinstance(messages, WidgetParams):
            messages = messages.to_dict()
            logger.info("✅ `messages` convertido a diccionario con .to_dict()")

        # 🔹 Verificar si `messages` es un diccionario después de la conversión
        if not isinstance(messages, dict):
            logger.error(f"❌ Formato incorrecto de messages. Tipo recibido: {type(messages)}")
            return None

        # 🔹 Reemplazar valores vacíos por "0"
        messages = {k: ("0" if v == "" else v) for k, v in messages.items()}

        # 🔹 Si `messages` está vacío, asignar {"parameters": "null"}
        if not messages:
            messages = {"parameters": "empty"}

        # 🔹 Determinar código y mensaje según si es error o éxito
        payload = {
            "code": -1 if is_error else 0,
            "message": notebook_name if is_error else "DONE",
            "source": "ETL",
            "additionalParameters": messages
        }

        token = Notify._obtener_token()
        if not token:
            logger.error("❌ No se pudo obtener el token, abortando solicitud.")
            return None

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        logger.info(f"🚀 Webhook URL: {webhook_url}")
        logger.info(f"📦 Payload ({'Error' if is_error else 'Éxito'}): {json.dumps(payload, indent=2)}")

        # 🔹 Enviar la petición
        response = requests.post(webhook_url, json=payload, headers=headers)

        logger.info(f"✅ Response: {response.status_code} - {response.text}")
        if response.status_code not in [200, 204]:
            raise ValueError(f"❌ Error enviando notificación: {response.text}")

        return response