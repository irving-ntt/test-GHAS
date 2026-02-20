import os
import json
import logging

# Configurar logger para imprimir a consola sin escribir en archivos
logging.basicConfig(
    level=logging.INFO,  # o WARNING si prefieres
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_target_path(base_dir="MITAFO", target_dir="CGRLS_0010", sub_dir="Conf"):
    """
    Busca la ruta completa del directorio target_dir dentro de base_dir y retorna la ruta completa.
    No modifica sys.path.
    
    Args:
        base_dir (str): Directorio base donde se buscará target_dir.
        target_dir (str): Nombre del directorio a encontrar.
        sub_dir (str): Subdirectorio opcional dentro del target_dir (ej. "modules").
    
    Returns:
        str: Ruta completa si se encuentra, o cadena vacía "" si no existe.
    """
    current_path = os.getcwd()
    path_parts = current_path.split("/")
    
    if base_dir in path_parts:
        base_index = path_parts.index(base_dir)
        search_path = "/".join(path_parts[:base_index + 1])

        for root, dirs, _ in os.walk(search_path):
            if target_dir in dirs:
                target_path = os.path.join(root, target_dir, sub_dir)
                if os.path.exists(target_path):
                    return target_path  # Retorna la ruta completa
        
    return ""  # Retorna cadena vacía si no se encuentra el directorio

class ConnSettings:
    def __init__(self, env_file=f"{get_target_path()}/conn.env"):
        self.vars = self._load_env(env_file)

        # 🚀 Intentar convertir CONN_OPTIONS y CONN_ADITIONAL_OPTIONS en JSON válidos
        self.CONN_OPTIONS = self._parse_json(self.vars.get("CONN_OPTIONS", "{}"))
        self.CONN_ADITIONAL_OPTIONS = self._parse_json(self.vars.get("CONN_ADITIONAL_OPTIONS", "{}"))

    def _load_env(self, filepath):
        """Carga las variables del archivo .env en un diccionario sin exportarlas al entorno."""
        env_vars = {}
        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                # Ignorar líneas vacías y comentarios
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    env_vars[key.strip()] = value.strip()
        return env_vars

    def _parse_json(self, json_str):
        """Convierte una cadena JSON en un diccionario, manejando errores sin exponer detalles sensibles."""
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            logger.warning("Error al procesar configuración: el formato JSON recibido no es válido.")
            return {}