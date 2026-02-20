"""Definiciones generales"""

import os

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

class ServicesSettings:
    def __init__(self, env_file=f"{get_target_path()}/services.env"):
        self.vars = self._load_env(env_file)
        self.WEBHOOK_URL_VAL_STR = self.vars.get("WEBHOOK_URL_VAL_STR", "")
        self.WEBHOOK_URL_CARG_ARCH = self.vars.get("WEBHOOK_URL_CARG_ARCH", "")
        self.WEBHOOK_URL_INTR_CONT = self.vars.get("WEBHOOK_URL_INTR_CONT", "")
        self.WEBHOOK_URL_DEFAULT = self.vars.get("WEBHOOK_URL_DEFAULT", "")
        self.WEBHOOK_URL_VAL_CONT = self.vars.get("WEBHOOK_URL_VAL_CONT", "")
        self.SCOPE_NOTIFICATIONS = self.vars.get("SCOPE_NOTIFICATIONS", "")
        self.WEBHOOK_URL_VAL_STR_TRAS = self.vars.get("WEBHOOK_URL_VAL_STR_TRAS", "")
        self.WEBHOOK_URL_VAL_CONT_TRAS = self.vars.get("WEBHOOK_URL_VAL_CONT_TRAS", "")


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