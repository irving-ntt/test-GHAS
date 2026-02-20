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
        search_path = "/".join(path_parts[: base_index + 1])

        for root, dirs, _ in os.walk(search_path):
            if target_dir in dirs:
                target_path = os.path.join(root, target_dir, sub_dir)
                if os.path.exists(target_path):
                    return target_path  # Retorna la ruta completa

    return ""  # Retorna cadena vacía si no se encuentra el directorio


class GeneralSettings:
    def __init__(self, env_file=f"{get_target_path()}/general.env"):
        self.vars = self._load_env(env_file)
        self.CATALOG = self.vars.get("CATALOG", "")
        self.SCHEMA = self.vars.get("SCHEMA", "")
        self.SCOPE = self.vars.get("SCOPE", "")
        self.CONN_USER = self.vars.get("CONN_USER", "")
        self.CONN_KEY = self.vars.get("CONN_KEY", "")
        self.EXTERNAL_LOCATION = self.vars.get("EXTERNAL_LOCATION", "")
        self.ROOT_REPO = self.vars.get("ROOT_REPO", "")
        self.PROCESS_USER = self.vars.get("PROCESS_USER", "")
        self.PATH_PROCESAR = self.vars.get("PATH_PROCESAR", "")
        self.PATH_RCDI = self.vars.get("PATH_RCDI", "")
        self.PATH_RCTRAS = self.vars.get("PATH_RCTRAS", "")
        self.PATH_INTEGRITY = self.vars.get("PATH_INTEGRITY", "")
        self.PATH_INF = self.vars.get("PATH_INF", "")

        # 🔹 NUEVAS CONFIGURACIONES PARA execute_oci_dml
        # Configuración de Databricks
        self.DATABRICKS_INSTANCE = self.vars.get("DATABRICKS_INSTANCE", "")
        self.DATABRICKS_CLUSTER_ID = self.vars.get("DATABRICKS_CLUSTER_ID", "")

        # Configuración del Job asíncrono
        self.OCI_DML_JOB_NAME = self.vars.get("OCI_DML_JOB_NAME", "")
        self.OCI_DML_MAX_CONCURRENT_RUNS = int(
            self.vars.get("OCI_DML_MAX_CONCURRENT_RUNS", "0")
        )
        self.OCI_DML_TIMEOUT_SECONDS = int(
            self.vars.get("OCI_DML_TIMEOUT_SECONDS", "0")
        )
        self.OCI_DML_MAX_RETRIES = int(self.vars.get("OCI_DML_MAX_RETRIES", "0"))
        self.OCI_DML_RETRY_INTERVAL_MS = int(
            self.vars.get("OCI_DML_RETRY_INTERVAL_MS", "0")
        )

        # Configuración de permisos del job
        self.OCI_DML_JOB_PERMISSION_LEVEL = self.vars.get(
            "OCI_DML_JOB_PERMISSION_LEVEL", ""
        )
        self.OCI_DML_JOB_GROUP_NAME = self.vars.get("OCI_DML_JOB_GROUP_NAME", "")

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
