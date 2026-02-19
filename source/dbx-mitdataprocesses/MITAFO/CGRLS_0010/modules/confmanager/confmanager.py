import os

from databricks.sdk.runtime import dbutils
from logger import logger


class ConfManager:
    """
    Clase para manejar la carga de archivos .env dentro de la carpeta 'Env'.

    Funcionalidades:
    - Carga automáticamente variables de los archivos .env en la carpeta 'Env'.
    - Convierte valores a tipos correctos (`bool`, `int`, `float`).
    - Permite acceder a variables como atributos (`conf.debug`).
    - Método `help()` para obtener la documentación.
    """

    def __init__(self):
        """
        Constructor de ConfManager. Carga automáticamente las variables de los archivos .env
        dentro de la carpeta 'Env'.
        """
        self.env_path = self._get_env_folder_path()
        self.env_variables = self._load_env_files()

    def _get_env_folder_path(self):
        """
        Obtiene la ruta de la carpeta Conf en Databricks.
        """
        try:
            notebook_path = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
        except Exception as e:
            raise RuntimeError(
                f"No se pudo obtener el path del notebook. Error: {str(e)}"
            )

        path_parts = notebook_path.split("/")
        if "Notebooks" not in path_parts:
            raise FileNotFoundError(
                f"La carpeta 'Notebooks' no se encontró en el path: {notebook_path}"
            )

        notebooks_index = path_parts.index("Notebooks")
        base_dir = "/".join(path_parts[:notebooks_index])  # Directorio raíz

        return f"/Workspace{base_dir}/Conf"

    def _load_env_files(self):
        """
        Carga todas las variables de los archivos .env en la carpeta 'Conf'.

        :return: Diccionario con las variables de entorno cargadas.
        """
        env_data = {}

        try:
            env_files = [f for f in os.listdir(self.env_path) if f.endswith(".env")]

            if not env_files:
                logger.warning(f"No se encontraron archivos .env en {self.env_path}")
                return env_data

            for file in env_files:
                file_path = os.path.join(self.env_path, file)
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith(
                            "#"
                        ):  # Ignorar líneas vacías o comentarios
                            continue
                        key, value = self._parse_env_line(line)
                        if key and value is not None:
                            env_data[key] = value

            logger.info(
                f"✅ Se cargaron {len(env_data)} variables de entorno desde {len(env_files)} archivos .env."
            )
        except Exception as e:
            logger.error(f"❌ Error al cargar archivos .env: {str(e)}")

        return env_data

    @staticmethod
    def _parse_env_line(line):
        """
        Procesa una línea del archivo .env y extrae la clave y el valor.
        Convierte automáticamente los valores en `bool`, `int`, `float` o `str`.

        :param line: Línea del archivo .env.
        :return: (clave, valor) limpios.
        """
        if "=" in line:
            key_value = line.split("=", 1)
        elif ":" in line:
            key_value = line.split(":", 1)
        else:
            logger.warning(f"⚠️ Línea mal formada en .env: {line}")
            return None, None

        key = key_value[0].strip()
        value = key_value[1].strip().strip('"').strip("'")  # Quitar comillas si existen

        # ✅ Conversión automática de tipo
        value = ConfManager._convert_value(value)

        return key, value

    @staticmethod
    def _convert_value(value):
        """
        Intenta convertir el valor en `bool`, `int`, `float` o `str`.

        :param value: Valor a convertir.
        :return: Valor convertido al tipo correcto.
        """
        # 🔹 Conversión a booleano (solo para valores explícitamente booleanos)
        if value.lower() in ["true", "yes", "on"]:
            return True
        if value.lower() in ["false", "no", "off"]:
            return False

        # 🔹 Conversión a numérico
        try:
            if "." in value:
                return float(value)  # Si tiene punto decimal, convertir a float
            return int(value)  # Si no tiene punto decimal, convertir a int
        except ValueError:
            pass  # Si no se puede convertir, devolver como string

        return value  # Mantener como string si no se pudo convertir

    def get(self, key, default=None):
        """
        Obtiene el valor de una variable de entorno.

        :param key: Clave de la variable.
        :param default: Valor por defecto si no se encuentra.
        :return: Valor de la variable o el valor por defecto.
        """
        return self.env_variables.get(key, default)

    def validate(self, required_keys):
        """
        Verifica que todas las claves requeridas estén presentes en las variables de entorno cargadas.

        :param required_keys: Lista de claves requeridas.
        :return: None, lanza excepción si falta alguna variable.
        """
        missing_keys = [key for key in required_keys if key not in self.env_variables]

        if missing_keys:
            raise ValueError(
                f"❌ Faltan las siguientes variables en los archivos .env: {', '.join(missing_keys)}"
            )

        logger.info(
            "✅ Todas las variables requeridas están presentes en los archivos .env."
        )

    def help(self):
        """
        Devuelve la documentación de la clase y sus métodos.
        """
        doc_string = """
        📌 **ConfManager - Gestión de Archivos .env**

        Métodos disponibles:
        - `get(key, default=None)`: Obtiene el valor de una variable de entorno.
        - `validate(required_keys)`: Verifica si ciertas variables están definidas.
        - `help()`: Muestra esta documentación.
        - `env_variables`: Devuelve un diccionario con todas las variables cargadas.
        - Acceso directo a variables: `conf.debug`, `conf.external_location`, etc.
        """
        return doc_string

    def __getattr__(self, key):
        """
        Permite acceder a las variables de entorno como atributos.
        """
        if key in self.env_variables:
            return self.env_variables[key]
        raise AttributeError(f"La variable de entorno '{key}' no existe.")

    def __repr__(self):
        """
        Representación de la instancia mostrando las variables cargadas.
        """
        return f"ConfManager({self.env_variables})"
