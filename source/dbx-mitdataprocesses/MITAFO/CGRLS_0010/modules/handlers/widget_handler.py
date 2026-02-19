import sys
from databricks.sdk.runtime import dbutils
from datetime import datetime

class WidgetParams:
    def __init__(self, param_types):
        """
        Carga los widgets existentes o los crea si no están definidos.
        :param param_types: Diccionario con los tipos esperados, ej:
            {
                "param1": int,
                "param2": float,
                "param3": bool,
                "param4": str,
                "fecha_inicio": (str, "YYYYMMDD")  # Validar formato, pero mantener como str
            }
        """
        self._param_types = param_types
        self._date_formats = {}  # Almacena formatos personalizados de fechas

        # Crear los widgets si no existen
        for key, expected_type in param_types.items():
            try:
                dbutils.widgets.get(key)
            except Exception:
                dbutils.widgets.text(key, "")  # Crear el widget vacío si no ha sido enviado en el Workflow

            # Si el tipo es una tupla (str, "YYYYMMDD"), almacenar el formato de fecha
            if isinstance(expected_type, tuple) and expected_type[0] == str:
                self._date_formats[key] = self._convert_date_format(expected_type[1])

        # 🔹 Obtener el nombre del notebook
        try:
            self.notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        except Exception:
            self.notebook_name = "Notebook_Desconocido"

        # 🔹 Generar clave única
        self.params_key = f"params_{self.notebook_name}"

        # 🔹 Guardar en `sys.modules`
        sys.modules[self.params_key] = self

    def _convert_date_format(self, format_str):
        """
        Convierte un formato de fecha tipo 'YYYYMMDD' a un formato compatible con datetime.strptime.
        """
        format_mappings = {
            "YYYY": "%Y",
            "MM": "%m",
            "DD": "%d",
            "HH": "%H",
            "mm": "%M",
            "SS": "%S"
        }
        for key, val in format_mappings.items():
            format_str = format_str.replace(key, val)
        return format_str

    def validate(self):
        """
        Valida que todos los widgets tengan valores y verifica si cumplen con los tipos de datos esperados.
        """
        errors = []

        for key, expected_type in self._param_types.items():
            value = dbutils.widgets.get(key).strip()

            # Validar si el widget está vacío
            if not value:
                errors.append(f"❌ Error: El parámetro '{key}' está vacío.")
                continue  # No se puede validar el tipo si está vacío

            # Validar tipo de dato
            try:
                if expected_type == int:
                    int(value)
                elif expected_type == float:
                    float(value)
                elif expected_type == bool:
                    if value.lower() not in ["true", "false", "1", "0"]:
                        raise ValueError(f"Debe ser booleano (true/false, 1/0).")
                elif isinstance(expected_type, tuple) and expected_type[0] == str:
                    date_format = self._date_formats[key]
                    datetime.strptime(value, date_format)  # Solo validar, no convertir
                elif expected_type == str:
                    pass  # Ya es string por defecto
                else:
                    raise ValueError(f"Tipo de dato no soportado: {expected_type}")
            except ValueError as e:
                errors.append(f"❌ Error: '{key}' debe cumplir con el formato {expected_type[1]}. {str(e)}")

        if errors:
            raise ValueError("\n".join(errors))

    def __getattr__(self, key):
        """Permite acceder a los parámetros como atributos, siempre en su tipo original."""
        if key in self._param_types:
            value = dbutils.widgets.get(key).strip()
            expected_type = self._param_types[key]

            # Si el valor está vacío, asignar un valor por defecto según el tipo esperado
            if value == "":
                if isinstance(expected_type, tuple) and expected_type[0] == str:
                    return "1"  # ✅ Devuelve "1" si es un string vacío, incluidas las fechas
                elif expected_type == str:
                    return "1"  # ✅ Para strings normales, devuelve "1"
                elif expected_type == int:
                    return 0  # Valor por defecto para enteros
                elif expected_type == float:
                    return 0.0  # Valor por defecto para flotantes
                elif expected_type == bool:
                    return False  # Valor por defecto para booleanos
                else:
                    return None  # Para otros casos, devolver None

            # ✅ Manejo especial para fechas (evita error de tipo no soportado)
            if isinstance(expected_type, tuple) and expected_type[0] == str:
                return value  # ✅ Se devuelve como string, sin intentar conversión

            # Conversión segura al tipo esperado
            try:
                if expected_type == int:
                    return int(value)
                elif expected_type == float:
                    return float(value)
                elif expected_type == bool:
                    return value.lower() in ["true", "1"]
                elif expected_type == str:
                    return value  # ✅ Si el valor ya está presente, devolverlo tal cual
                else:
                    raise AttributeError(f"Tipo no soportado para '{key}'.")
            except ValueError:
                raise ValueError(f"⚠️ Error al convertir el parámetro `{key}`: '{value}' no es un {expected_type} válido.")

        raise AttributeError(f"Parámetro '{key}' no definido.")

    def to_dict(self):
        """Convierte los parámetros en un diccionario con valores en su tipo correspondiente."""
        return {key: getattr(self, key) for key in self._param_types}