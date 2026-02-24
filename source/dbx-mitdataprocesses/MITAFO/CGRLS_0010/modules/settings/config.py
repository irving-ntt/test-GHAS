"""Configuraciones del módulo para manejar las variables de configuración."""

from .general import GeneralSettings
from .conn import ConnSettings
from .services import ServicesSettings

class Settings:
    def __init__(self):
        self.GENERAL = GeneralSettings()
        self.CONN = ConnSettings()
        self.SERVICES = ServicesSettings()

SETTINGS = Settings()