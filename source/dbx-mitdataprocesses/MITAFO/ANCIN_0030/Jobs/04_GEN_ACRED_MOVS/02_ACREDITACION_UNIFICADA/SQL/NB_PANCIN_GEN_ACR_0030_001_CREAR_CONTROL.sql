-- Crear tabla de control de archivos PANCIN si no existe
-- Equivalente al archivo físico 3281_{SR_FOLIO}.txt

CREATE TABLE IF NOT EXISTS #CATALOG#.#SCHEMA#.CONTROL_ARCHIVOS_PANCIN (
    SR_FOLIO STRING NOT NULL,
    SR_SUBPROCESO STRING,
    SR_SUBETAPA STRING,
    FECHA_CREACION TIMESTAMP,
    FECHA_ACTUALIZACION TIMESTAMP,
    ESTADO STRING
)
USING DELTA
COMMENT 'Tabla de control de archivos PANCIN - Reemplaza archivos físicos 3281_{SR_FOLIO}.txt'