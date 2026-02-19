-- Insertar registro de control para el folio especificado
-- Equivalente al archivo físico 3281_{SR_FOLIO}.txt

WITH s AS (
  SELECT
    '#SR_FOLIO#'       AS SR_FOLIO,
    '#SR_SUBPROCESO#'  AS SR_SUBPROCESO,
    '#SR_SUBETAPA#'    AS SR_SUBETAPA,
    from_utc_timestamp(current_timestamp(), 'America/Mexico_City') AS ts_now
)
MERGE INTO #CATALOG#.#SCHEMA#.CONTROL_ARCHIVOS_PANCIN t
USING s
ON t.SR_FOLIO = s.SR_FOLIO
WHEN MATCHED THEN UPDATE SET
  t.SR_SUBPROCESO       = s.SR_SUBPROCESO,
  t.SR_SUBETAPA         = s.SR_SUBETAPA,
  t.FECHA_ACTUALIZACION = s.ts_now,
  t.ESTADO              = 'ACTIVO'
WHEN NOT MATCHED THEN INSERT (
  SR_FOLIO, SR_SUBPROCESO, SR_SUBETAPA,
  FECHA_CREACION, FECHA_ACTUALIZACION, ESTADO
) VALUES (
  s.SR_FOLIO, s.SR_SUBPROCESO, s.SR_SUBETAPA,
  s.ts_now,   s.ts_now,        'ACTIVO'
);