-- ==========================================
-- OCI: EXTRACCIÓN DE CIFRAS DE CONTROL (CONTEOS DE SEMÁFOROS)
-- ==========================================
-- Parámetros esperados:
--   #CX_CRE_ESQUEMA#
--   #TL_CRE_MOVIMIENTOS#
--   #SR_FOLIO#
-- ==========================================

SELECT SUM(semaforo_verde) AS COUNT_VERDE,
       SUM(semaforo_rojo)  AS COUNT_ROJO,
       SUM(semaforo_naranja) AS COUNT_NARANJA
FROM (
  SELECT 
    CASE WHEN FTN_ID_SEMAFORO_MOV = 190 THEN 1 ELSE 0 END AS semaforo_verde,
    CASE WHEN FTN_ID_SEMAFORO_MOV = 191 THEN 1 ELSE 0 END AS semaforo_rojo,
    CASE WHEN FTN_ID_SEMAFORO_MOV = 192 THEN 1 ELSE 0 END AS semaforo_naranja
  FROM #CX_CRE_ESQUEMA#.#TL_CRE_MOVIMIENTOS#
  WHERE FTC_FOLIO = '#SR_FOLIO#'
  UNION ALL
  SELECT 
    CASE WHEN FTN_ID_SEMAFORO_MOV = 190 THEN 1 ELSE 0 END AS semaforo_verde,
    CASE WHEN FTN_ID_SEMAFORO_MOV = 191 THEN 1 ELSE 0 END AS semaforo_rojo,
    CASE WHEN FTN_ID_SEMAFORO_MOV = 192 THEN 1 ELSE 0 END AS semaforo_naranja
  FROM #CX_CRE_ESQUEMA#.#TL_CRE_MOVIMIENTOS#
  WHERE FTC_FOLIO_REL = '#SR_FOLIO#'
) 