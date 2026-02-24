-- NB_PATRIF_REVMOV_0500_REV_MOVS_003_EXTRACT_FULL.sql
-- ========================================================================
-- Propósito: Extraer registros completos de Pre-Movimientos para actualizar
-- ========================================================================
-- NOTA: Se extraen TODOS los campos de los registros que se van a actualizar
--       para no perder información al hacer DELETE + INSERT
-- ========================================================================

SELECT *
FROM #CX_CRE_ESQUEMA#.#TL_PRE_MOVIMIENTOS#
WHERE FTC_FOLIO = '#SR_FOLIO#'

