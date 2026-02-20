-- NB_PATRIF_REVMOV_0300_ARCH_INGTY_006_RESPUESTA_DELETE.sql
-- ========================================================================
-- Propósito: Eliminar registro anterior de respuesta Integrity
-- ========================================================================
-- Stage Original: DB_500_RESPUESTA_ITGY (BeforeSQL)
-- ========================================================================

DELETE FROM #CX_PRO_ESQUEMA#.#TL_PRO_RESPUESTA_ITGY#
WHERE FTC_FOLIO = '#SR_FOLIO#'

