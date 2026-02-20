-- NB_PATRIF_REVMOV_0500_REV_MOVS_002_DELETE.sql
-- ========================================================================
-- Propósito: Eliminar registros previos antes del INSERT
-- ========================================================================
-- NOTA: En DataStage original se usa UPDATE (WriteMode=1)
--       En Databricks usamos DELETE + INSERT para simplicidad
--       Si se requiere UPDATE, usar tabla AUX con MERGE (como en job 0400)
-- ========================================================================

DELETE FROM #CX_CRE_ESQUEMA#.#TL_PRE_MOVIMIENTOS#
WHERE FTC_FOLIO = '#SR_FOLIO#'
