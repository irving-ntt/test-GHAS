-- NB_PATRIF_REVMOV_0400_DESACT_INDIC_004_CLEANUP_AUX.sql
-- ========================================================================
-- Propósito: Limpiar tabla auxiliar después del MERGE
-- ========================================================================

DELETE FROM #CX_DATAUX_ESQUEMA#.TTAFOGRAL_IND_CTA_INDV_AUX
WHERE FTC_FOLIO = '#SR_FOLIO#'

