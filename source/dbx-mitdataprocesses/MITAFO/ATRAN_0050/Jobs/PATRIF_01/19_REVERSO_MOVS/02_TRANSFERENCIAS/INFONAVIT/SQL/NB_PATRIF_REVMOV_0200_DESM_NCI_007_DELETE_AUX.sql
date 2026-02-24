-- NB_PATRIF_REVMOV_0200_DESM_NCI_007_DELETE_AUX.sql
-- Propósito: Limpiar registros específicos de tabla auxiliar después del MERGE
-- Tipo: OCI (Oracle) - DML DELETE

DELETE FROM #CX_DATAUX_ESQUEMA#.#TL_DATAUX_MATRIZ_CONV_AUX#
WHERE FTC_FOLIO = #SR_FOLIO#

