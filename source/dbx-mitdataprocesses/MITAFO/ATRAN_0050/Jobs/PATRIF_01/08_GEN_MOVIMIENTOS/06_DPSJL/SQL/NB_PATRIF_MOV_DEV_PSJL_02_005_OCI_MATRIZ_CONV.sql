-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_02_005_OCI_MATRIZ_CONV.sql
-- =============================================================================
-- Propósito: Extraer información de matriz de convivencia (marcas)
-- Tipo: OCI (Oracle)
-- Stage Original: DB_700_MATRIZ_CONV
-- =============================================================================

SELECT
    FTN_ID_MARCA,
    FTC_FOLIO,
    FTN_NUM_CTA_INVDUAL,
    FRN_ID_MOV_SUBCTA
FROM #CX_CRN_ESQUEMA#.#TL_CRN_MATRIZ_CONV#
WHERE FTC_FOLIO = '#SR_FOLIO#'
  AND FTB_ESTATUS_MARCA = 1
