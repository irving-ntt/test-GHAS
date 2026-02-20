-- =============================================================================
-- NB_PATRIF_GMO_0100_DEV_SALD_EXC_002_OCI_VALOR_ACCION.sql
-- =============================================================================
-- Propósito: Extraer valores de acción para lookup desde Oracle
-- Tipo: OCI (Oracle)
-- Stage Original: DB_200_VALOR_ACCION
-- =============================================================================

SELECT 
    FCN_ID_VALOR_ACCION,
    FCD_FEH_ACCION,
    FCN_VALOR_ACCION
FROM #CX_CRN_ESQUEMA#.#TL_CRN_VALOR_ACCION#
WHERE FCN_ID_SIEFORE = #FCN_ID_SIEFORE#
  AND FCN_ID_REGIMEN = #FCN_ID_REGIMEN#

