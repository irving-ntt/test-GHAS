-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_02_002_OCI_VALOR_ACCION.sql
-- =============================================================================
-- Propósito: Extraer catálogo de valores de acción
-- Tipo: OCI (Oracle)
-- Stage Original: DB_300_VALOR_ACCION
-- =============================================================================

SELECT 
    FCN_ID_VALOR_ACCION,
    FCD_FEH_ACCION
FROM #CX_CRN_ESQUEMA#.#TL_CRN_VALOR_ACCION#

