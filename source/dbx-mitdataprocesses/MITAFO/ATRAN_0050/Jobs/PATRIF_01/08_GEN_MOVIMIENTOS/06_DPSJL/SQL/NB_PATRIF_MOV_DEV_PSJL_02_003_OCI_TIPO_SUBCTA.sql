-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_02_003_OCI_TIPO_SUBCTA.sql
-- =============================================================================
-- Propósito: Extraer catálogo de tipos de subcuenta
-- Tipo: OCI (Oracle)
-- Stage Original: DB_400_TIPO_SUBCTA
-- =============================================================================

SELECT 
    FCN_ID_TIPO_SUBCTA,
    FCN_ID_CAT_SUBCTA,
    FCN_ID_PLAZO
FROM #CX_CRN_ESQUEMA#.#TL_CRN_TIPO_SUBCTA#

