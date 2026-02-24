-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_02_004_OCI_MOV_SUBCTA.sql
-- =============================================================================
-- Propósito: Extraer configuración de movimientos de subcuenta con deducibilidad
-- Tipo: OCI (Oracle)
-- Stage Original: DB_500_MOV_SUBCTA
-- =============================================================================

SELECT 
    DISTINCT
    MS.FRN_ID_MOV_SUBCTA,
    MS.FCN_ID_TIPO_SUBCTA,
    CM.FTN_DEDUCIBLE
FROM #CX_CRN_ESQUEMA#.#TL_CRN_MOV_SUBCTA# MS 
INNER JOIN #CX_CRN_ESQUEMA#.#TL_CRN_CONFIG_CONCEP_MOV# CM
    ON MS.FRN_ID_MOV_SUBCTA = CM.FRN_ID_MOV_SUBCTA
WHERE MS.FCN_ID_SUBPROCESO = #SR_SUBPROCESO#

