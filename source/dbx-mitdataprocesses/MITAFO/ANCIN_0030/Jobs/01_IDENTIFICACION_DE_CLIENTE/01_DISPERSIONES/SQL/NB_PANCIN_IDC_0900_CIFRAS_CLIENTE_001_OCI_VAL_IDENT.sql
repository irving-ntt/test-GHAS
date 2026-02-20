-- NB_PANCIN_IDC_0900_CIFRAS_CLIENTE_001_OCI_VAL_IDENT.sql
-- Propósito: Extraer validación de identificación de clientes desde Oracle
-- Tipo: OCI (Oracle)

SELECT /*+ ALL_ROWS */
    FTN_ESTATUS_DIAG,
    FTC_FOLIO
FROM #CX_CRE_ESQUEMA#.#TL_VAL_IDENT_CTE#
WHERE FTC_FOLIO = '#SR_FOLIO#'
