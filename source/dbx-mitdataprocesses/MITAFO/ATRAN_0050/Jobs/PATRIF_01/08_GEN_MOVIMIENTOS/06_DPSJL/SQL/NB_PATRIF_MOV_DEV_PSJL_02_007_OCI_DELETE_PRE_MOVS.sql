-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_02_007_OCI_DELETE_PRE_MOVS.sql
-- =============================================================================
-- Propósito: Eliminar registros previos de pre-movimientos (BeforeSQL)
-- Tipo: OCI (Oracle DML - DELETE)
-- Stage Original: DB_900_ETL_PRE_MOVS (BeforeSQL)
-- =============================================================================

DELETE FROM CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS 
WHERE FTC_FOLIO = '#SR_FOLIO#' 
  AND FCN_ID_SUBPROCESO = #SR_SUBPROCESO#

