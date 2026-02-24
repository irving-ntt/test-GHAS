-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_006A_DELETE_PREVIOUS.sql
-- =====================================================================================
-- Eliminar registros previos por folio y subproceso
-- Se ejecuta con db.execute_oci_dml()
-- =====================================================================================

DELETE FROM CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS 
WHERE FTC_FOLIO = '#SR_FOLIO#' 
AND FCN_ID_SUBPROCESO = #SR_SUBPROCESO# 