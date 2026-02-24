-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_002A_READ_VALOR_ACCION.sql
-- =====================================================================================
-- Consulta para traer datos de Oracle: TCAFOGRAL_VALOR_ACCION
-- Se ejecuta con db.read_data("oracle", query)
-- =====================================================================================

SELECT 
    FCN_ID_VALOR_ACCION,
    FCD_FEH_ACCION,
    FCN_VALOR_ACCION
FROM CIERREN.TCAFOGRAL_VALOR_ACCION 