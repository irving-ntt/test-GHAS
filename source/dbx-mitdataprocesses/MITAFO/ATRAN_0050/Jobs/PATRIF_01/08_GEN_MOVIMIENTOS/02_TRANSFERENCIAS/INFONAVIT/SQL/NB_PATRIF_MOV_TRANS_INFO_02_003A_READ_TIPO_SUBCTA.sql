-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_003A_READ_TIPO_SUBCTA.sql
-- =====================================================================================
-- Consulta para traer datos de Oracle: TCCRXGRAL_TIPO_SUBCTA
-- Se ejecuta con db.read_data("oracle", query)
-- =====================================================================================

SELECT 
    FCN_ID_TIPO_SUBCTA,
    FCN_ID_CAT_SUBCTA,
    FCN_ID_PLAZO
FROM CIERREN.TCCRXGRAL_TIPO_SUBCTA 