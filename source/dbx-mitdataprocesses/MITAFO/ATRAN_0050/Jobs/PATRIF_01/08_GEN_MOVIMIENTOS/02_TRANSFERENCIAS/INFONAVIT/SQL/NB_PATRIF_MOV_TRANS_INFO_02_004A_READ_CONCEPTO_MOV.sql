-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_004A_READ_CONCEPTO_MOV.sql
-- =====================================================================================
-- Consulta para traer datos de Oracle: Concepto de Movimiento
-- Se ejecuta con db.read_data("oracle", query)
-- =====================================================================================

SELECT 
    MS.FRN_ID_MOV_SUBCTA,
    MS.FCN_ID_TIPO_SUBCTA,
    CM.FFN_ID_CONCEPTO_MOV,
    CM.FTN_DEDUCIBLE
FROM CIERREN.TRAFOGRAL_MOV_SUBCTA MS 
INNER JOIN CIERREN.TFAFOGRAL_CONFIG_CONCEP_MOV CM
ON MS.FRN_ID_MOV_SUBCTA = CM.FRN_ID_MOV_SUBCTA
WHERE MS.FCN_ID_SUBPROCESO = #SR_SUBPROCESO# 