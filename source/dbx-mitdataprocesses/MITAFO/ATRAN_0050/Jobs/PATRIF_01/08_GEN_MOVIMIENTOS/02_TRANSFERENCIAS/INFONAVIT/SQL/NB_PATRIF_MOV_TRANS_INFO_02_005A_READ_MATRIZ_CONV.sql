-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_005A_READ_MATRIZ_CONV.sql
-- =====================================================================================
-- Consulta para traer datos de Oracle: Matriz de Convivencia
-- Se ejecuta con db.read_data("oracle", query)
-- =====================================================================================

SELECT
    M.FTN_ID_MARCA,
    M.FTC_FOLIO,
    M.FTN_NUM_CTA_INVDUAL,
    M.FRN_ID_MOV_SUBCTA,
    SUB.FCN_ID_TIPO_SUBCTA
FROM CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA M 
INNER JOIN CIERREN.TRAFOGRAL_MOV_SUBCTA SUB
ON M.FRN_ID_MOV_SUBCTA = SUB.FRN_ID_MOV_SUBCTA
WHERE M.FTC_FOLIO = '#SR_FOLIO#'
AND M.FTB_ESTATUS_MARCA = 1
AND SUB.FCN_ID_SUBPROCESO = #SR_SUBPROCESO# 