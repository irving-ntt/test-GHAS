-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_002B_JOIN_VALOR_ACCION.sql
-- =====================================================================================
-- Join entre saldos y tabla de valor de acción (solo Delta)
-- Se ejecuta con db.sql_delta(query)
-- =====================================================================================

SELECT 
    s.FTN_NUM_CTA_INVDUAL,
    s.FTN_IND_SDO_DISP,
    s.FTN_SDO_AIVS,
    s.FTN_VALOR_AIVS,
    s.FTN_MONTO_PESOS,
    s.FCN_ID_TIPO_SUBCTA,
    s.FTN_NUM_APLI_INTE_VIV,
    s.FTN_SALDO_VIV,
    s.FCN_ESTATUS,
    s.FTC_FOLIO,
    s.FCN_ID_REGIMEN,
    s.FTN_ID_SUBP,
    s.FTN_CONTA_SERV,
    s.FTN_MOTI_RECH,
    s.FTD_FEH_CRE,
    s.FTC_USU_CRE,
    va.FCD_FEH_ACCION,
    va.FCN_VALOR_ACCION
FROM #CATALOG_SCHEMA#.NB_PATRIF_MOV_TRANS_INFO_02_001_#SR_FOLIO# s
INNER JOIN #CATALOG_SCHEMA#.TEMP_VALOR_ACCION_#SR_FOLIO# va
ON s.FCN_ID_VALOR_ACCION = va.FCN_ID_VALOR_ACCION 