-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_003B_LOOKUP_TIPO_SUBCTA.sql
-- =====================================================================================
-- Lookup con tabla de tipo de subcuenta (solo Delta)
-- Se ejecuta con db.sql_delta(query)
-- =====================================================================================

SELECT 
    jva.FTN_NUM_CTA_INVDUAL,
    jva.FTN_IND_SDO_DISP,
    jva.FTN_SDO_AIVS,
    jva.FTN_VALOR_AIVS,
    jva.FTN_MONTO_PESOS,
    jva.FCN_ID_TIPO_SUBCTA,
    jva.FTN_NUM_APLI_INTE_VIV,
    jva.FTN_SALDO_VIV,
    jva.FCN_ESTATUS,
    jva.FTC_FOLIO,
    jva.FCN_ID_REGIMEN,
    jva.FTN_ID_SUBP,
    jva.FTN_CONTA_SERV,
    jva.FTN_MOTI_RECH,
    jva.FTD_FEH_CRE,
    jva.FTC_USU_CRE,
    jva.FCD_FEH_ACCION,
    ts.FCN_ID_CAT_SUBCTA,
    ts.FCN_ID_PLAZO,
    jva.FCN_VALOR_ACCION
FROM #CATALOG_SCHEMA#.NB_PATRIF_MOV_TRANS_INFO_02_002_#SR_FOLIO# jva
LEFT JOIN #CATALOG_SCHEMA#.TEMP_TIPO_SUBCTA_#SR_FOLIO# ts
ON jva.FCN_ID_TIPO_SUBCTA = ts.FCN_ID_TIPO_SUBCTA; 