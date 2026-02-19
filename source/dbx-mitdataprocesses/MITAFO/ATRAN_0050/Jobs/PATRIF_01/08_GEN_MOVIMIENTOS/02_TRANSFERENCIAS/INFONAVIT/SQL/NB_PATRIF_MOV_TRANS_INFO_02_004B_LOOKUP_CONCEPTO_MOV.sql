-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_004B_LOOKUP_CONCEPTO_MOV.sql
-- =====================================================================================
-- Lookup con configuración de concepto de movimiento (solo Delta)
-- Se ejecuta con db.sql_delta(query)
-- =====================================================================================

SELECT 
    lts.FTN_NUM_CTA_INVDUAL,
    lts.FTN_IND_SDO_DISP,
    lts.FTN_SDO_AIVS,
    lts.FTN_VALOR_AIVS,
    lts.FTN_MONTO_PESOS,
    lts.FCN_ID_TIPO_SUBCTA,
    lts.FTN_NUM_APLI_INTE_VIV,
    lts.FTN_SALDO_VIV,
    lts.FCN_ESTATUS,
    lts.FTC_FOLIO,
    lts.FCN_ID_REGIMEN,
    lts.FTN_ID_SUBP,
    lts.FTN_CONTA_SERV,
    lts.FTN_MOTI_RECH,
    lts.FTD_FEH_CRE,
    lts.FTC_USU_CRE,
    lts.FCD_FEH_ACCION,
    lts.FCN_ID_CAT_SUBCTA,
    lts.FCN_ID_PLAZO,
    cm.FFN_ID_CONCEPTO_MOV,
    cm.FTN_DEDUCIBLE,
    lts.FCN_VALOR_ACCION
FROM #CATALOG_SCHEMA#.NB_PATRIF_MOV_TRANS_INFO_02_003_#SR_FOLIO# lts
LEFT JOIN #CATALOG_SCHEMA#.TEMP_CONCEPTO_MOV_#SR_FOLIO# cm
ON lts.FCN_ID_TIPO_SUBCTA = cm.FCN_ID_TIPO_SUBCTA 