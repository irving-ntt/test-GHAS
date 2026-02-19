-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_005B_JOIN_MATRIZ_CONV.sql
-- =====================================================================================
-- Join con matriz de convivencia (solo Delta)
-- Se ejecuta con db.sql_delta(query)
-- =====================================================================================

SELECT 
    lcm.FTN_NUM_CTA_INVDUAL,
    lcm.FTN_IND_SDO_DISP,
    lcm.FTN_SDO_AIVS,
    lcm.FTN_VALOR_AIVS,
    lcm.FTN_MONTO_PESOS,
    lcm.FCN_ID_TIPO_SUBCTA,
    lcm.FTN_NUM_APLI_INTE_VIV,
    lcm.FTN_SALDO_VIV,
    lcm.FCN_ESTATUS,
    lcm.FTC_FOLIO,
    lcm.FCN_ID_REGIMEN,
    lcm.FTN_ID_SUBP,
    lcm.FTN_CONTA_SERV,
    lcm.FCD_FEH_ACCION,
    lcm.FTN_MOTI_RECH,
    lcm.FCN_ID_CAT_SUBCTA,
    mc.FTN_ID_MARCA,
    lcm.FCN_ID_PLAZO,
    lcm.FFN_ID_CONCEPTO_MOV,
    lcm.FTN_DEDUCIBLE,
    lcm.FCN_VALOR_ACCION
FROM #CATALOG_SCHEMA#.NB_PATRIF_MOV_TRANS_INFO_02_004_#SR_FOLIO# lcm
INNER JOIN #CATALOG_SCHEMA#.TEMP_MATRIZ_CONV_#SR_FOLIO# mc
ON lcm.FTC_FOLIO = mc.FTC_FOLIO
AND lcm.FTN_NUM_CTA_INVDUAL = mc.FTN_NUM_CTA_INVDUAL