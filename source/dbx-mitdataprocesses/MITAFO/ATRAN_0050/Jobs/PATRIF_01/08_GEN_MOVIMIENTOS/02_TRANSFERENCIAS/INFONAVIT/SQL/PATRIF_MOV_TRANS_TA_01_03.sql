-- =============================================================================
-- QUERY 3: Generar el resultado final sumando los intereses
-- (Resultado: DELTA_MOV_TRANS_RESULTADO)
-- Este query une los datos de saldos (Delta) con los intereses (OCI) y calcula el resultado final
-- =============================================================================

SELECT
    COALESCE(a.FTN_NUM_CTA_INVDUAL, 0) AS FTN_NUM_CTA_INVDUAL,
    a.FTN_IND_SDO_DISP,
    a.FTN_SDO_AIVS,
    a.FTN_VALOR_AIVS,
    CASE
        WHEN a.tipo_subcta = 15 THEN COALESCE(a.FTN_MONTO_PESOS, 0) + COALESCE(b.FTN_INTER_SALDO_VIVI97_ULT, 0)
        WHEN a.tipo_subcta = 16 THEN COALESCE(a.FTN_MONTO_PESOS, 0) + COALESCE(b.FTN_INTER_SALDO_VIVI92, 0)
        ELSE COALESCE(a.FTN_MONTO_PESOS, 0)
    END AS FTN_MONTO_PESOS,
    a.FCN_ID_TIPO_SUBCTA,
    a.FTN_NUM_APLI_INTE_VIV,
    a.FTN_SALDO_VIV,
    COALESCE(a.FTC_FOLIO, '0') AS FTC_FOLIO,
    a.FCN_ESTATUS,
    a.FCN_ID_REGIMEN,
    a.FTN_ID_SUBP,
    a.FCN_ID_VALOR_ACCION,
    a.FTN_CONTA_SERV,
    a.FTN_MOTI_RECH,
    a.FTD_FEH_CRE,
    a.FTC_USU_CRE
FROM #CATALOG_SCHEMA#.#DELTA_TMP_MOV_TRANS_SALDOS# a
LEFT JOIN #CATALOG_SCHEMA#.#DELTA_TMP_MOV_TRANS_CON_INTERESES# b
  ON a.FTC_FOLIO = b.FTC_FOLIO_BITACORA
 AND a.FTN_NUM_CTA_INVDUAL = b.FTN_NUM_CTA_INVDUAL
 AND a.FTN_CONTA_SERV = b.FTN_CONTA_SERV
ORDER BY a.FTN_NUM_CTA_INVDUAL, a.FCN_ID_TIPO_SUBCTA