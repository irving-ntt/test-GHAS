-- =============================================================================
-- QUERY 1: Filtrar y homologar datos de DELTA_700_SALDOS_PROC
-- (Resultado: DELTA_TMP_MOV_TRANS_SALDOS)
-- =============================================================================

WITH
subcta_15 AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP_VIV97 AS FTN_IND_SDO_DISP,
        FTN_SDO_AIVS,
        FTN_VALOR_AIVS,
        FTN_MONTO_PESOS,
        FCN_ID_TIPO_SUBCTA_97 AS FCN_ID_TIPO_SUBCTA,
        FTN_NUM_APLI_INTE_VIVI97_SOL AS FTN_NUM_APLI_INTE_VIV,
        FTN_SALDO_VIVI97_SOLI AS FTN_SALDO_VIV,
        FTC_FOLIO_BITACORA AS FTC_FOLIO,
        FCN_ESTATUS,
        FCN_ID_REGIMEN,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTC_USU_CRE,
        15 AS tipo_subcta
    FROM #CATALOG_SCHEMA#.#DELTA_700_SALDOS_PROC#
    WHERE FCN_ID_TIPO_SUBCTA_97 = #p_tipo_subcta15#
      AND FTN_IND_SDO_DISP_VIV97 = 1
),
subcta_16 AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP_92 AS FTN_IND_SDO_DISP,
        FTN_SDO_AIVS_92 AS FTN_SDO_AIVS,
        FTN_VALOR_AIVS,
        FTN_MONTO_PESOS_92 AS FTN_MONTO_PESOS,
        FCN_ID_TIPO_SUBCTA_92 AS FCN_ID_TIPO_SUBCTA,
        FTN_NUM_APLI_INTE_VIVI92_SOL AS FTN_NUM_APLI_INTE_VIV,
        0 AS FTN_SALDO_VIV,
        FTC_FOLIO_BITACORA AS FTC_FOLIO,
        FCN_ESTATUS,
        FCN_ID_REGIMEN,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_MOTI_RECH,
        FTD_FEH_CRE,
        FTC_USU_CRE,
        16 AS tipo_subcta
    FROM #CATALOG_SCHEMA#.#DELTA_700_SALDOS_PROC#
    WHERE FCN_ID_TIPO_SUBCTA_92 = #p_tipo_subcta16#
      AND FTN_IND_SDO_DISP_92 = 1
)
SELECT * FROM subcta_15
UNION ALL
SELECT * FROM subcta_16