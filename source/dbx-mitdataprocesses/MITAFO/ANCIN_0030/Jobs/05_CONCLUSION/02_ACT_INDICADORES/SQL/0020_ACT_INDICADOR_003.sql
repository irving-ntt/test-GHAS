    SELECT
        NULL AS FCN_ID_IND_CTA_INDV,
        FTN_NUM_CTA_INVDUAL,
        6 AS FFN_ID_CONFIG_INDI,
        TO_CHAR(FTD_FEH_LIQUIDACION, 'dd/MM/yyyy') AS FCC_VALOR_IND,
        '1' AS FTC_VIGENCIA,
         from_utc_timestamp(current_timestamp(), 'GMT-6')  FTD_FEH_REG,
        '#SR_USUARIO#' AS FTC_USU_REG,
         from_utc_timestamp(current_timestamp(), 'GMT-6') FTD_FEH_ACT,
        '#SR_USUARIO#' AS FCC_USU_ACT
    FROM #CATALOG_NAME#.#SCHEMA_NAME#.DELTA_DISPER_ACT_IND_02_#SR_FOLIO#
    WHERE (#SR_SUBPROCESO# = 8 OR #SR_SUBPROCESO# = 120 OR #SR_SUBPROCESO# = 118)
        AND FCN_ID_TIPO_SUBCTA = 19
        AND FCN_ID_IND_6 IS NULL
    UNION ALL
    SELECT
        NULL AS FCN_ID_IND_CTA_INDV,
        FTN_NUM_CTA_INVDUAL,
        16 AS FFN_ID_CONFIG_INDI,
        TO_CHAR(FTD_FEH_LIQUIDACION, 'dd/MM/yyyy') AS FCC_VALOR_IND,
        '1' AS FTC_VIGENCIA,
        from_utc_timestamp(current_timestamp(), 'GMT-6')  FTD_FEH_REG,
        '#SR_USUARIO#' AS FTC_USU_REG,
         from_utc_timestamp(current_timestamp(), 'GMT-6')  FTD_FEH_ACT,
        '#SR_USUARIO#' AS FCC_USU_ACT
    FROM #CATALOG_NAME#.#SCHEMA_NAME#.DELTA_DISPER_ACT_IND_02_#SR_FOLIO#
    WHERE (#SR_SUBPROCESO# = 8 OR #SR_SUBPROCESO# = 120 OR #SR_SUBPROCESO# = 118)
        AND FCN_ID_TIPO_SUBCTA = 23
        AND FCN_ID_IND_16 IS NULL
    UNION ALL
    SELECT
        CASE
            WHEN FCN_ID_IND_13 IS NOT NULL THEN FCN_ID_IND_13
            ELSE NULL
        END AS FCN_ID_IND_CTA_INDV,
        FTN_NUM_CTA_INVDUAL,
        CASE
            WHEN INDI_13 IS NULL THEN 13
            ELSE INDI_13
        END AS FFN_ID_CONFIG_INDI,
        '1' AS FCC_VALOR_IND,
        '1' AS FTC_VIGENCIA,
         from_utc_timestamp(current_timestamp(), 'GMT-6') FTD_FEH_REG,
        '#SR_USUARIO#' AS FTC_USU_REG,
         from_utc_timestamp(current_timestamp(), 'GMT-6') FTD_FEH_ACT,
        '#SR_USUARIO#' AS FCC_USU_ACT
    FROM #CATALOG_NAME#.#SCHEMA_NAME#.DELTA_DISPER_ACT_IND_02_#SR_FOLIO#
    WHERE FTC_VIG_13 = 0 OR FCN_ID_IND_13 IS NULL