-- NB_PATRIF_REVMOV_0400_DESACT_INDIC_003_MERGE.sql
-- ========================================================================
-- Propósito: Hacer MERGE desde tabla AUX hacia tabla final
-- ========================================================================
-- Stage Original: BD_400_IND_CTA_INDV (Oracle Connector - UPDATE mode)
-- WriteMode: 1 (UPDATE)
-- Table: CIERREN.TTAFOGRAL_IND_CTA_INDV
-- Keys (composite): FCN_ID_IND_CTA_INDV, FTN_NUM_CTA_INVDUAL, FFN_ID_CONFIG_INDI
-- ========================================================================

MERGE INTO #CX_CRN_ESQUEMA#.#TL_IND_CTA_INDV# target
USING (
    SELECT
        FCN_ID_IND_CTA_INDV,
        FTN_NUM_CTA_INVDUAL,
        FFN_ID_CONFIG_INDI,
        FCC_VALOR_IND,
        FTC_VIGENCIA,
        FTD_FEH_ACT,
        FCC_USU_ACT
    FROM (
        SELECT
            FCN_ID_IND_CTA_INDV,
            FTN_NUM_CTA_INVDUAL,
            FFN_ID_CONFIG_INDI,
            FCC_VALOR_IND,
            FTC_VIGENCIA,
            FTD_FEH_ACT,
            FCC_USU_ACT,
            ROW_NUMBER() OVER (
                PARTITION BY
                    FCN_ID_IND_CTA_INDV,
                    FTN_NUM_CTA_INVDUAL,
                    FFN_ID_CONFIG_INDI
                ORDER BY
                    FTD_FEH_ACT DESC
            ) AS rn
        FROM #CX_DATAUX_ESQUEMA#.TTAFOGRAL_IND_CTA_INDV_AUX
    )
    WHERE rn = 1
) source
ON (
    target.FCN_ID_IND_CTA_INDV   = source.FCN_ID_IND_CTA_INDV
    AND target.FTN_NUM_CTA_INVDUAL = source.FTN_NUM_CTA_INVDUAL
    AND target.FFN_ID_CONFIG_INDI  = source.FFN_ID_CONFIG_INDI
)
WHEN MATCHED THEN
    UPDATE SET
        target.FCC_VALOR_IND = source.FCC_VALOR_IND,
        target.FTC_VIGENCIA  = source.FTC_VIGENCIA,
        target.FTD_FEH_ACT   = source.FTD_FEH_ACT,
        target.FCC_USU_ACT   = source.FCC_USU_ACT