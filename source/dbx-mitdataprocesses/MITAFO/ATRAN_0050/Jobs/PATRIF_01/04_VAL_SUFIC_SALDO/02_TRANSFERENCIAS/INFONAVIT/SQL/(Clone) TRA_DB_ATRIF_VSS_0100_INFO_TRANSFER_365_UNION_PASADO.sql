WITH
-- 1. DS_500_NSS_AG: Main input dataset
DS_500_NSS_AG AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_ID_SUBP,
        FTN_CONTA_SERV,
        FTC_NSS_TRABA_AFORE,
        FTN_NUM_APLI_INTE_VIVI92_SOL,
        FTN_NUM_APLI_INTE_VIVI97_SOL,
        CONTEO,
        FCN_VALOR_ACCION,
        FCN_ID_VALOR_ACCION,
        FTC_FOLIO_BITACORA,
        FCN_ID_TIPO_SUBCTA_97,
        ValorSaldosSolPesos_VIV97,
        ValorSaldosSolPesos_VIV92,
        ValorSaldosDisPesos_VIV97,
        ValorSaldosDisPesos_VIV92
    FROM #DS_500_NSS_AG#
),

-- 2. TF_501_RESTA_SALDOS: Calculate available balances and flags
tf_501_resta_saldos AS (
    SELECT
        *,
        -- SaldosVIV97: If ValorSaldosSolPesos_VIV97 = 0 then 0 else ValorSaldosDisPesos_VIV97
        CASE WHEN ValorSaldosSolPesos_VIV97 = 0 THEN 0 ELSE ValorSaldosDisPesos_VIV97 END AS SaldosVIV97,
        -- BanderaVIV97: If ValorSaldosSolPesos_VIV97 = 0 OR SaldosVIV97 <= 0 then 0 else 1
        CASE WHEN ValorSaldosSolPesos_VIV97 = 0 OR ValorSaldosDisPesos_VIV97 <= 0 THEN 0 ELSE 1 END AS BanderaVIV97,
        -- SaldosVIV92: If ValorSaldosSolPesos_VIV92 = 0 then 0 else ValorSaldosDisPesos_VIV92
        CASE WHEN ValorSaldosSolPesos_VIV92 = 0 THEN 0 ELSE ValorSaldosDisPesos_VIV92 END AS SaldosVIV92,
        -- BanderaVIV92: If ValorSaldosSolPesos_VIV92 = 0 OR SaldosVIV92 <= 0 then 0 else 1
        CASE WHEN ValorSaldosSolPesos_VIV92 = 0 OR ValorSaldosDisPesos_VIV92 <= 0 THEN 0 ELSE 1 END AS BanderaVIV92
    FROM #DS_500_NSS_AG#
),

-- 3. FI_502_REJ: Split available and rejected
fi_502_rej_available AS (
    SELECT *
    FROM tf_501_resta_saldos
    WHERE BanderaVIV97 > 0 OR BanderaVIV92 > 0
),
fi_502_rej_rejected AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_ID_SUBP,
        FTN_NUM_APLI_INTE_VIVI92_SOL,
        FTN_NUM_APLI_INTE_VIVI97_SOL,
        FTN_CONTA_SERV
    FROM tf_501_resta_saldos
    WHERE BanderaVIV97 = 0 AND BanderaVIV92 = 0
),

-- 4. TF_503_RESTA_SALDO: Final available balance calculations
tf_503_resta_saldo AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        -- FTN_IND_SDO_DISP_VIV97: 1 if vSaldo97 > 0 else 0
        CASE WHEN vSaldo97 > 0 THEN 1 ELSE 0 END AS FTN_IND_SDO_DISP_VIV97,
        -- FTN_IND_SDO_DISP_92: 1 if vSaldo92 > 0 else 0
        CASE WHEN vSaldo92 > 0 THEN 1 ELSE 0 END AS FTN_IND_SDO_DISP_92,
        FCN_VALOR_ACCION,
        vSaldo97 AS FTN_MONTO_PESOS,
        vSaldo92 AS FTN_MONTO_PESOS_92,
        FTC_FOLIO_BITACORA,
        CAST(15 AS NUMERIC(4,0)) AS FCN_ID_TIPO_SUBCTA_97,
        CAST(16 AS NUMERIC(10,0)) AS FCN_ID_TIPO_SUBCTA_92,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_NUM_APLI_INTE_VIVI92_SOL,
        FTN_NUM_APLI_INTE_VIVI97_SOL,
        -- DIAG92: '01' if not sufficient, else NULL
        CASE WHEN ValorSaldosSolPesos_VIV92 = 0 AND vSaldoSol92 >= ValorSaldosSolPesos_VIV92 THEN NULL ELSE '01' END AS DIAG92,
        CASE WHEN ValorSaldosSolPesos_VIV97 = 0 AND vSaldoSol97 >= ValorSaldosSolPesos_VIV97 THEN NULL ELSE '01' END AS DIAG97
    FROM (
        SELECT
            *,
            -- vSaldoSol97: If SaldosVIV97 < 0 then 0 else SaldosVIV97
            CASE WHEN SaldosVIV97 < 0 THEN 0 ELSE SaldosVIV97 END AS vSaldoSol97,
            -- vSaldoSol92: If SaldosVIV92 < 0 then 0 else SaldosVIV92
            CASE WHEN SaldosVIV92 < 0 THEN 0 ELSE SaldosVIV92 END AS vSaldoSol92
        FROM fi_502_rej_available
    ) t1
    CROSS JOIN LATERAL (
        -- vSaldo97: min(ValorSaldosSolPesos_VIV97, vSaldoSol97)
        SELECT
            CASE WHEN ValorSaldosSolPesos_VIV97 <= vSaldoSol97 THEN ValorSaldosSolPesos_VIV97 ELSE vSaldoSol97 END AS vSaldo97,
            CASE WHEN ValorSaldosSolPesos_VIV92 <= vSaldoSol92 THEN ValorSaldosSolPesos_VIV92 ELSE vSaldoSol92 END AS vSaldo92
    ) t2
),

-- 5. #DS_400_REJINSSALDOS#: Rejected input dataset
ds_400_rejinsaldos AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_ID_SUBP,
        FTN_NUM_APLI_INTE_VIVI92_SOL,
        FTN_NUM_APLI_INTE_VIVI97_SOL,
        FTN_CONTA_SERV
    FROM #DS_400_REJINSSALDOS#
),

-- 6. FU_401_UNION_REJ: Union of rejections
fu_401_union_rej AS (
    SELECT * FROM ds_400_rejinsaldos
    UNION ALL
    SELECT * FROM fi_502_rej_rejected
),

-- 7. CG_402_GEN_COL_FALTANTES: Generate missing columns for rejections
cg_402_gen_col_faltantes AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        0 AS FTN_IND_SDO_DISP_VIV97,
        0 AS FTN_IND_SDO_DISP_92,
        CAST(0 AS NUMERIC(18,14)) AS FCN_VALOR_ACCION,
        CAST(0 AS NUMERIC(18,6)) AS FTN_MONTO_PESOS,
        CAST(0 AS NUMERIC(18,6)) AS FTN_MONTO_PESOS_92,
        CAST(#sr_folio# AS VARCHAR(30)) AS FTC_FOLIO_BITACORA,
        CAST(15 AS NUMERIC(4,0)) AS FCN_ID_TIPO_SUBCTA_97,
        CAST(16 AS NUMERIC(10,0)) AS FCN_ID_TIPO_SUBCTA_92,
        FTN_ID_SUBP,
        NULL AS FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_NUM_APLI_INTE_VIVI92_SOL,
        FTN_NUM_APLI_INTE_VIVI97_SOL,
        NULL AS DIAG92,
        NULL AS DIAG97
    FROM fu_401_union_rej
),

-- 8. DS_400_NNS_UNICO: Additional input dataset (direct)
ds_400_nns_unico AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP_VIV97,
        FTN_IND_SDO_DISP_92,
        FCN_VALOR_ACCION,
        FTN_MONTO_PESOS,
        FTN_MONTO_PESOS_92,
        FTC_FOLIO_BITACORA,
        FCN_ID_TIPO_SUBCTA_97,
        FCN_ID_TIPO_SUBCTA_92,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        FTN_NUM_APLI_INTE_VIVI92_SOL,
        FTN_NUM_APLI_INTE_VIVI97_SOL,
        DIAG92,
        DIAG97
    FROM #DS_400_NNS_UNICO#
),

-- 9. FU_504_UNI_COMPLETA: Funnel (union) of all three flows
fu_504_uni_completa AS (
    SELECT * FROM cg_402_gen_col_faltantes
    UNION ALL
    SELECT * FROM tf_503_resta_saldo
    UNION ALL
    SELECT * FROM ds_400_nns_unico
),

-- 10. TF_505_GEN_CAMPOS: Final transformation before writing to output table
tf_505_gen_campos AS (
    SELECT *
       /* FTN_NUM_CTA_INVDUAL,
        FTN_IND_SDO_DISP_VIV97,
        FTN_IND_SDO_DISP_92,
        -- FTN_SDO_AIVS: If FTN_IND_SDO_DISP_VIV97 = 1 then FTN_MONTO_PESOS / FCN_VALOR_ACCION else 0
        CASE WHEN FTN_IND_SDO_DISP_VIV97 = 1 THEN FTN_MONTO_PESOS / NULLIF(FCN_VALOR_ACCION,0) ELSE 0 END AS FTN_SDO_AIVS,
        -- FTN_SDO_AIVS_92: If FTN_IND_SDO_DISP_92 = 1 then FTN_MONTO_PESOS_92 / FCN_VALOR_ACCION else 0
        CASE WHEN FTN_IND_SDO_DISP_92 = 1 THEN FTN_MONTO_PESOS_92 / NULLIF(FCN_VALOR_ACCION,0) ELSE 0 END AS FTN_SDO_AIVS_92,
        -- FTN_VALOR_AIVS: If no rejection, FCN_VALOR_ACCION else 0
        CASE WHEN (FTN_IND_SDO_DISP_92 = 0 AND FTN_IND_SDO_DISP_VIV97 = 0) THEN 0 ELSE FCN_VALOR_ACCION END AS FTN_VALOR_AIVS,
        FTN_MONTO_PESOS,
        FTN_MONTO_PESOS_92,
        -- FCN_ESTATUS: 0 if both indicators are 0, else 1
        CASE WHEN FTN_IND_SDO_DISP_92 = 0 AND FTN_IND_SDO_DISP_VIV97 = 0 THEN 0 ELSE 1 END AS FCN_ESTATUS,
        CURRENT_TIMESTAMP AS FTD_FEH_CRE,
        'DATABRICKS' AS FTC_USU_CRE,
        FTC_FOLIO_BITACORA,
        CAST(138 AS NUMERIC(10,0)) AS FCN_ID_REGIMEN,
        FCN_ID_TIPO_SUBCTA_97,
        FCN_ID_TIPO_SUBCTA_92,
        FTN_ID_SUBP,
        FCN_ID_VALOR_ACCION,
        FTN_CONTA_SERV,
        -- FTN_MOTI_RECH: If both indicators are 0, set to parameter, else NULL
        CASE WHEN FTN_IND_SDO_DISP_92 = 0 AND FTN_IND_SDO_DISP_VIV97 = 0 THEN 584 ELSE NULL END AS FTN_MOTI_RECH*/
    FROM fu_504_uni_completa
)

-- Final SELECT: This is the output as it would be written to BD_700_SUF_SALDOS
SELECT *
FROM tf_505_gen_campos
