WITH

-- 1. Only requested balances SDO_SOL > 0
FL_100_SEL_SDO_SOL AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTC_NSS,
        FTN_CONSE_REG_LOTE,
        SDO_SOL,
        BANDERA_MONTO,
        BANDERA_SOL,
        FCN_ID_TIPO_SUBCTA
    FROM #DS_400_SDO_SOL_IMSS#
    WHERE SDO_SOL > 0
),

-- 2. Join with available balances per subaccount
JN_200_AG_SDO_DISP AS (
    SELECT
        f.FTN_NUM_CTA_INVDUAL,
        f.FTC_NSS,
        f.FTN_CONSE_REG_LOTE,
        f.SDO_SOL,
        f.BANDERA_MONTO,
        f.BANDERA_SOL,
        f.FCN_ID_TIPO_SUBCTA,
        d.SDO_DISPONIBLE_PESOS,
        d.SDO_DISPONIBLE_ACCION,
        d.FCN_ID_SIEFORE,
        d.FCD_FEH_ACCION,
        d.FCN_VALOR_ACCION,
        d.FCN_ID_REGIMEN,
        d.FCN_ID_VALOR_ACCION
    FROM FL_100_SEL_SDO_SOL f
    LEFT JOIN #DS_101_SDO_DISP_SUBCTAS_IMSS# d
      ON f.FTN_NUM_CTA_INVDUAL = d.FTN_NUM_CTA_INVDUAL
     AND f.FCN_ID_TIPO_SUBCTA = d.FCN_ID_TIPO_SUBCTA
),

-- 3. Aggregate available balances per account
AG_400_SUM_DISP_X_CTA AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        SUM(SDO_DISPONIBLE_PESOS) AS SDO_DISPONIBLE_X_CTA
    FROM #DS_101_SDO_DISP_SUBCTAS_IMSS#
    GROUP BY FTN_NUM_CTA_INVDUAL
),

-- 4. Join detailed and aggregated data
JN_500_AG_DISP AS (
    SELECT
        j.*,
        a.SDO_DISPONIBLE_X_CTA
    FROM JN_200_AG_SDO_DISP j
    LEFT JOIN AG_400_SUM_DISP_X_CTA a
      ON j.FTN_NUM_CTA_INVDUAL = a.FTN_NUM_CTA_INVDUAL
),

-- 5. Split by subaccount type and available balance
RET97 AS (
    SELECT *
    FROM JN_500_AG_DISP
    WHERE SDO_DISPONIBLE_PESOS > 0 AND FCN_ID_TIPO_SUBCTA = 1
),
CV AS (
    SELECT *
    FROM JN_500_AG_DISP
    WHERE SDO_DISPONIBLE_PESOS > 0 AND FCN_ID_TIPO_SUBCTA = 2
),
CS AS (
    SELECT *
    FROM JN_500_AG_DISP
    WHERE SDO_DISPONIBLE_PESOS > 0 AND FCN_ID_TIPO_SUBCTA = 3
),

-- 6. For each, calculate remaining available balance after allocation and Bandera
TF_601_RESTA_SDOS_DISP_RET97 AS (
    SELECT
        *,
        SDO_DISPONIBLE_PESOS - SDO_SOL AS SALDOS,
        CASE WHEN (SDO_DISPONIBLE_PESOS - SDO_SOL) <= 0 THEN 0 ELSE 1 END AS Bandera
    FROM RET97
),
TF_602_RESTA_SDOS_DISP_CV AS (
    SELECT
        *,
        SDO_DISPONIBLE_PESOS - SDO_SOL AS SALDOS,
        CASE WHEN (SDO_DISPONIBLE_PESOS - SDO_SOL) <= 0 THEN 0 ELSE 1 END AS Bandera
    FROM CV
),
TF_603_RESTA_SDOS_DISP_CS AS (
    SELECT
        *,
        SDO_DISPONIBLE_PESOS - SDO_SOL AS SALDOS,
        CASE WHEN (SDO_DISPONIBLE_PESOS - SDO_SOL) <= 0 THEN 0 ELSE 1 END AS Bandera
    FROM CS
),

-- 7. Funnel (union all) the three streams
FN_700_UNE_SDOS AS (
    SELECT * FROM TF_601_RESTA_SDOS_DISP_RET97
    UNION ALL
    SELECT * FROM TF_602_RESTA_SDOS_DISP_CV
    UNION ALL
    SELECT * FROM TF_603_RESTA_SDOS_DISP_CS
),

-- 8. Filter for Bandera = 1 (sufficient available balance)
FL_800_SEP_SDOS_DISP AS (
    SELECT *
    FROM FN_700_UNE_SDOS
    WHERE Bandera = 1
),

-- 9. Conversion and allocation per subaccount type (TF_900_VAL_SUF_SDOS)
TF_900_VAL_SUF_SDOS AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        1 AS FTN_ESTATUS,
        -- SDO_ACCIONES: allocated amount in actions
        CASE 
            WHEN COALESCE(FCN_VALOR_ACCION,0) > 0 THEN 
                CASE 
                    WHEN SDO_SOL <= SALDOS THEN SDO_SOL / FCN_VALOR_ACCION
                    ELSE SALDOS / FCN_VALOR_ACCION
                END
            ELSE NULL
        END AS FTN_SDO_ACCIONES,
        -- SDO_PESOS: allocated amount in pesos
        CASE 
            WHEN SDO_SOL <= SALDOS THEN SDO_SOL
            ELSE SALDOS
        END AS FTN_SDO_PESOS,
        -- VALOR_AIVS: value per action
        FCN_VALOR_ACCION AS FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        -- FFN_ID_CONCEPTO_MOV: movement concept, by subaccount and BANDERA_MONTO
        CASE 
            WHEN FCN_ID_TIPO_SUBCTA = 1 THEN
                CASE 
                    WHEN BANDERA_MONTO = 1 THEN 4063
                    WHEN BANDERA_MONTO = 2 THEN 4182
                    WHEN BANDERA_MONTO = 3 THEN 4526
                    ELSE NULL
                END
            WHEN FCN_ID_TIPO_SUBCTA = 2 THEN
                CASE 
                    WHEN BANDERA_MONTO = 1 THEN 4070
                    WHEN BANDERA_MONTO = 2 THEN 4527
                    WHEN BANDERA_MONTO = 3 THEN 4183
                    WHEN BANDERA_MONTO = 4 THEN 4528
                    WHEN BANDERA_MONTO = 5 THEN 4529
                    WHEN BANDERA_MONTO = 6 THEN 4526
                    ELSE NULL
                END
            WHEN FCN_ID_TIPO_SUBCTA = 3 THEN
                4184
            ELSE NULL
        END AS FFN_ID_CONCEPTO_MOV,
        -- BAND_DIAG: 0 if SDO_SOL <= SALDOS else 1
        CASE WHEN SDO_SOL <= SALDOS THEN 0 ELSE 1 END AS BAND_DIAG
    FROM FL_800_SEP_SDOS_DISP
),

-- 10. Final funnel (union all subaccount types)
FN_901_UNE_SDOS_X_SUBCTA AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        1 AS FTN_ESTATUS,
        FTN_SDO_ACCIONES,
        FTN_SDO_PESOS,
        FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FFN_ID_CONCEPTO_MOV,
        BAND_DIAG
    FROM TF_900_VAL_SUF_SDOS
)

-- 11. Output for DS_902_SDOS_IMSS
SELECT
    FTN_CONSE_REG_LOTE,
    FTN_NUM_CTA_INVDUAL,
    FTN_ESTATUS,
    FTN_SDO_ACCIONES,
    FTN_SDO_PESOS,
    FTN_VALOR_AIVS,
    FCD_FEH_ACCION,
    FCN_ID_VALOR_ACCION,
    FCN_ID_TIPO_SUBCTA,
    FCN_ID_SIEFORE,
    FFN_ID_CONCEPTO_MOV,
    BAND_DIAG
FROM FN_901_UNE_SDOS_X_SUBCTA