WITH ds_1002_nss_multiples AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        FCN_ESTATUS,
        FTN_SDO_ACCIONES,
        FTN_SDO_PESOS,
        FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FFN_ID_CONCEPTO_MOV,
        SALDO_DISPONIBLE_AIVS,
        FTC_NSS,
        CONTEO,
        FTN_NUM_APLI_VIV_APOR_PAT_DEV
    FROM #DS_1002_NSS_MULTIPLES#
),

-- DS_1000_SDOS_1NSS_INFO: Already-calculated accounts with one NSS
ds_1000_sdos_1nss_info AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        FCN_ESTATUS,
        FTN_SDO_ACCIONES,
        FTN_SDO_PESOS,
        FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FFN_ID_CONCEPTO_MOV,
        FTC_DIAG_DEV
    FROM #DS_1000_SDOS_1NSS_INFO#
),

-- DS_300_CTAS_SIN_DISP_INFO: Accounts with insufficient or null available balance
ds_300_ctas_sin_disp_info AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_CONSE_REG_LOTE,
        FCN_ID_TIPO_SUBCTA
    FROM #DS_300_CTAS_SIN_DISP_INFO#
),

-- 2. TF_901_RESTA_SALDOS: Calculate running sufficiency per NSS group
resta_saldos AS (
    SELECT
        *,
        -- Calculate running sufficiency (Saldos) per FTC_NSS, ordered by FTN_CONSE_REG_LOTE, FTN_NUM_CTA_INVDUAL
        SALDO_DISPONIBLE_AIVS
            - SUM(FTN_NUM_APLI_VIV_APOR_PAT_DEV) OVER (
                PARTITION BY FTC_NSS
                ORDER BY FTN_CONSE_REG_LOTE, FTN_NUM_CTA_INVDUAL
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
              ) AS Saldos
    FROM ds_1002_nss_multiples
),

-- 3. FL_902_FIL_VALIDOS: Split into valid (Bandera=1) and rejected (Bandera=0)
fl_902_fil_validos_valid AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        FCN_ESTATUS,
        FTN_SDO_ACCIONES,
        FTN_SDO_PESOS,
        FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FFN_ID_CONCEPTO_MOV,
        SALDO_DISPONIBLE_AIVS,
        FTC_NSS,
        CONTEO,
        FTN_NUM_APLI_VIV_APOR_PAT_DEV,
        Saldos,
        CASE WHEN Saldos > 0 THEN 1 ELSE 0 END AS Bandera
    FROM resta_saldos
    WHERE Saldos > 0
),
fl_902_fil_validos_rej AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_CONSE_REG_LOTE,
        FCN_ID_TIPO_SUBCTA
    FROM resta_saldos
    WHERE Saldos <= 0
),

-- 4. TF_902_APLICA_MATRIZ_DE_REGLAS: Apply business rules to valid records
tf_902_aplica_matriz_de_reglas AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        1 AS FCN_ESTATUS,
        CASE
            WHEN FTN_NUM_APLI_VIV_APOR_PAT_DEV <= Saldos THEN FTN_NUM_APLI_VIV_APOR_PAT_DEV
            ELSE Saldos
        END AS FTN_SDO_ACCIONES,
        CASE
            WHEN FTN_NUM_APLI_VIV_APOR_PAT_DEV <= Saldos THEN FTN_NUM_APLI_VIV_APOR_PAT_DEV
            ELSE Saldos
        END * FTN_VALOR_AIVS AS FTN_SDO_PESOS,
        FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FFN_ID_CONCEPTO_MOV,
        CASE
            WHEN FTN_NUM_APLI_VIV_APOR_PAT_DEV <= Saldos THEN '01'
            ELSE '04'
        END AS FTC_DIAG_DEV
    FROM fl_902_fil_validos_valid
),

-- 5. FU_601_UNION_REJ: Funnel rejected records from FL_902_FIL_VALIDOS and DS_300_CTAS_SIN_DISP_INFO
fu_601_union_rej AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_CONSE_REG_LOTE,
        FCN_ID_TIPO_SUBCTA
    FROM fl_902_fil_validos_rej
    UNION ALL
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FTN_CONSE_REG_LOTE,
        FCN_ID_TIPO_SUBCTA
    FROM ds_300_ctas_sin_disp_info
),

-- 6. CG_602_GEN_CAMPOS: Generate default/null values for rejected/unmatched records
cg_602_gen_campos AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        FCN_ID_TIPO_SUBCTA,
        0 AS FCN_ESTATUS,
        NULL AS FTN_SDO_ACCIONES,
        NULL AS FTN_SDO_PESOS,
        NULL AS FTN_VALOR_AIVS,
        NULL AS FCD_FEH_ACCION,
        NULL AS FCN_ID_VALOR_ACCION,
        NULL AS FCN_ID_SIEFORE,
        NULL AS FFN_ID_CONCEPTO_MOV,
        '02' AS FTC_DIAG_DEV
    FROM fu_601_union_rej
),

-- 7. FU_603_UNION_TOTAL: Funnel all processed records (main, one-NSS, rejected)
fu_603_union_total AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        FCN_ESTATUS,
        FTN_SDO_ACCIONES,
        FTN_SDO_PESOS,
        FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FFN_ID_CONCEPTO_MOV,
        FTC_DIAG_DEV
    FROM tf_902_aplica_matriz_de_reglas
    UNION ALL
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        FCN_ESTATUS,
        FTN_SDO_ACCIONES,
        FTN_SDO_PESOS,
        FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FFN_ID_CONCEPTO_MOV,
        FTC_DIAG_DEV
    FROM ds_1000_sdos_1nss_info
    UNION ALL
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        FCN_ESTATUS,
        FTN_SDO_ACCIONES,
        FTN_SDO_PESOS,
        FTN_VALOR_AIVS,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FFN_ID_CONCEPTO_MOV,
        FTC_DIAG_DEV
    FROM cg_602_gen_campos
),

-- 8. TF_604_GEN_CAMPOS: Final transformer, applies nullification for status=0
tf_604_gen_campos AS (
    SELECT
        FTN_CONSE_REG_LOTE,
        FTN_NUM_CTA_INVDUAL,
        FCN_ESTATUS AS FTN_ESTATUS_indiv,
        FTN_SDO_ACCIONES,
        FTN_SDO_PESOS,
        CASE WHEN FCN_ESTATUS = 0 THEN NULL ELSE FTN_VALOR_AIVS END AS FTN_VALOR_AIVS,
        CASE WHEN FCN_ESTATUS = 0 THEN NULL ELSE FCD_FEH_ACCION END AS FCD_FEH_ACCION,
        CASE WHEN FCN_ESTATUS = 0 THEN NULL ELSE FCN_ID_VALOR_ACCION END AS FCN_ID_VALOR_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        CASE WHEN FCN_ESTATUS = 0 THEN NULL ELSE FFN_ID_CONCEPTO_MOV END AS FFN_ID_CONCEPTO_MOV,
        FTC_DIAG_DEV,
        FCN_ESTATUS AS FTN_ESTATUS
    FROM fu_603_union_total
)

-- 9. DS_13_INS_SDOS_INFO: Final output
SELECT *
FROM tf_604_gen_campos