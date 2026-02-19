WITH

-- 1. Read SDO_SOL per account/subaccount (from DataSet, after aggregation)
AG_120_SDOS_SOL AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FCN_ID_TIPO_SUBCTA,
        SUM(SDO_SOL) AS SDO_SOL
    FROM #DS_400_SDO_SOL_IMSS#
    GROUP BY FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA
),

-- 2. Filter: Only SDO_SOL > 0 and for the subaccount being processed
FL_100_SEL_SDO_SOL AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FCN_ID_TIPO_SUBCTA,
        SDO_SOL
    FROM AG_120_SDOS_SOL
    WHERE SDO_SOL > 0
      -- AND FCN_ID_TIPO_SUBCTA = <p_SUBCUENTA> -- If running for a specific subaccount, uncomment and set value
),

-- 3. Read available balances per subaccount (from DataSet)
DS_101_SDO_DISP_SUBCTAS_IMSS AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        SDO_DISPONIBLE_PESOS,
        SDO_DISPONIBLE_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FCD_FEH_ACCION,
        FCN_VALOR_ACCION,
        FCN_ID_REGIMEN,
        FCN_ID_VALOR_ACCION
    FROM #DS_101_SDO_DISP_SUBCTAS_IMSS#
),

-- 4. Prepare for join: Copy for join (CP_160)
CP_160 AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        SDO_DISPONIBLE_PESOS,
        SDO_DISPONIBLE_ACCION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FCD_FEH_ACCION,
        FCN_VALOR_ACCION,
        FCN_ID_REGIMEN,
        FCN_ID_VALOR_ACCION
    FROM #DS_101_SDO_DISP_SUBCTAS_IMSS#
),

-- 5. Join SDO_SOL and SDO_DISPONIBLE (inner join on account and subaccount)
JN_200_AG_SDO_DISP AS (
    SELECT
        f.FTN_NUM_CTA_INVDUAL,
        f.FCN_ID_TIPO_SUBCTA,
        f.SDO_SOL,
        d.SDO_DISPONIBLE_PESOS,
        d.SDO_DISPONIBLE_ACCION,
        d.FCN_ID_SIEFORE,
        d.FCD_FEH_ACCION,
        d.FCN_VALOR_ACCION,
        d.FCN_ID_REGIMEN,
        d.FCN_ID_VALOR_ACCION
    FROM FL_100_SEL_SDO_SOL f
    INNER JOIN CP_160 d
      ON f.FTN_NUM_CTA_INVDUAL = d.FTN_NUM_CTA_INVDUAL
     AND f.FCN_ID_TIPO_SUBCTA = d.FCN_ID_TIPO_SUBCTA
),

-- 6. Transformer TF_130_SALDO: Calculate MONTO_MOV (assignable amount) and diagnostics
TF_130_SALDO AS (
    SELECT
        j.FTN_NUM_CTA_INVDUAL,
        j.FCN_ID_TIPO_SUBCTA,
        j.SDO_SOL,
        j.SDO_DISPONIBLE_PESOS,
        j.SDO_DISPONIBLE_ACCION,
        j.FCN_ID_SIEFORE,
        j.FCD_FEH_ACCION,
        j.FCN_VALOR_ACCION,
        j.FCN_ID_REGIMEN,
        j.FCN_ID_VALOR_ACCION,
        -- MONTO_MOV: If available >= requested, assign requested; if available < requested and > 0, assign available; else 0
        CASE
            WHEN j.SDO_DISPONIBLE_PESOS >= j.SDO_SOL THEN j.SDO_SOL
            WHEN j.SDO_DISPONIBLE_PESOS < j.SDO_SOL AND j.SDO_DISPONIBLE_PESOS > 0 THEN j.SDO_DISPONIBLE_PESOS
            ELSE 0
        END AS MONTO_MOV,
        -- Diagnostic: 1 if assignable > 0, else 0
        CASE
            WHEN j.SDO_DISPONIBLE_PESOS >= j.SDO_SOL THEN 1
            WHEN j.SDO_DISPONIBLE_PESOS < j.SDO_SOL AND j.SDO_DISPONIBLE_PESOS > 0 THEN 1
            ELSE 0
        END AS DIAGNOSTICO
    FROM JN_200_AG_SDO_DISP j
),

-- 7. AG_140: Aggregate MONTO_MOV by all keys (sum assignable per group)
AG_140 AS (
    SELECT
        FTN_NUM_CTA_INVDUAL,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FCN_ID_REGIMEN,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_VALOR_ACCION,
        SUM(MONTO_MOV) AS MONTO_MOV
    FROM TF_130_SALDO
    GROUP BY
        FTN_NUM_CTA_INVDUAL,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FCN_ID_REGIMEN,
        FCD_FEH_ACCION,
        FCN_ID_VALOR_ACCION,
        FCN_VALOR_ACCION
),

-- 8. Join detail and aggregate (JO_150_SALDOS)
JO_150_SALDOS AS (
    SELECT
        t.FTN_NUM_CTA_INVDUAL,
        t.FCN_ID_TIPO_SUBCTA,
        t.SDO_SOL,
        t.SDO_DISPONIBLE_PESOS,
        t.SDO_DISPONIBLE_ACCION,
        t.FCN_ID_SIEFORE,
        t.FCD_FEH_ACCION,
        t.FCN_VALOR_ACCION,
        t.FCN_ID_REGIMEN,
        t.FCN_ID_VALOR_ACCION,
        a.MONTO_MOV
    FROM TF_130_SALDO t
    INNER JOIN AG_140 a
      ON t.FTN_NUM_CTA_INVDUAL = a.FTN_NUM_CTA_INVDUAL
     AND t.FCN_ID_TIPO_SUBCTA = a.FCN_ID_TIPO_SUBCTA
     AND t.FCN_ID_SIEFORE = a.FCN_ID_SIEFORE
     AND t.FCN_ID_REGIMEN = a.FCN_ID_REGIMEN
     AND t.FCD_FEH_ACCION = a.FCD_FEH_ACCION
     AND t.FCN_ID_VALOR_ACCION = a.FCN_ID_VALOR_ACCION
     AND t.FCN_VALOR_ACCION = a.FCN_VALOR_ACCION
),

-- 9. Final transformer TF_150_SDOS_SUBT: Calculate output columns as per DataStage logic
TF_150_SDOS_SUBT AS (
    SELECT
        j.FTN_NUM_CTA_INVDUAL,
        NULL AS FTC_NSS, -- Not available in this pipeline
        NULL AS FTN_CONSE_REG_LOTE, -- Not available in this pipeline
        j.SDO_SOL,
        j.FCN_ID_TIPO_SUBCTA,
        j.FCN_ID_SIEFORE,
        j.FCN_ID_REGIMEN,
        j.FCD_FEH_ACCION,
        j.FCN_ID_VALOR_ACCION,
        j.FCN_VALOR_ACCION AS FTN_VALOR_AIVS,
        -- FTN_SDO_PESOS: assignable amount in pesos
        j.MONTO_MOV AS FTN_SDO_PESOS,
        -- FTN_SDO_ACCIONES: assignable amount in actions
        CASE WHEN j.FCN_VALOR_ACCION IS NOT NULL AND j.FCN_VALOR_ACCION > 0
            THEN j.MONTO_MOV / j.FCN_VALOR_ACCION
            ELSE NULL
        END AS FTN_SDO_ACCIONES,
        -- FFN_ID_CONCEPTO_MOV: Movement concept (example: use fixed value, or parameter per subaccount type)
        CASE
            WHEN j.FCN_ID_TIPO_SUBCTA = 1 THEN 4063
            WHEN j.FCN_ID_TIPO_SUBCTA = 2 THEN 4070
            WHEN j.FCN_ID_TIPO_SUBCTA = 3 THEN 4184
            ELSE NULL
        END AS FFN_ID_CONCEPTO_MOV,
        -- FTN_ESTATUS: always 1 if assignable > 0, else 0
        CASE WHEN j.MONTO_MOV > 0 THEN 1 ELSE 0 END AS FTN_ESTATUS,
        -- FTC_DIAG_DEV: '01' if assignable = requested, '02' if assignable = 0, else '04'
        CASE
            WHEN j.MONTO_MOV = j.SDO_SOL THEN '01'
            WHEN j.MONTO_MOV = 0 THEN '02'
            ELSE '04'
        END AS FTC_DIAG_DEV,
        -- FTN_ESTATUS_indiv: 1 if assignable > 0, else 0
        CASE WHEN j.MONTO_MOV > 0 THEN 1 ELSE 0 END AS FTN_ESTATUS_indiv
    FROM JO_150_SALDOS j
    WHERE j.SDO_SOL > 0
)

-- 10. Final output for DS_600_SDOS_IMSS
SELECT
    FTN_NUM_CTA_INVDUAL,
    FTN_CONSE_REG_LOTE,
    SDO_SOL,
    FCN_ID_TIPO_SUBCTA,
    FCN_ID_SIEFORE,
    FCN_ID_REGIMEN,
    FCD_FEH_ACCION,
    FCN_ID_VALOR_ACCION,
    FTN_VALOR_AIVS,
    FTN_SDO_PESOS,
    FTN_SDO_ACCIONES,
    FFN_ID_CONCEPTO_MOV,
    FTN_ESTATUS,
    FTC_DIAG_DEV,
    FTN_ESTATUS_indiv
FROM TF_150_SDOS_SUBT