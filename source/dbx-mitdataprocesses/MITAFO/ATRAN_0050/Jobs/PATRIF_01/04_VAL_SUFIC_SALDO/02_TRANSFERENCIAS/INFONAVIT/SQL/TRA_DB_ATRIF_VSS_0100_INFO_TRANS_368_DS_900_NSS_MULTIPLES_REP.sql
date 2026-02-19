 SELECT 
    v.FTN_NUM_CTA_AFORE AS FTN_NUM_CTA_INVDUAL, -- Column 1: Output column from CO_502_SUMREG
    v.FTN_ID_SUBP,                             -- Column 2: Output column from CO_502_SUMREG
    NVL(v.FTN_SALDO_VIVI97_SOLI, 0) AS FTN_SALDO_VIVI97_SOLI, -- Column 3: Output column from CO_502_SUMREG
    NVL(v.FTN_NUM_APLI_INTE_VIVI97_SOL, 0) AS FTN_NUM_APLI_INTE_VIVI97_SOL, -- Column 4: Output column from CO_502_SUMREG
    v.FECHA_VALOR_AIVS,                        -- Column 5: Derived column from JO_504_AG_BANDERA_NSS
    v.FTN_CONTA_SERV,                          -- Column 6: Output column from CO_502_SUMREG
    v.FTC_NSS_TRABA_AFORE,                     -- Column 7: Output column from CO_502_SUMREG
    NVL(v.SALDO_DISPONIBLE_AIVS_VIV97, 0) AS SALDO_DISPONIBLE_AIVS, -- Column 8: Output column from CO_502_SUMREG
    NVL(d.FCN_VALOR_ACCION, 0) AS FCN_VALOR_ACCION, -- Column 9: Output column from DB_700_VALOR_ACCION
    d.FCN_ID_VALOR_ACCION,                     -- Column 10: Output column from DB_700_VALOR_ACCION
    1 AS FTN_IND_SDO_DISP_VIV97,               -- Column 11: Rule: Set FTN_IND_SDO_DISP_VIV97 to 1
    '#sr_folio#' AS FTC_FOLIO_BITACORA,        -- Column 13: Rule: Pass FTC_FOLIO_BITACORA
    15 AS FCN_ID_TIPO_SUBCTA_97                -- Column 14: Rule: Map FCN_ID_VALOR_ACCION to FCN_ID_TIPO_SUBCTA_97
FROM 
    (
        -- JO_504_AG_BANDERA_NSS logic
        SELECT 
            a.FTN_NUM_CTA_AFORE,
            a.FTN_ID_SUBP,
            NVL(a.FTN_SALDO_VIVI97_SOLI, 0) AS FTN_SALDO_VIVI97_SOLI,
            NVL(a.FTN_NUM_APLI_INTE_VIVI97_SOL, 0) AS FTN_NUM_APLI_INTE_VIVI97_SOL,
            ADD_MONTHS(TRUNC(a.FTD_FEH_PRESEN, 'MONTH'), 1) AS FECHA_VALOR_AIVS, -- Derived column
            a.FTN_CONTA_SERV,
            a.FTC_NSS_TRABA_AFORE,
            NVL(c.SALDO_DISPONIBLE_AIVS, 0) AS SALDO_DISPONIBLE_AIVS_VIV97
        FROM 
            PROCESOS.TTCRXGRAL_TRANS_INFONA a -- Input table for DS_500_TRANFERENCIAS
        LEFT JOIN 
            (SELECT 
                FTC_NUM_CTA_INVDUAL,
                SUM(NVL(FTN_DISP_ACCIONES, 0)) AS SALDO_DISPONIBLE_AIVS -- Aggregated column for SALDO_DISPONIBLE_AIVS_VIV97
             FROM 
                CIERREN.TTAFOGRAL_BALANCE_MOVS
             WHERE 
                FCN_ID_SIEFORE = 81           -- Rule: Filter by FCN_ID_SIEFORE = 81
                AND FCN_ID_TIPO_SUBCTA = 15   -- Rule: Filter by FCN_ID_TIPO_SUBCTA = 16
             GROUP BY 
                FTC_NUM_CTA_INVDUAL) c
        ON 
            a.FTN_NUM_CTA_AFORE = c.FTC_NUM_CTA_INVDUAL -- Rule: Join on FTN_NUM_CTA_AFORE = FTC_NUM_CTA_INVDUAL
        WHERE 
            a.FTC_FOLIO_BITACORA = '#sr_folio#' -- Rule: Filter by FTC_FOLIO_BITACORA
            AND ((FTN_MOTIVO_RECHAZO <> 93)
AND   (FTN_MOTIVO_RECHAZO <> 254)
AND  (FTN_MOTIVO_RECHAZO <> 375) 
AND  (FTN_MOTIVO_RECHAZO <> 1178)
OR (FTN_MOTIVO_RECHAZO IS NULL))
            AND NVL(c.SALDO_DISPONIBLE_AIVS, 0) > 0     -- Rule: SALDO_DISPONIBLE_AIVS_VIV97 > 0
            AND NVL(a.FTN_NUM_APLI_INTE_VIVI97_SOL, 0) > 0 -- Rule: FTN_NUM_APLI_INTE_VIVI97_SOL > 0
    ) v
LEFT JOIN 
    (
        -- Aggregated data from AG_503_SumNSS
        SELECT 
            FTC_NSS_TRABA_AFORE,              -- Column passed to AG_503_SumNSS
            COUNT(c.FTC_NUM_CTA_INVDUAL) AS CONTEO -- Rule: Count occurrences of FTC_NUM_CTA_INVDUAL
        FROM 
            PROCESOS.TTCRXGRAL_TRANS_INFONA a -- Input table for DS_500_TRANFERENCIAS
        LEFT JOIN 
            (SELECT 
                FTC_NUM_CTA_INVDUAL,
                SUM(NVL(FTN_DISP_ACCIONES, 0)) AS SALDO_DISPONIBLE_AIVS -- Aggregated column for SALDO_DISPONIBLE_AIVS
             FROM 
                CIERREN.TTAFOGRAL_BALANCE_MOVS
             WHERE 
                FCN_ID_SIEFORE = 81           -- Rule: Filter by FCN_ID_SIEFORE = 81
                AND FCN_ID_TIPO_SUBCTA = 15   -- Rule: Filter by FCN_ID_TIPO_SUBCTA = 16
             GROUP BY 
                FTC_NUM_CTA_INVDUAL) c
        ON 
            a.FTN_NUM_CTA_AFORE = c.FTC_NUM_CTA_INVDUAL -- Rule: Join on FTN_NUM_CTA_AFORE = FTC_NUM_CTA_INVDUAL
        WHERE 
            a.FTC_FOLIO_BITACORA = '#sr_folio#' -- Rule: Filter by FTC_FOLIO_BITACORA
            AND ((FTN_MOTIVO_RECHAZO <> 93)
AND   (FTN_MOTIVO_RECHAZO <> 254)
AND  (FTN_MOTIVO_RECHAZO <> 375) 
AND  (FTN_MOTIVO_RECHAZO <> 1178)
OR (FTN_MOTIVO_RECHAZO IS NULL))
            AND NVL(c.SALDO_DISPONIBLE_AIVS, 0) > 0     -- Rule: SALDO_DISPONIBLE_AIVS_VIV97 > 0
            AND NVL(a.FTN_NUM_APLI_INTE_VIVI97_SOL, 0) > 0 -- Rule: FTN_NUM_APLI_INTE_VIVI97_SOL > 0
        GROUP BY 
            FTC_NSS_TRABA_AFORE
    ) a
ON 
    v.FTC_NSS_TRABA_AFORE = a.FTC_NSS_TRABA_AFORE -- Rule: Join on FTC_NSS_TRABA_AFORE
LEFT JOIN 
    (
        -- DB_700_VALOR_ACCION logic
        SELECT 
            FCD_FEH_ACCION,                  -- Column passed to LO_505_BUS_VAL_ACCION
            FCN_VALOR_ACCION,               -- Column passed to LO_505_BUS_VAL_ACCION
            FCN_ID_VALOR_ACCION             -- Column passed to LO_505_BUS_VAL_ACCION
        FROM 
            CIERREN.TCAFOGRAL_VALOR_ACCION
        WHERE 
            FCN_ID_SIEFORE = 81             -- Rule: Filter by FCN_ID_SIEFORE = 81
            AND FCN_ID_REGIMEN = 138        -- Rule: Filter by FCN_ID_REGIMEN = 138
            --AND TO_TIMESTAMP(TRUNC(FCD_FEH_ACCION), 'DD-MM-YY HH24:MI:SS') = TO_TIMESTAMP(ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), 1), 'DD-MM-YY HH24:MI:SS') -- Rule: Match FECHA_VALOR_AIVS
    ) d
ON 
    v.FECHA_VALOR_AIVS = d.FCD_FEH_ACCION -- Rule: Join on FECHA_VALOR_AIVS = FCD_FEH_ACCION
WHERE  -- Rule: Filter for multiple NSS (CONTEO > 1)
    NVL(a.CONTEO, 0) > 1 