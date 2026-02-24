WITH AggTransacciones AS (
    SELECT
        fn.Indicador_Saldo_Disp,
        fn.FCN_ID_TIPO_SUBCTA,
        fn.FTC_TIPO_ARCH,
        fn.FTC_ESTATUS_REGISTRO,
        fn.FTC_FOLIO_BITACORA,
        proc.CONTEO_ACT,
        proc.CONTEO_RECHAZO,
        COUNT(Constante) AS Contar,
        SUM(fn.FTN_MONTO_PESOS) AS FTN_MONTO_PESOS_ACEP,
        SUM(fn.FTN_SDO_AIVS) AS FTN_SDO_AIVS_ACEP,
        SUM(FTN_INTER_SALDO) AS FTN_INTER_SALDO
    FROM #DELTA_600_TRANS# AS fn
    LEFT JOIN #DELTA_PROC_CONTEO# AS proc
        ON fn.FTC_FOLIO_BITACORA = proc.FTC_FOLIO_BITACORA
    GROUP BY
        fn.Indicador_Saldo_Disp,
        fn.FCN_ID_TIPO_SUBCTA,
        fn.FTC_TIPO_ARCH,
        fn.FTC_ESTATUS_REGISTRO,
        fn.FTC_FOLIO_BITACORA,
        proc.CONTEO_ACT,
        proc.CONTEO_RECHAZO
)
SELECT
    agg.Indicador_Saldo_Disp,
    agg.FCN_ID_TIPO_SUBCTA,
    agg.FTC_TIPO_ARCH,
    agg.FTC_ESTATUS_REGISTRO,
    agg.FTC_FOLIO_BITACORA AS FTC_FOLIO,
    agg.CONTEO_ACT AS FTN_NUM_REG_ACT_PRO,
    agg.CONTEO_RECHAZO AS FTN_NUM_REG_RECH_PRO,
    agg.FTN_MONTO_PESOS_ACEP,
    agg.FTN_SDO_AIVS_ACEP,
    agg.FTN_INTER_SALDO,
    agg.Contar,
    mov.FRN_ID_MOV_SUBCTA
FROM AggTransacciones AS agg
LEFT JOIN #DELTA_MOV_SUBCTA# AS mov
    ON agg.FCN_ID_TIPO_SUBCTA = mov.FCN_ID_TIPO_SUBCTA
    --AND mov.FCN_ID_SUBPROCESO = #SR_SUBPROCESO#
