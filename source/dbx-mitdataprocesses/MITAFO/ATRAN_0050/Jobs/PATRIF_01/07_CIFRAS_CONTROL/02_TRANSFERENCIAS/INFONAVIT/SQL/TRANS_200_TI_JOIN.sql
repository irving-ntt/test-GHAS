-- SELECT
--     agg.Indicador_Saldo_Disp,
--     agg.FCN_ID_TIPO_SUBCTA,
--     agg.FTC_ESTATUS_REGISTRO,
--     agg.FTC_FOLIO_BITACORA,
--     agg.CONTEO_TOTAL,
--     agg.CONTEO_RECHAZO,
--     agg.COUNT,
--     agg.FTN_MONTO_PESOS_ACEP,
--     agg.FTN_SDO_AIVS_ACEP,
--     mov.FRN_ID_MOV_SUBCTA
-- FROM (
--     SELECT
--         fn.Indicador_Saldo_Disp,
--         fn.FCN_ID_TIPO_SUBCTA,
--         fn.FTC_ESTATUS_REGISTRO,
--         fn.FTC_FOLIO_BITACORA,
--         proc.CONTEO_TOTAL,
--         proc.CONTEO_RECHAZO,
--         COUNT(*) AS COUNT,
--         SUM(fn.FTN_MONTO_PESOS) AS FTN_MONTO_PESOS_ACEP,
--         SUM(fn.FTN_SDO_AIVS) AS FTN_SDO_AIVS_ACEP
--     FROM #DELTA_600_TRANS# fn
--     LEFT JOIN #DELTA_PROC_CONTEO# proc
--         ON fn.FTC_FOLIO_BITACORA = proc.FTC_FOLIO_BITACORA
--     GROUP BY
--         fn.Indicador_Saldo_Disp,
--         fn.FCN_ID_TIPO_SUBCTA,
--         fn.FTC_ESTATUS_REGISTRO,
--         fn.FTC_FOLIO_BITACORA,
--         proc.CONTEO_TOTAL,
--         proc.CONTEO_RECHAZO
-- ) agg
-- LEFT JOIN CIERREN.TRAFOGRAL_MOV_SUBCTA mov
--     ON agg.FCN_ID_TIPO_SUBCTA = mov.FCN_ID_TIPO_SUBCTA
--     AND mov.FCN_ID_SUBPROCESO = #SR_SUBPROCESO#

-- Agregación de transacciones y conteos por folio y tipo de subcuenta
WITH AggTransacciones AS (
    SELECT
        fn.Indicador_Saldo_Disp,
        fn.FCN_ID_TIPO_SUBCTA,
        fn.FTC_ESTATUS_REGISTRO,
        fn.FTC_FOLIO_BITACORA,
        proc.CONTEO_TOTAL,
        proc.CONTEO_RECHAZO,
        COUNT(*) AS COUNT,
        SUM(fn.FTN_MONTO_PESOS) AS FTN_MONTO_PESOS_ACEP,
        SUM(fn.FTN_SDO_AIVS) AS FTN_SDO_AIVS_ACEP
    FROM #DELTA_600_TRANS# AS fn
    LEFT JOIN #DELTA_PROC_CONTEO# AS proc
        ON fn.FTC_FOLIO_BITACORA = proc.FTC_FOLIO_BITACORA
    GROUP BY
        fn.Indicador_Saldo_Disp,
        fn.FCN_ID_TIPO_SUBCTA,
        fn.FTC_ESTATUS_REGISTRO,
        fn.FTC_FOLIO_BITACORA,
        proc.CONTEO_TOTAL,
        proc.CONTEO_RECHAZO
)
SELECT
    agg.Indicador_Saldo_Disp,
    agg.FCN_ID_TIPO_SUBCTA,
    agg.FTC_ESTATUS_REGISTRO,
    agg.FTC_FOLIO_BITACORA,
    agg.CONTEO_TOTAL,
    agg.CONTEO_RECHAZO,
    agg.COUNT,
    agg.FTN_MONTO_PESOS_ACEP,
    agg.FTN_SDO_AIVS_ACEP,
    mov.FRN_ID_MOV_SUBCTA
FROM AggTransacciones AS agg
LEFT JOIN #DELTA_MOV_SUBCTA# AS mov
    ON agg.FCN_ID_TIPO_SUBCTA = mov.FCN_ID_TIPO_SUBCTA
    --AND mov.FCN_ID_SUBPROCESO = #SR_SUBPROCESO#

