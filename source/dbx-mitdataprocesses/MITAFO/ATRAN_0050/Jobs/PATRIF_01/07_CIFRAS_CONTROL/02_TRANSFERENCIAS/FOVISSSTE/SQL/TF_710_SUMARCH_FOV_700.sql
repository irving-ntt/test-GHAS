SELECT
    t_cctrl.FTC_FOLIO,
    t_cctrl.FRN_ID_MOV_SUBCTA AS FFN_ID_CONCEPTO_IMP,
    CASE WHEN t_cctrl.ftc_tipo_arch = '09' THEN t_cctrl.FTN_IMPORTE_RECH_PESOS_PRO ELSE NULL END AS FTN_IMPORTE_RECH_PESOS_PRO,
    CASE WHEN t_cctrl.ftc_tipo_arch = '09' THEN t_cctrl.FTN_IMPORTE_RECH_AIVS_PRO ELSE NULL END AS FTN_IMPORTE_RECH_AIVS_PRO,
    t_cctrl.Constante AS FTN_NUM_REG_RECH_PRO,
    from_utc_timestamp(current_timestamp(), 'America/Mexico_City') AS FTD_FEH_ACT,
    '#SR_USER#' AS FTC_USU_ACT
FROM #DELTA_TRANS_400_JOIN# t_cctrl



