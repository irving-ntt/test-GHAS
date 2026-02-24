SELECT  '#sr_folio#' as FTC_FOLIO
       ,#sr_proceso# AS FCN_ID_PROCESO
       ,#sr_subproceso# AS FCN_ID_SUBPROCESO
       ,DISP.FTN_NUM_CTA_INVDUAL
       ,DECODE('#sr_tipo_mov#','ABONO',0,
                              'CARGO',DISP.FCN_ID_TIPO_SUBCTA)FCN_ID_TIPO_SUBCTA
       ,NVL(DISP.FTF_MONTO_PESOS,0) FTF_MONTO
       ,DISP.FFN_ID_CONCEPTO_MOV
       ,'#sr_tipo_mov#' as TIPO_MOV
       ,'#sr_folio_rel#' as FTC_FOLIO_REL
FROM  CIERREN_ETL.TTSISGRAL_ETL_DISPERSION DISP
WHERE DISP.FTC_FOLIO = DECODE('#sr_tipo_mov#','ABONO','#sr_folio#','#sr_folio_rel#')
AND  (CASE
          WHEN '#sr_tipo_mov#' = 'ABONO' THEN 1
          WHEN '#sr_tipo_mov#' = 'CARGO' THEN DISP.FTF_MONTO_PESOS
      END) > 0 
AND ( ('#sr_tipo_mov#' = 'ABONO' AND DISP.FFN_ID_CONCEPTO_MOV NOT IN (231)) --231	RETIRO SUBSEC VIVIENDA 
      OR ('#sr_tipo_mov#' = 'CARGO' AND DISP.FFN_ID_CONCEPTO_MOV IN (231)) --231	RETIRO SUBSEC VIVIENDA
    )
AND   NVL(DISP.FNN_ID_VIV_GARANTIA,0) = DECODE('#sr_tipo_mov#','ABONO',NVL(DISP.FNN_ID_VIV_GARANTIA,0),1)
