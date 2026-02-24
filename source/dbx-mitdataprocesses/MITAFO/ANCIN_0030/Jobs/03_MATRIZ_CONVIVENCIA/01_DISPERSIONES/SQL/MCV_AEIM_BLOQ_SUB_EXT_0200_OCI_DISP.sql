 /*Query que obtiene el saldo de las cuentas que tienen un credito de vivienda y fueron identidicadas*/

 SELECT '#sr_folio#' as FTC_FOLIO --Folio Subsecuente
       ,'#sr_folio_rel#' as FTC_FOLIO_REL -- Folio de la dispersión
       ,'#sr_proceso#' AS FCN_ID_PROCESO       -- 276 PROCESO VIRTUAL
       ,'#sr_subproceso#' AS FCN_ID_SUBPROCESO    -- 277 SUBSECUENTE VIRTUAL 839 SUBSECUENTE VIRTUAL ISSSTE
       ,DISP.FTN_NUM_CTA_INVDUAL
       ,0 FCN_ID_TIPO_SUBCTA
      ,NVL((
            CASE 
                  WHEN DISP.FFN_ID_CONCEPTO_MOV IN (
                  637,  -- RETIRO FOVISSSTE 92
                  638,  -- RETIRO FOVISSSTE 2008
                  1002, -- RETIRO INT. EXT. FOVISSSTE 92
                  1003  -- RETIRO INT. EXT. FOVISSSTE 2008
                  )
                  THEN DISP.FTF_MONTO_ACCIONES
                  ELSE DISP.FTF_MONTO_PESOS 
            END
      ), 0) AS FTF_MONTO
       ,CMOV.FCN_ID_TIPO_MONTO
FROM   CIERREN_ETL.TTSISGRAL_ETL_DISPERSION DISP
      ,CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE IDNT
      ,CIERREN.TFAFOGRAL_CONFIG_CONCEP_MOV CMOV
WHERE DISP.FTC_FOLIO = IDNT.FTC_FOLIO
AND   DISP.FTN_NUM_CTA_INVDUAL = IDNT.FTN_NUM_CTA_INVDUAL
AND   DISP.FTN_NSS = IDNT.FTN_NSS
AND   DISP.FTC_CURP = IDNT.FTC_CURP
AND   DISP.FFN_ID_CONCEPTO_MOV = CMOV.FFN_ID_CONCEPTO_MOV
AND   DISP.FTC_FOLIO = '#sr_folio_rel#'  -- Folio de la dispersión
AND DISP.FFN_ID_CONCEPTO_MOV IN (
        231,  -- RETIRO SUBSEC VIVIENDA
        637,  -- RETIRO FOVISSSTE 92
        638,  -- RETIRO FOVISSSTE 2008
        1002, -- RETIRO INT. EXT. FOVISSSTE 92
        1003  -- RETIRO INT. EXT. FOVISSSTE 2008
    )
    AND (
        CASE 
            WHEN DISP.FFN_ID_CONCEPTO_MOV IN (
             637, --RETIRO FOVISSSTE 92
             638  --RETIRO FOVISSSTE 2008
             )
            THEN DISP.FTF_MONTO_ACCIONES
            ELSE DISP.FTF_MONTO_PESOS 
        END
    ) > 0 --Validación de Saldo Mayor a Cero
AND   DISP.FNN_ID_VIV_GARANTIA =  1 -- Indicador de vivienda activo
AND   IDNT.FTN_ESTATUS_DIAG = 1 --Cliente identificado
