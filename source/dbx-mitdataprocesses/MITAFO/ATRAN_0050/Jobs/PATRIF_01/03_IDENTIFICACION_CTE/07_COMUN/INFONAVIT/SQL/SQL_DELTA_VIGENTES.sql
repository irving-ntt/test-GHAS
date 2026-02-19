SELECT * FROM (
   SELECT
      FTC_BANDERA                ,
      FTN_NUM_CTA_INVDUAL        ,
      FTC_RFC_BUC                ,
      FTC_FOLIO                  ,
      FTN_ID_ARCHIVO             ,
      FTC_NSS                    ,
      FTC_CURP                   ,
      FTC_RFC                    ,
      FTC_APELLIDO_PATER_AFORE   ,
      FTC_APELLIDO_MATER_AFORE   ,
      FTC_NOMBRE_AFORE           ,
      FTC_CORREO_ELEC            ,
      FTN_CELULAR                ,
      FCN_ID_IND_CTA_INDV        ,
      FFN_ID_CONFIG_INDI         ,
      FCC_VALOR_IND              ,
      FTC_VIGENCIA               ,
      FTN_DETALLE                ,
      ROW_NUMBER() OVER (PARTITION BY FTC_NSS ORDER BY FTC_NSS ASC) AS rn
   FROM #DELTA_TABLA_NAME#
   WHERE FTN_NUM_CTA_INVDUAL IS NOT NULL
     AND FCC_VALOR_IND = 1
     AND FTC_VIGENCIA = 1
)
-- WHERE rn = 1