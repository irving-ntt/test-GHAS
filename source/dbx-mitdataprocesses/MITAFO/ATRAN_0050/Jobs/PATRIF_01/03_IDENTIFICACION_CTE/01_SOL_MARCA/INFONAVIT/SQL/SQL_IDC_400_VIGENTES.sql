SELECT 
      FTC_IDENTIFICADOS        ,
      FTN_NUM_CTA_INVDUAL      ,
      FTN_ID_DIAGNOSTICO       ,
      FTC_RFC_BUC              ,
      FTC_FOLIO                ,
      FTN_ID_ARCHIVO           ,
      FTC_NSS                  ,
      FTC_CURP                 ,
      FTC_RFC                  ,
      FTN_ID_SUBP_NO_CONV      ,
      FTN_VIGENCIA             ,
      FTC_APELLIDO_PATER_AFORE ,
      FTC_APELLIDO_MATER_AFORE ,
      FTC_NOMBRE_AFORE         ,
      FTC_CORREO_ELEC          ,
      FTN_CELULAR
FROM (   
   SELECT
      1 AS FTC_IDENTIFICADOS                      ,
      FTN_NUM_CTA_INVDUAL                         ,
      NULL AS FTN_ID_DIAGNOSTICO                  ,
      FTC_RFC_BUC                                 ,
      FTC_FOLIO                                   ,
      FTN_ID_ARCHIVO                              ,
      FTC_NSS                                     ,
      FTC_CURP                                    ,
      FTC_RFC                                     ,
      NULL AS FTN_ID_SUBP_NO_CONV                 ,
      LEFT(TRIM(FCC_VALOR_IND),1) AS FTN_VIGENCIA ,
      FTC_APELLIDO_PATER_AFORE                    ,
      FTC_APELLIDO_MATER_AFORE                    ,
      FTC_NOMBRE_AFORE                            ,
      FTC_CORREO_ELEC                             ,
      FTN_CELULAR                                 ,
      ROW_NUMBER() OVER (PARTITION BY FTC_NSS, FTN_NUM_CTA_INVDUAL ORDER BY FTC_NSS, FTN_NUM_CTA_INVDUAL ASC) AS rn
   FROM #DELTA_TABLA_NAME#
   ORDER BY FTC_NSS, FTN_NUM_CTA_INVDUAL
) WHERE rn = 1