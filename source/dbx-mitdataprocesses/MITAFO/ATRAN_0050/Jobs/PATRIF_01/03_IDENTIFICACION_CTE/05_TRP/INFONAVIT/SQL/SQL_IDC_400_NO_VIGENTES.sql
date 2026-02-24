   SELECT
      0 AS FTC_IDENTIFICADOS      ,
      FTN_NUM_CTA_INVDUAL         ,
      254 AS FTN_ID_DIAGNOSTICO  ,
      FTC_RFC_BUC                 ,
      FTC_FOLIO                   ,
      FTN_ID_ARCHIVO              ,
      FTC_NSS                     ,
      FTC_CURP                    ,
      FTC_RFC                     ,
      NULL AS FTN_ID_SUBP_NO_CONV ,
      0 AS FTN_VIGENCIA           ,
      FTC_APELLIDO_PATER_AFORE    ,
      FTC_APELLIDO_MATER_AFORE    ,
      FTC_NOMBRE_AFORE            ,
      FTC_CORREO_ELEC             ,
      FTN_CELULAR                 ,
      FTC_INSTITUTO_ORIGEN
   FROM #DELTA_TABLA_NAME#
   ORDER BY FTC_NSS, FTN_NUM_CTA_INVDUAL 
