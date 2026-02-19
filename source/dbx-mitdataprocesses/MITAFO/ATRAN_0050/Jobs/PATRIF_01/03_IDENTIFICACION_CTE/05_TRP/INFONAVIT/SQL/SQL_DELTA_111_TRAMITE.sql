SELECT
   FTC_BANDERA              ,
   FTN_NUM_CTA_INVDUAL      ,
   FTC_RFC_BUC              ,
   FTC_FOLIO                ,
   FTN_ID_ARCHIVO           ,
   FTC_NSS                  ,
   FTC_CURP                 ,
   FTC_RFC                  ,
   FTC_APELLIDO_PATERNO ,
   FTC_APELLIDO_MATERNO ,
   FTC_NOMBRE         ,
   FTC_CORREO_ELEC          ,
   FTN_CELULAR              ,
   FTC_INSTITUTO_ORIGEN    
   
FROM #DELTA_TABLA_NAME#
WHERE FTC_BANDERA IS NOT NULL
ORDER BY FTN_NUM_CTA_INVDUAL