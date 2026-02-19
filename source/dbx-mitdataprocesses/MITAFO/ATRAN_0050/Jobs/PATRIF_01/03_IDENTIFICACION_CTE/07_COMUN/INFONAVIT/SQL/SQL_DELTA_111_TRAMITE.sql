SELECT
   FTC_BANDERA              ,
   FTN_NUM_CTA_INVDUAL      ,
   FTC_RFC_BUC              ,
   FTC_FOLIO                ,
   FTN_ID_ARCHIVO           ,
   FTC_NSS                  ,
   FTC_CURP                 ,
   FTC_RFC                  ,
   FTC_APELLIDO_PATER_AFORE ,
   FTC_APELLIDO_MATER_AFORE ,
   FTC_NOMBRE_AFORE         ,
   FTC_CORREO_ELEC          ,
   FTN_CELULAR,

   FTD_FEH_CRE
   
FROM #DELTA_TABLA_NAME#
WHERE FTC_BANDERA IS NOT NULL
ORDER BY FTN_NUM_CTA_INVDUAL