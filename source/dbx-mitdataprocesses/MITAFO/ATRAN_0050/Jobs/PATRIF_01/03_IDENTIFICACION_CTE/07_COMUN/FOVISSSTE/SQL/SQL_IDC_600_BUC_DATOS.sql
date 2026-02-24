SELECT 
   FTC_CURP                 ,
   FTN_NSS                  ,
   FTN_NUM_CTA_INVDUAL      ,
   FTC_APELLIDO_PATER_AFORE ,
   FTC_APELLIDO_MATER_AFORE ,
   FTC_NOMBRE_AFORE         ,
   FTC_CORREO_ELEC          ,
   FTN_CELULAR              ,
   FTC_NSS_BUC              ,
   FTD_FECHA_CERTIFICACION
FROM (
   SELECT
      FTC_CURP                 ,
      FTN_NSS                  ,
      FTN_NUM_CTA_INVDUAL      ,
      FTC_APELLIDO_PATER_AFORE ,
      FTC_APELLIDO_MATER_AFORE ,
      FTC_NOMBRE_AFORE         ,
      FTC_CORREO_ELEC          ,
      FTN_CELULAR              ,
      FTC_NSS_BUC              ,
      FTD_FECHA_CERTIFICACION  ,
      ROW_NUMBER() OVER (PARTITION BY FTN_NSS ORDER BY FTN_NSS ASC) AS rn
   FROM #DELTA_600_BUC#
   ORDER BY FTN_NSS ASC
)
WHERE rn = 1