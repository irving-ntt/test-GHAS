SELECT 
   FTN_NSS             ,
   FTC_CURP            ,
   FTN_NUM_CTA_INVDUAL ,
   FTC_CORREO_ELEC     ,
   FTN_CELULAR         
FROM (
   SELECT
      FTN_NSS             ,
      FTC_CURP            ,
      FTN_NUM_CTA_INVDUAL ,
      FTC_CORREO_ELEC     ,
      FTN_CELULAR         ,
      ROW_NUMBER() OVER (PARTITION BY FTN_NSS, FTC_CURP ORDER BY FTN_NSS ASC, FTC_CURP ASC) AS rn
   FROM #DELTA_600_BUC#
   ORDER BY FTN_NSS ASC
)
WHERE rn = 1