SELECT
   FTC_FOLIO                 ,
   FTN_ID_ARCHIVO            ,
   FTC_NSS_IMSS AS FTN_NSS   ,
   FTC_CURP                  ,
   FTC_RFC                   ,
   FTC_NOMBRE_CTE            ,
   FTN_NUM_CTA_INVDUAL       ,
   ESTATUS_DIAG AS FTN_ESTATUS_DIAG  ,
   FTC_ID_DIAGNOSTICO        ,
   FTC_ID_SUBP_NO_VIG        ,
   FTD_FECHA_CERTIFICACION   ,
   FTN_CTE_PENSIONADO        ,
   FTC_CLAVE_ENT_RECEP       ,
   FTC_CLAVE_TMP             
FROM #DELTA_CARGA_2# 

