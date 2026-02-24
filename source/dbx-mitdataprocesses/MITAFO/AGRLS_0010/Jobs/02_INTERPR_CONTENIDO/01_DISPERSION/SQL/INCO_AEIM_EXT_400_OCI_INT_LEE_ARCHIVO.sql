select  TRIM(SUBSTR(LEE.FTC_LINEA,55,50)) FTC_NOMBRE_COMPLETO_ARCH
               ,replace(regexp_substr(SUBSTR(LEE.FTC_LINEA,55,50), '\$[^\$]+$'),'$','') as FTC_NOMBRES
               ,replace(regexp_substr(SUBSTR(LEE.FTC_LINEA,55,50), '[^\$]+\$'),'$','') as FTC_AP_PATERNO
               ,replace(regexp_substr(SUBSTR(LEE.FTC_LINEA,55,50), '\$[^\$]+\$'),'$','') as FTC_AP_MATERNO
               ,SUBSTR(LEE.FTC_LINEA,13,11) FTN_NSS_ARCH 
               ,SUBSTR(LEE.FTC_LINEA,24,13) FTC_RFC_ARCH 
               ,SUBSTR(LEE.FTC_LINEA,148,11) FNC_REG_PATRONAL_IMSS 
               ,SUBSTR(LEE.FTC_LINEA,172,3) FTC_CLAVE_ENT_RECEP -- IMSS
               ,0 FTN_NUM_CTA_INVDUAL
       FROM CIERREN_ETL.TTSISGRAL_ETL_LEE_ARCHIVO LEE
       WHERE SUBSTR(LEE.FTC_LINEA,1,2) IN (02,03)  
	AND FTN_ID_ARCHIVO = #sr_id_archivo#