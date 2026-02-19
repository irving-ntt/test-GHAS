SELECT DISTINCT
     IZQ.FTC_BANDERA              ,
     IZQ.FTN_NUM_CTA_INVDUAL      ,
     IZQ.FTC_RFC_BUC              ,
     IZQ.FTC_FOLIO                ,
     IZQ.FTN_ID_ARCHIVO           ,
     IZQ.FTC_NSS                  ,
     IZQ.FTC_CURP                 ,
     IZQ.FTC_RFC                  ,
     IZQ.FTC_APELLIDO_PATER_AFORE ,
     IZQ.FTC_APELLIDO_MATER_AFORE ,
     IZQ.FTC_NOMBRE_AFORE         ,
     IZQ.FTC_CORREO_ELEC          ,
     IZQ.FTN_CELULAR              ,
     IZQ.FTC_NSS_BUC              ,

    
     DER.FCN_ID_IND_CTA_INDV      ,
     DER.FFN_ID_CONFIG_INDI       ,
     DER.FCC_VALOR_IND            ,
     DER.FTC_VIGENCIA             ,
     DER.FTN_DETALLE
FROM      #CATALOG_SCHEMA#.DELTA_IZQUIERDA_#SR_FOLIO# IZQ
LEFT JOIN #CATALOG_SCHEMA#.DELTA_DERECHA_#SR_FOLIO# DER
ON IZQ.FTN_NUM_CTA_INVDUAL = DER.FTN_NUM_CTA_INVDUAL
ORDER BY IZQ.FTN_NUM_CTA_INVDUAL