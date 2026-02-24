SELECT FTC_FOLIO ,
      FCN_CVE_SIEFORE          SIEFORE
      ,FTN_NUM_CTA_INVDUAL      NUMCUE
      ,FTC_DIG_VERI             DVCUE 
      ,FFN_COD_MOV_ITGY         CODMOV
      ,FNN_NSS_ISSSTE_SUA       NSSTRA
      ,FND_FEH_CRE_SUA          FECPRO
      ,FTD_FEH_LIQUIDACION      FEHCCON
      ,FCN_VALOR_ACCION         VALCUO
      ,FND_FECTRA_SUA           FECTRA
      ,FNN_SECLOT_SUA           SECLOT
      ,FNN_ID_REFERENCIA        CORREL
      ,FND_FECHA_PAGO_SUA       FECPAG
      ,FNC_PERIODO_PAGO_PATRON  PERPAG
      ,FNC_FOLIO_PAGO_SUA       FOLSUA
      ,FND_FECHA_PAGO_SUA       FECSUA
      ,FNC_REG_PATRN_IMSS_SUA   NSSEMP
      ,lpad(replace (to_char( SUM (FTF_MONPES1), 'FM9999999999999.90'),'.'),15,0)   MONPES1
      ,lpad(replace (to_char( SUM (FTF_MONCUO1), 'FM99999999.9999990'),'.'),15,0)   MONCUO1
      ,lpad(replace (to_char( SUM (FTF_MONPES2), 'FM9999999999999.90'),'.'),15,0)   MONPES2
      ,lpad(replace (to_char( SUM (FTF_MONCUO2), 'FM99999999.9999990'),'.'),15,0)   MONCUO2
      ,lpad(replace (to_char( SUM (FTF_MONPES3), 'FM9999999999999.90'),'.'),15,0)   MONPES3
      ,lpad(replace (to_char( SUM (FTF_MONCUO3), 'FM99999999.9999990'),'.'),15,0)   MONCUO3
      ,lpad(replace (to_char( SUM (FTF_MONPES4), 'FM9999999999999.90'),'.'),15,0)   MONPES4
      ,lpad(replace (to_char( SUM (FTF_MONCUO4), 'FM99999999.9999990'),'.'),15,0)   MONCUO4
      ,lpad(replace (to_char( SUM (FTF_MONPES5), 'FM9999999999999.90'),'.'),15,0)   MONPES5
      ,lpad(replace (to_char( SUM (FTF_MONCUO5), 'FM99999999.9999990'),'.'),15,0)   MONCUO5
      ,lpad(replace (to_char( SUM (FTF_MONPES6), 'FM9999999999999.90'),'.'),15,0)   MONPES6
      ,lpad(replace (to_char( SUM (FTF_MONCUO6), 'FM99999999.9999990'),'.'),15,0)   MONCUO6
      ,lpad(replace (to_char( SUM (FTF_MONPES7), 'FM9999999999999.90'),'.'),15,0)   MONPES7
      ,lpad(replace (to_char( SUM (FTF_MONCUO7), 'FM99999999.9999990'),'.'),15,0)   MONCUO7
      ,lpad(replace (to_char( SUM (FTF_MONPES8), 'FM9999999999999.90'),'.'),15,0)   MONPES8
      ,lpad(replace (to_char( SUM (FTF_MONCUO8), 'FM99999999.9999990'),'.'),15,0)   MONCUO8
      ,lpad(replace (to_char( SUM (FTF_MONPES9), 'FM9999999999999.90'),'.'),15,0)   MONPES9
      ,lpad(replace (to_char( SUM (FTF_MONCUO9), 'FM99999999.9999990'),'.'),15,0)   MONCUO9
      ,FND_FECHA_1                   FECHA_1
      ,FND_FECHA_2                   FECHA_2
      ,FND_FECHA_VALOR_RCV           FND_FECVRCV
      ,FND_FECHA_VALOR_IMSS_ACV_VIV  FECVVIV
      ,FND_FECVGUB                   FECVGUB
      ,FNC_CVESERV                   CVESERV
      ,FNC_VARMAX                    VARMAX
    FROM CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO
    WHERE FTC_FOLIO = '#sr_folio#'
    group by FTC_FOLIO ,
    FCN_CVE_SIEFORE        
    ,FTN_NUM_CTA_INVDUAL    
    ,FTC_DIG_VERI           
    ,FFN_COD_MOV_ITGY       
    ,FNN_NSS_ISSSTE_SUA     
    ,FND_FEH_CRE_SUA        
    ,FTN_ID_ARCHIVO         
    ,FTD_FEH_LIQUIDACION    
    ,FCN_VALOR_ACCION       
    ,FND_FECTRA_SUA         
    ,FNN_SECLOT_SUA         
    ,FNN_ID_REFERENCIA      
    ,FND_FECHA_PAGO_SUA     
    ,FNC_PERIODO_PAGO_PATRON
    ,FNC_FOLIO_PAGO_SUA     
    ,FND_FECHA_PAGO_SUA     
    ,FNC_REG_PATRN_IMSS_SUA 
    ,FND_FECHA_1                 
    ,FND_FECHA_2                 
    ,FND_FECHA_VALOR_RCV         
    ,FND_FECHA_VALOR_IMSS_ACV_VIV
    ,FND_FECVGUB                 
    ,FNC_CVESERV                 
    ,FNC_VARMAX