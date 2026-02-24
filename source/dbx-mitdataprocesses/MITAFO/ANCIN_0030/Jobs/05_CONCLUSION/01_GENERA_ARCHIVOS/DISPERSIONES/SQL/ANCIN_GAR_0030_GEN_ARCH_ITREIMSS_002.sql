SELECT
    COALESCE(CVE_SIEFORE, RPAD('', 2, '0')) AS CVE_SIEFORE,  -- length: 2
    COALESCE(FTN_NUM_CTA_INVDUAL, RPAD('', 9, '0')) AS FTN_NUM_CTA_INVDUAL,  -- length: 9
    COALESCE(DVCUE, RPAD('', 1, '0')) AS DVCUE,  -- length: 1
    SUBSTR(LPAD(ROW_NUMBER() OVER (ORDER BY FFN_CODMOV), 8, '0'), -8) AS NUMREG,  -- length: 8
    COALESCE(FFN_CODMOV, RPAD('', 3, '0')) AS FFN_CODMOV,  -- length: 3
    LPAD(COALESCE(NSSTRA, ''), 11, ' ') AS NSSTRA,  -- length: 11
    COALESCE(FCD_FECPRO, RPAD('', 8, '0')) AS FCD_FECPRO,  -- length: 8
    RPAD('', 7, '0') AS SECPRO,  -- length: 7
    COALESCE(FTD_FEHCCON, RPAD('', 8, '0')) AS FTD_FEHCCON,  -- length: 8
    COALESCE(FCN_VALOR_ACCION, RPAD('', 12, '0')) AS FCN_VALOR_ACCION,  -- length: 12
    COALESCE(FECTRA, RPAD('', 8, '0')) AS FECTRA,  -- length: 8
    '994' AS SECLOT,  -- length: 3
    RPAD('', 8, '0') AS FNN_CORREL,  -- length: 8
    COALESCE(FTD_FEHCCON, RPAD('', 8, '0')) AS FND_FECHA_PAGO,  -- length: 8
    COALESCE(FNC_PERIODO_PAGO_PATRON, RPAD('', 6, '0')) AS FNC_PERIODO_PAGO_PATRON,  -- length: 6
    COALESCE(FNC_FOLIO_PAGO_SUA, RPAD('', 6, '0')) AS FNC_FOLIO_PAGO_SUA,  -- length: 6
    COALESCE(FECSUA, RPAD('', 8, '0')) AS FECSUA,  -- length: 8
    COALESCE(NSSEMP, RPAD('', 11, '0')) AS NSSEMP,  -- length: 11
    COALESCE(MONPES1, RPAD('', 15, '0')) AS MONPES_1,  -- length: 15
    COALESCE(MONCUO1, RPAD('', 15, '0')) AS MONCUO_1,  -- length: 15
---157
    RPAD('', 15, '0') AS MONPES_2,  -- length: 15
    RPAD('', 15, '0') AS MONCUO_2,  -- length: 15
---187
    CASE 
        WHEN FFN_CODMOV IN (114)
        THEN COALESCE(MONPES3, RPAD('', 15, '0'))
        ELSE RPAD('', 15, '0')
    END AS MONPES_3,  -- length: 15

    CASE 
        WHEN FFN_CODMOV IN (114)
        THEN COALESCE(MONCUO3, RPAD('', 15, '0'))
        ELSE RPAD('', 15, '0')
    END AS MONCUO_3,  -- length: 15

    RPAD('', 15, '0')  AS MONPES_4,  -- length: 15

    RPAD('', 15, '0')  AS MONCUO_4,  -- length: 15

    RPAD('', 15, '0')  AS MONPES_5,  -- length: 15

    RPAD('', 15, '0')  AS MONCUO_5,  -- length: 15

    RPAD('', 15, '0')  AS MONPES_6,  -- length: 15

    RPAD('', 15, '0')  AS MONCUO_6,  -- length: 15

    RPAD('', 15, '0')  AS MONPES_7,  -- length: 15

    RPAD('', 15, '0') AS MONCUO_7,  -- length: 15

    RPAD('', 15, '0')  AS MONPES_8,  -- length: 15

    RPAD('', 15, '0') AS MONCUO_8,  -- length: 15

    RPAD('', 15, '0')  AS MONPES_9,  -- length: 15

    RPAD('', 15, '0') AS MONCUO_9,  -- length: 15

    CASE 
        WHEN FFN_CODMOV IN (124)
        THEN '00000044'
        ELSE RPAD('', 8, '0')
    END AS FECHA_1,-- length: 8

    RPAD('', 8, '0') AS FECHA_2,  -- length: 8
    
    COALESCE(FND_FECVRCV, RPAD('', 8, '0')) AS FND_FECVRCV,  -- length: 8
    RPAD('', 8, '0') AS FND_FECHA_VALOR_IMSS_ACV_VIV,  -- length: 8
    COALESCE(FECVGUB, RPAD('', 8, '0')) AS FECVGUB,  -- length: 8

    '01' AS CVESERV,  -- length: 2
    RPAD('', 5, '0') AS VARMAX  -- length: 5

FROM #CATALOG_SCHEMA#.#tabla_delta#
