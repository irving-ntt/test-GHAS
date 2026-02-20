SELECT
    SIEFORE ||
    RIGHT(NUMCUE, 10)  ||
    COALESCE(DVCUE, ' ')  ||
    RIGHT('00000000' || CAST(ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS VARCHAR(8)), 8)  ||
    CODMOV ||
    COALESCE(NSSTRA, '           ')  ||
    TO_CHAR(CURRENT_DATE, 'yyyyMMdd')  ||
    '0000000'  ||
    FEHCCON ||
    VALCUO ||
    FECPRO  ||
    SECLOT ||
    CORREL ||
    FEHCCON  ||
    FEHCCON  ||
    CAST(FTN_FOLIO_SUA AS VARCHAR(6))  ||
    '00000000'  ||
    '           '  ||
    MONPES1 ||
    MONCUO1 ||
    MONPES2 ||
    MONCUO2 ||
    MONPES3 ||
    MONCUO3 ||
    MONPES4 ||
    MONCUO4 ||
    MONPES5 ||
    MONCUO5 ||
    MONPES6 ||
    MONCUO6 ||
    MONPES7 ||
    MONCUO7 ||
    MONPES8 ||
    MONCUO8 ||
    MONPES9 ||
    MONCUO9 ||
    '00000000'  ||
    '00000000'  ||
    '00000000'  ||
    '00000000'  ||
    '00000000'  ||
    CVESERV ||
    '00000' AS VARMAX
FROM #DELTA_TABLA_NAME1# T