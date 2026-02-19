SELECT
  SIEFORE  ||
  RIGHT(NUMCUE, 10)  ||
  TRIM(DVCUE)  ||
  RIGHT('00000000' || CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS STRING), 8)  ||
  CODMOV ||
  CASE WHEN NSSTRA IS NULL THEN '           ' ELSE NSSTRA END  ||
  FECPRO ||
  '0000000'  ||
  FEHCCON ||
  VALCUO ||
  date_format(to_date(from_utc_timestamp(current_timestamp(), 'America/Mexico_City')), 'yyyyMMdd') ||
  '859'  ||
  CORREL ||
  FECPAG ||
  PERPAG ||
  '000000'  ||
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
  '02'  ||
  '00000' as T1
FROM #DELTA_TABLA_NAME1#