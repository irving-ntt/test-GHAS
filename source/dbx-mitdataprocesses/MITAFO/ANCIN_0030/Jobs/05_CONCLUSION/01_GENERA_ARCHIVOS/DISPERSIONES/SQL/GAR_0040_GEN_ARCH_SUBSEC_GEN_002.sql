      SELECT
        COALESCE(CVE_SIEFORE, '00') ||
        COALESCE(FTN_NUM_CTA_INVDUAL, '0000000000') ||
        COALESCE(DVCUE, '0') ||
        RIGHT('00000000' || CAST(ROW_NUMBER() OVER (ORDER BY FFN_CODMOV) AS VARCHAR(8)), 8) ||
        COALESCE(FFN_CODMOV, '000') ||
        CASE
          WHEN NSSTRA IS NULL THEN RPAD(' ', 11)
          ELSE TRIM(NSSTRA) || RPAD(' ', 11 - LENGTH(TRIM(NSSTRA)))
        END ||
        COALESCE(FCD_FECPRO, '00000000') ||
        '0000000' ||
        COALESCE(FTD_FEHCCON, '00000000') ||
        COALESCE(FCN_VALOR_ACCION, '000000000000') ||
        COALESCE(FECTRA, '00000000') ||
        COALESCE(SECLOT, '000') ||
        '00000000' ||
        COALESCE(FECTRA, '00000000') || 
        COALESCE(FNC_PERIODO_PAGO_PATRON, '000000') ||
        '000000' ||
        '00000000' ||
        CASE
          WHEN FCN_ID_CONCEPTO_MOV IN ('638', '637') THEN 'SUB-APORT  '
          ELSE 'SUB-EXTEMP '
        END ||
        CASE
          WHEN FFN_CODMOV = '831' THEN COALESCE(MONPES1, '000000000000000')
          ELSE '000000000000000'
        END ||
        CASE
          WHEN FFN_CODMOV = '831' THEN COALESCE(MONCUO1, '000000000000000')
          ELSE '000000000000000'
        END ||
        '000000000000000' ||
        '000000000000000' ||
        CASE
          WHEN FFN_CODMOV = '855' THEN COALESCE(MONPES3, '000000000000000')
          ELSE '000000000000000'
        END ||
        CASE
          WHEN FFN_CODMOV = '855' THEN COALESCE(MONCUO3, '000000000000000')
          ELSE '000000000000000'
        END ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '000000000000000' ||
        '00000000' ||
        '00000000' ||
        '00000000' ||
        COALESCE(FND_FECHA_VALOR_IMSS_ACV_VIV, '00000000') ||
        '00000000' ||
        '02' ||
        '00000' AS VARMAX
      FROM #DELTA_TABLA_NAME1# T
      WHERE FFN_CODMOV = #p_CODMOV# -- This parameter is defined or given at notebook level, IS NOT a paremeter requested to services, this because this query must be used in diferent notebooks just changin this filter