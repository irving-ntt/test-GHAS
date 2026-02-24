SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro__diferente_a_02
	,CASE WHEN substr(FTC_LINEA,3,1) in ('1','2','3') then NULL else substr(FTC_LINEA,3,1) end Error_en_Tipo_de_Trabajador
	,CASE WHEN TRIM(substr(FTC_LINEA,4,18)) <> '' then NULL else substr(FTC_LINEA,4,18) end Error_en_CURP_igual_a_vacio
	,CASE WHEN substr(FTC_LINEA,43,1) = '5' then NULL else substr(FTC_LINEA,43,1) end Error_en_Tipo_Movimiento
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,44,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,44,15) end Error_en_AIVS_FOVISSSTE_92_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,59,20), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,59,20) end Error_en_Valor_de_AIVS_FOVISSSTE_92_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,79,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,79,15) end Error_en_Saldo_Total_de_Vivienda_FOVISSSTE_92_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,94,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,94,15) end Error_en_AIVS_FOVISSSTE_2008_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,109,20), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,109,20) end Error_en_Valor_de_AIVS_FOVISSSTE_2008_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,129,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,129,15) end Error_en_Saldo_Total_de_Vivienda_FOVISSSTE_2008_con_formato_invalido
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,144,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,144,8) end Error_en_Fecha_de_liquidacion_erroneo
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '09'