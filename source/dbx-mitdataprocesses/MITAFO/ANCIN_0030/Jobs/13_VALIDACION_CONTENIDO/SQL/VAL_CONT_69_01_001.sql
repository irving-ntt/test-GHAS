SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_01
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,3,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,3,8) end Error_en_campo_Identificador_de_fecha
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,11,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,11,9) end Error_en_campo_Total_de_registros_diferente_a_dato_numerico
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'