SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_09
	,CASE WHEN CAST(substr(FTC_LINEA,3,1) AS DOUBLE) = '0' then NULL else substr(FTC_LINEA,3,1) end Error_en_campo_diferente_a_cero
	,CASE WHEN CAST(substr(FTC_LINEA,4,34) AS DOUBLE) = '0' then NULL else substr(FTC_LINEA,4,34) end Error_en_campo_diferente_a_cero_2
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'