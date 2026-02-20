WITH RES_01 AS
(
	SELECT
		COUNT(1) ID_01
		,SUM(CAST(substr(FTC_LINEA,475,15) AS DOUBLE)) ID_02 -- Suma de Número de Aplicaciones de Intereses de Vivienda 97
		,SUM(CAST(substr(FTC_LINEA,490,15) AS DOUBLE)) ID_03 -- Suma de Saldo de Vivienda 97 solicitado
	

	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '02'
)





SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_09
	,CASE WHEN CAST(substr(FTC_LINEA,3,9) AS DOUBLE) = RES_01.ID_01 then NULL else substr(FTC_LINEA,3,9) end Error_en_cantidad_de_registros_de_detalle
	,CASE WHEN CAST(substr(FTC_LINEA,42,18) AS DOUBLE) = RES_01.ID_02 then NULL else substr(FTC_LINEA,42,18) end Error_en_campo_Suma_de_Numero_de_Aplicaciones_de_Intereses_de_Vivienda_97
	,CASE WHEN CAST(substr(FTC_LINEA,60,15) AS DOUBLE) = RES_01.ID_03 then NULL else substr(FTC_LINEA,60,15) end Error_en_campo_Suma_de_Saldo_de_Vivienda_97_solicitado
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01 RES_01 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'


