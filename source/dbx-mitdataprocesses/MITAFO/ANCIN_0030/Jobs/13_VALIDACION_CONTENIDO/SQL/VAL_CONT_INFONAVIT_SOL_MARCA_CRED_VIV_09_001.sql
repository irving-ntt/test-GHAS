WITH RES_01 AS
(
	SELECT
		COUNT(1) ID_01
		,SUM(CAST(substr(FTC_LINEA,490,15) AS DOUBLE)) ID_02 -- Saldo de Vivienda 97
		,SUM(CAST(substr(FTC_LINEA,565,15) AS DOUBLE)) ID_03 -- Saldo de Vivienda 92
		,SUM(CAST(substr(FTC_LINEA,660,15) AS DOUBLE)) ID_04 -- Intereses de saldo de vivienda 97
		,SUM(CAST(substr(FTC_LINEA,675,15) AS DOUBLE)) ID_05 -- Intereses de saldo de vivienda 92
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_tipo_de_registro
	,CASE WHEN CAST(substr(FTC_LINEA,3,9) AS INT) = RES_01.ID_01 then NULL else substr(FTC_LINEA,3,9) end Error_en_cantidad_de_registros_de_detalle
	,CASE WHEN CAST(substr(FTC_LINEA,57,15) AS DOUBLE) = RES_01.ID_02 then NULL else substr(FTC_LINEA,57,15) end Error_en_suma_de_saldo_de_vivienda_97
	,CASE WHEN CAST(substr(FTC_LINEA,132,15) AS DOUBLE) = RES_01.ID_03 then NULL else substr(FTC_LINEA,132,15) end Error_en_suma_de_saldo_de_vivienda_92
	,CASE WHEN CAST(substr(FTC_LINEA,147,15) AS DOUBLE) = RES_01.ID_04 then NULL else substr(FTC_LINEA,147,15) end Error_en_suma_de_intereses_de_vivienda_97
	,CASE WHEN CAST(substr(FTC_LINEA,162,15) AS DOUBLE) = RES_01.ID_05 then NULL else substr(FTC_LINEA,162,15) end Error_en_suma_de_intereses_de_vivienda_92
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01 RES_01 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) = '09'
	--AND substr(FTC_LINEA,1,2) != '02'












