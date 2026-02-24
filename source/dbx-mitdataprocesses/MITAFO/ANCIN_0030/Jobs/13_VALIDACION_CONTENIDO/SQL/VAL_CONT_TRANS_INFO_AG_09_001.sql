WITH RES_02 AS
(
	SELECT
		COUNT(1) ID_02
		,SUM(CAST(substr(FTC_LINEA,490,15) AS DOUBLE)) ID_04
		,SUM(CAST(substr(FTC_LINEA,565,15) AS DOUBLE)) ID_06
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro
	,CASE WHEN CAST(substr(FTC_LINEA, 3, 9) AS DOUBLE) = RES_02.ID_02 then NULL else substr(FTC_LINEA,3,9) end Error_en_Cantidad_de_registros_de_detalle
	,CASE WHEN CAST(substr(FTC_LINEA, 57, 15) AS DOUBLE) = RES_02.ID_04 then NULL else substr(FTC_LINEA,57,15) end Error_en_Suma_de_aplicaciones_de_Intereses_de_Vivienda_92
	,CASE WHEN CAST(substr(FTC_LINEA, 132, 15) AS DOUBLE) = RES_02.ID_06 then NULL else substr(FTC_LINEA,132,15) end Error_en_Suma_de_aplicaciones_de_Intereses_de_Vivienda_97
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'