WITH RES_01 AS
(
	SELECT
		COUNT(1) ID_01
		,SUM(CAST(substr(FTC_LINEA,475,15) AS DOUBLE)) ID_02 -- Número de aplicaciones de intereses del fondo de la vivienda 92
		,SUM(CAST(substr(FTC_LINEA,510,15) AS DOUBLE)) ID_03 -- Importe total del fondo de la vivienda 92
		,SUM(CAST(substr(FTC_LINEA,542,15) AS DOUBLE)) ID_04 -- Número de aplicación de intereses del fondo de la vivienda 2008
		,SUM(CAST(substr(FTC_LINEA,577,15) AS DOUBLE)) ID_05 -- Importe total del fondo de la vivienda 2008
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_09
	,CASE WHEN CAST(substr(FTC_LINEA,3,9) AS INT) = RES_01.ID_01 then NULL else substr(FTC_LINEA,3,9) end Error_en_cantidad_de_registros_de_detalle
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01 RES_01 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'
