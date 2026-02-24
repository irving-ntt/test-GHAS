WITH RES_01 AS
(
	SELECT
		COUNT(1) ID_01
		,SUM(CAST(substr(FTC_LINEA,475,15) AS DOUBLE)) ID_02 -- Número de aplicaciones de interes de viv 97 de la última aportación
		,SUM(CAST(substr(FTC_LINEA,490,15) AS DOUBLE)) ID_03 -- última aportación de vivienda 97
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '02'
)





SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_registro_diferente_a_09
	,CASE WHEN CAST(substr(FTC_LINEA,3,9) AS INT) = RES_01.ID_01 then NULL else substr(FTC_LINEA,3,9) end Error_en_cantidad_de_registros_de_detalle
	,CASE WHEN CAST(substr(FTC_LINEA,42,15) AS DOUBLE) = RES_01.ID_02 then NULL else substr(FTC_LINEA,42,15) end Error_en_campo_Suma_de_numero_de_aplicaciones_de_intereses_de_vivienda_97_de_la_ultima_aportacion
	,CASE WHEN CAST(substr(FTC_LINEA,60,15) AS DOUBLE) = RES_01.ID_03 then NULL else substr(FTC_LINEA,60,15) end Error_en_campo_Suma_de_la_ultima_aportacion_de_vivienda_97
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01 RES_01 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'


