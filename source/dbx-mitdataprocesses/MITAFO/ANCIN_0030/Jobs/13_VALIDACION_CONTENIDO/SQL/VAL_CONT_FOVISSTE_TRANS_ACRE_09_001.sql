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
	,CASE WHEN CAST(substr(FTC_LINEA,42,18) AS DOUBLE) = RES_01.ID_02 then NULL else substr(FTC_LINEA,42,18) end Error_en_campo_Suma_de_numero_de_aplicaciones_de_intereses_de_vivienda_92
	,CASE WHEN CAST(substr(FTC_LINEA,60,15) AS DOUBLE) = RES_01.ID_03 then NULL else substr(FTC_LINEA,60,15) end Error_en_campo_Suma_de_importe_total_del_fondo_de_la_vivienda_92
	,CASE WHEN CAST(substr(FTC_LINEA,75,18) AS DOUBLE) = RES_01.ID_04 then NULL else substr(FTC_LINEA,75,18) end Error_en_campo_Suma_del_numero_de_Aplicacion_de_Intereses_de_vivienda_2008
	,CASE WHEN CAST(substr(FTC_LINEA,93,15) AS DOUBLE) = RES_01.ID_05 then NULL else substr(FTC_LINEA,93,15) end Error_en_campo_Suma_de_importe_total_del_fondo_de_vivienda_2008
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01 RES_01 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'
