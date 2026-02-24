WITH RES_01 AS
(
	SELECT
		COUNT(1) ID_01
		,SUM(CAST(substr(FTC_LINEA,490,15) AS DOUBLE)) ID_02 -- Saldo de Vivienda 97
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN CAST(substr(FTC_LINEA,3,9) AS INT) = RES_01.ID_01 then NULL else substr(FTC_LINEA,3,9) end Cantidad_de_registros_de_detalle
	,CASE WHEN substr(FTC_LINEA,12,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,12,30) end Filler_01
	,CASE WHEN CAST(substr(FTC_LINEA,60,15) AS DOUBLE) = RES_01.ID_02 then NULL else substr(FTC_LINEA,60,15) end Suma_de_saldo_de_vivienda_97
	,CASE WHEN substr(FTC_LINEA,75,656) = REPEAT(' ', 656) then NULL else substr(FTC_LINEA,72,656) end Filler_02
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01 RES_01 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) = '09'
	--AND substr(FTC_LINEA,1,2) != '02'









