WITH RES_02 AS(
	SELECT
		COUNT(1) ID_03
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_01
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,3,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,3,8) end Error_en_Formato_de_Fecha_envio
	,CASE WHEN CAST(substr(FTC_LINEA, 11, 9) AS DOUBLE) = RES_02.ID_03 then NULL else substr(FTC_LINEA,11,9) end Error_en_Numero_de_registros_vs_Detalle
	,CASE WHEN trim(substr(FTC_LINEA,20,109)) = '' then NULL else substr(FTC_LINEA,20, 109) end Error_en_Filler
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_02
		ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '01'