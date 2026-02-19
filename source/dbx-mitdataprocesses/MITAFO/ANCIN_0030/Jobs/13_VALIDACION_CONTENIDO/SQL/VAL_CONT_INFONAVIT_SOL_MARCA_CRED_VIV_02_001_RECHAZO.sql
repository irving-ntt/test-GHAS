WITH RES_01 AS
(
	SELECT
		COUNT(1) ID_01
		,substr(FTC_LINEA,28,3) ID_02 -- Saldo de Vivienda 97
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '01'
	GROUP BY 
    FTN_NO_LINEA,
    substr(FTC_LINEA, 1, 2),
    substr(FTC_LINEA, 28, 3)	
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end tipo_de_registro
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,3,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,3,10) end contador_de_servicio
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Consecutivo_de_registro_dentro_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL else substr(FTC_LINEA,13,2) end tipo_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL else substr(FTC_LINEA,15,3) end clave_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL else substr(FTC_LINEA,18,2) end tipo_de_entidad_cedente_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL else substr(FTC_LINEA,20,3) end clave_de_entidad_ced_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,23,2) = '16' then NULL else substr(FTC_LINEA,23,2) end origen__Tipo_de_la_Transferencia
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,25,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,25,8) end fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,33,8) = REPEAT(' ',8) then NULL else substr(FTC_LINEA,33,8) end Filler_01
	--,CASE WHEN substr(FTC_LINEA,240,16) = CONCAT(substr(FTC_LINEA,13,2),substr(FTC_LINEA,15,3),substr(FTC_LINEA,25,8),RES_01.ID_02) THEN NULL ELSE substr(FTC_LINEA,240,16) END identificador_de_lote_de_la_solicitud
	,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filler_02
	--,CASE WHEN substr(FTC_LINEA,490,15) = REPEAT('0', 15) then NULL else substr(FTC_LINEA,490,15) END Saldo_Vivienda_97
	,CASE WHEN substr(FTC_LINEA,505,78) = REPEAT(' ', 78) then NULL else substr(FTC_LINEA,505,78) end Filler_03
	,CASE WHEN substr(FTC_LINEA,703,2) in ('04','08','11','16','19','20') then NULL else substr(FTC_LINEA,703,2) end Motivo_Devolucion
	,CASE WHEN substr(FTC_LINEA,705,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,705,8) end Filler_04
	,CASE WHEN substr(FTC_LINEA,719,12) = REPEAT(' ', 12) then NULL else substr(FTC_LINEA,719,12) end Filler_05
FROM #DELTA_TABLE_NAME_001#
JOIN RES_01 RES_01 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) = '02'
	--AND substr(FTC_LINEA,1,2) != '09'