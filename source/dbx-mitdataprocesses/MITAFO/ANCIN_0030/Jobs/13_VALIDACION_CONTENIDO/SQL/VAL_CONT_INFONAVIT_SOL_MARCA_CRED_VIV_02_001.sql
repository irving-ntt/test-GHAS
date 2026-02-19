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
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_tipo_de_registro
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,3,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,3,10) end Error_en_contador_de_servicio
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Error_de_Consecutivo_de_registro_dentro_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL else substr(FTC_LINEA,13,2) end Error_en_tipo_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL else substr(FTC_LINEA,15,3) end Error_en_clave_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL else substr(FTC_LINEA,18,2) end Error_en_tipo_de_entidad_cedente_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL else substr(FTC_LINEA,20,3) end Error_en_clave_de_entidad_ced_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,23,2) = '16' then NULL else substr(FTC_LINEA,23,2) end Error_en_origen__Tipo_de_la_Transferencia
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,25,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,25,8) end Error_en_fecha_de_presentacion
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,59,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,59,11) end Error_en_campo_NSS_del_trabajador_segun_INFONAVIT

	,CASE WHEN substr(FTC_LINEA,240,16) = CONCAT(substr(FTC_LINEA,13,2),substr(FTC_LINEA,15,3),substr(FTC_LINEA,25,8),RES_01.ID_02) THEN NULL ELSE substr(FTC_LINEA,240,16) END Error_en_identificador_de_lote_de_la_solicitud
	
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,271,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,271,11) end Error_en_campo_NSS_del_trabajador_segun_AFORE
	,CASE WHEN substr(FTC_LINEA,490,15) = '000000000000000' then NULL else substr(FTC_LINEA,490,15) end Error_en_campo_saldo_de_vivienda_97
	,CASE WHEN substr(FTC_LINEA,565,15) = '000000000000000' then NULL else substr(FTC_LINEA,565,15) end Error_en_campo_saldo_de_vivienda_92
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,650,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,650,10) end Error_en_numero_de_credito_INFONAVIT
	,CASE WHEN substr(FTC_LINEA,660,15) = '000000000000000' then NULL else substr(FTC_LINEA,660,15) end Error_en_campo_intereses_de_saldo_de_vivienda_97
	,CASE WHEN substr(FTC_LINEA,675,15) = '000000000000000' then NULL else substr(FTC_LINEA,675,15) end Error_en_campo_intereses_de_saldo_de_vivienda_92
FROM #DELTA_TABLE_NAME_001#
JOIN RES_01 RES_01 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) = '02'
	--AND substr(FTC_LINEA,1,2) != '09'