--Plantilla: Layout de Respuesta Devolución de Pagos sin Justificacion Legal A-P v1.0 1
--Subproceso: 3832
--Ejemplo máscara nombre: PREFTD_20250828_72942_NOTDEV.TXT
WITH RES_01 AS(
	SELECT
		substr(FTC_LINEA,25,3) ID_09
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '01'
),RES_02 AS(
	SELECT
		COUNT(1) ID_13
		,SUM(CASE WHEN substr(FTC_LINEA,282,2) IN ('01','04') THEN 1 ELSE 0 END) ID_14
		,SUM(CASE WHEN substr(FTC_LINEA,282,2) IN ('02') THEN 1 ELSE 0 END) ID_15
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
),RES_08 AS(
	SELECT
		,SUM(CASE WHEN TRY_CAST(substr(FTC_LINEA,5,16) AS BIGINT) = 1 THEN TRY_CAST(substr(FTC_LINEA,44,15) AS BIGINT) ELSE 0 END) ID_10
		,SUM(CASE WHEN TRY_CAST(substr(FTC_LINEA,5,16) AS BIGINT) = 3 THEN TRY_CAST(substr(FTC_LINEA,161,15) AS BIGINT) ELSE 0 END) ID_11
		,SUM(CASE WHEN TRY_CAST(substr(FTC_LINEA,5,16) AS BIGINT) = 3 THEN TRY_CAST(substr(FTC_LINEA,107,18) AS BIGINT) ELSE 0 END) ID_19
		,COUNT(1) ID_17
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '08'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_Tipo_registro
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,3,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,3,2) end Error_Identificador_servicio
	,CASE WHEN substr(FTC_LINEA,5,2) = '56' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,5,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,5,2) end Error_Identificador_operacion
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,7,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,7,2) end Error_Tipo_entidad_origen
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,9,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,9,3) end Error_Clave_entidad_origen
	,CASE WHEN substr(FTC_LINEA,12,2) = '03' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,12,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,12,2) end Error_Tipo_entidad_destino
	,CASE WHEN substr(FTC_LINEA,14,3) = '001' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,14,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,14,3) end Error_Clave_entidad_destino
	,CASE WHEN substr(FTC_LINEA,17,8) = substr(regexp_extract(#NOMBRE_ARCHIVO#,'_[0-9]{8}_[0-9]{5}_[A-Z]{6}.TXT$', 0), 2, 8) else substr(FTC_LINEA,20,8) end Error_Fecha_creacion_lote
	,CASE WHEN substr(FTC_LINEA,25,3) = RES_01.ID_09 THEN NULL ELSE substr(FTC_LINEA,25,3) END Error_Consecutivo_dia
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,28,15) as BIGINT) = RES_08.ID_10 THEN NULL ELSE substr(FTC_LINEA,28,15) END Error_total_retiro_ces_vejez_dev
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,43,15) as BIGINT) = RES_08.ID_11 THEN NULL ELSE substr(FTC_LINEA,43,15) END Error_total_aport_patronal_viv_dev
	,CASE WHEN substr(FTC_LINEA,58,15) = REPEAT('0', 15) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,58,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,58,15) END Error_Num_total_cuotas_guber_dev
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,73,8) as BIGINT) = RES_02.ID_13 THEN NULL ELSE substr(FTC_LINEA,73,8) END Error_registros_aport_lote
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,81,6) as BIGINT) = RES_02.ID_14 THEN NULL ELSE substr(FTC_LINEA,81,6) END Error_Num_aport_acep_dev
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,87,6) as BIGINT) = RES_02.ID_15 THEN NULL ELSE substr(FTC_LINEA,87,6) END Error_Num_aport_rech
	,CASE WHEN substr(FTC_LINEA,93,2) = REPEAT('0', 2) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,93,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,93,2) END Error_Num_aport_pend
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,99,6) as BIGINT) = RES_08.ID_17 THEN NULL ELSE substr(FTC_LINEA,99,6) END Error_reg_cuentas_pagar
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,105,6) as BIGINT) = (RES_02.ID_13 + RES_08.ID_17) THEN NULL ELSE substr(FTC_LINEA,105,6) END Error_reg_cuentas_pagar
	,CASE WHEN TRY_CAST(substr(FTC_LINEA,111,19) as BIGINT) = RES_08.ID_19 THEN NULL ELSE substr(FTC_LINEA,111,19) END Error_Num_aplic_inter_viv_aport_viv_dev
	--,Filler
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01 ON 1 = 1
	JOIN RES_02 ON 1 = 1
	JOIN RES_08 ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'