--Plantilla:LayoutRespuestaPorta
--TRANSFERENCIA DE RECURSOS POR PORTABILIDAD
--Subproceso: 3286
--Ejemplo máscara nombre: PTCFT.DP.A01534.PORTATRA.GDG
WITH RES_02 AS(
	SELECT
		COUNT(1) ID_09
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,3,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,3,2) end Identificador_del_servicio
	,CASE WHEN substr(FTC_LINEA,5,2) = '56' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,5,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,5,2) end Identificador_de_operacion
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,7,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,7,2) end Tipo_entidad_origen
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,9,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,9,3) end Clave_entidad_origen
	,CASE WHEN substr(FTC_LINEA,12,2) = '03' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,12,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,12,2) end Tipo_entidad_destino
	,CASE WHEN substr(FTC_LINEA,14,3) = '001' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,14,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,14,3) end Clave_entidad_destino
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,17,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,17,8) end Fecha_de_proceso
	,CASE WHEN CAST(substr(FTC_LINEA, 25, 10) AS BIGINT) = RES_02.ID_09 then NULL else substr(FTC_LINEA,25,10) end Error_Cantidad_registros_detalle
	--,Filler
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_02 ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'