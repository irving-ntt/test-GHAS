--Plantilla: Validacion_Layout_de_respuesta_Transferencias_FOVISSSTE_v1.1 1
--TRANSFERENCIAS DE ACREDITADOS FOVISSSTE
--Subproceso: 363
--Ejemplo máscara nombre: PREFT.DP.A01534.NOTDEV.GDG
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_Registro
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Error_Consecutivo_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,3) end Clave_de_entidad_emisora_del_movimiento
	,CASE WHEN substr(FTC_LINEA,16,1) in ('1','2','3') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,16,1))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,16,1) end Tipo_de_Trabajador
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,17,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,17,8) end Fecha_de_movimiento
	--,CURP_del_trabajador
	,CASE WHEN substr(FTC_LINEA,43,8) = REPEAT('0', 8) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,43,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,43,8) end ID_Procesar
	--,NSS_del_trabajador
	,CASE WHEN substr(FTC_LINEA,62,2) in ('01','02') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,62,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,62,2) end Indicador_de_Aceptacion
	,CASE WHEN substr(FTC_LINEA,64,2) in ('08','13','14','  ') then NULL else substr(FTC_LINEA,64,2) end Motivo_de_Devolucion
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,66,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,66,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,66,15) end Numero_de_Aplicacion_de_Intereses_del_Fondo_de_la_Vivienda_FOVISSSTE_92
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,81,20), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,81,20))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,81,20) end Valor_de_la_Aplicacion_de_Intereses_del_Fondo_de_la_Vivienda_FOVISSSTE_92
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,101,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,101,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,101,15) end Importe_total_del_Fondo_de_la_vivienda_FOVISSSTE__92
	,CASE WHEN substr(FTC_LINEA,116,2) in ('  ','01') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,116,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,116,2) end Indicador_de_diferencia_en_vivienda_FOVISSSTE_92
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,118,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,118,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,118,15) end Numero_de_Aplicacion_de_Intereses_del_Fondo_de_la_Vivienda_FOVISSSTE_2008
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,133,20), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,133,20))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,133,20) end Valor_de_la_Aplicacion_de_Intereses_del_Fondo_de_la_Vivienda_FOVISSSTE_2008
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,153,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,153,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,153,15) end Importe_total_del_Fondo_de_la_vivienda_FOVISSSTE_2008
	,CASE WHEN substr(FTC_LINEA,168,2) in ('  ','01') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,168,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,168,2) end Indicador_de_diferencia_en_vivienda_FOVISSTE_2008
	,CASE WHEN substr(FTC_LINEA,170,2) in ('01','02', '  ') then NULL else substr(FTC_LINEA,170,2) end Codigo_Resultado_de_la_Operacion
	,CASE WHEN substr(FTC_LINEA,172,3) in ('   ') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,172,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,172,3) end Diagnostico_del_Proceso
	--,CASE WHEN substr(FTC_LINEA,175,556) = REPEAT(' ', 556) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,175,556))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,175,556) end Filler
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'