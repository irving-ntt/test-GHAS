--Plantilla: Layout de Respuesta Devolución de Pagos sin Justificacion Legal A-P v1.0 1
--Subproceso: 3832
--Ejemplo máscara nombre: PREFTD_20250828_72942_NOTDEV.TXT
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Tipo_registro
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,3,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,3,2) end Identificador_servicio
	,CASE WHEN substr(FTC_LINEA,5,2) = '56' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,5,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,5,2) end Identificador_operacion
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,7,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,7,2) end Tipo_de_entidad_origen
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,9,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,9,3) end Clave_entidad_origen
	,CASE WHEN substr(FTC_LINEA,12,2) = '03' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,12,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,12,2) end Tipo_entidad_destino
	,CASE WHEN substr(FTC_LINEA,14,3) = '001' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,14,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,14,3) end Clave_entidad_destino
	--,CASE WHEN substr(FTC_LINEA,17,8) = substr(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{5}_[A-Z]{6}.TXT$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,20,8) end Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,28,3) = substr(regexp_extract(#NOMBRE_ARCHIVO#,'_[0-9]{8}_[0-9]{5}_[A-Z]{6}.TXT$', 0), 11, 3) else substr(FTC_LINEA,28,3) end Error_folio_del_lote
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,28,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,28,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,28,8) end Fecha_limite_de_resp
	--,Resultado_de_la_operacion
	--,Diagnostico_1
	--,Diagnostico_2
	--,Diagnostico_3
	--,Filler
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '01'