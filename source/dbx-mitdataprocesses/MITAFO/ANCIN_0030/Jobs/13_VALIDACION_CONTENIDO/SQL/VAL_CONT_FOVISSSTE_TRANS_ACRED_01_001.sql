--Plantilla: Validacion_Layout_de_respuesta_Transferencias_FOVISSSTE_v1.1 1
--TRANSFERENCIAS DE ACREDITADOS FOVISSSTE
--Subproceso: 363
--Ejemplo máscara nombre: PREFT.DP.A01534.NOTDEV.GDG
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_Registro
	,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,3,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,3,2) end Identificador_de_Servicio
	,CASE WHEN substr(FTC_LINEA,5,2) = '96' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,5,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,5,2) end Identificador_de_Operacion
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,7,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,7,2) end Tipo_de_entidad_origen
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,9,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,9,3) end Clave_de_entidad_origen
	,CASE WHEN substr(FTC_LINEA,12,2) = '03' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,12,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,12,2) end Tipo_de_entidad_destino
	,CASE WHEN substr(FTC_LINEA,14,3) = '001' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,14,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,14,3) end Clave_de_entidad_destino
	--,CASE WHEN substr(FTC_LINEA,17,3) = REPEAT(' ', 3) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,17,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,17,3) end Filler
	--,Fecha_de_presentacion
	--,Consecutivo_del_lote_en_el_dia
	,CASE WHEN substr(FTC_LINEA,31,2) = '02' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,31,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,31,2) end Clave_de_modalidad_de_recepcion
	--,CASE WHEN substr(FTC_LINEA,33,698) = REPEAT(' ', 698) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,33,698))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,33,698) end Filler
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '01'