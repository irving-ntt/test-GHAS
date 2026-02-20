--Plantilla: LayoutUGAceptados
--USO DE GARANTÍA POR 43 BIS
--Subproceso: 368
--Ejemplo máscara nombre: PTCFTA_20250807_001_DEVUSOG43.txt
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
  ,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,3,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,3,2) end Identificador_de_servicio
  ,CASE WHEN substr(FTC_LINEA,5,2) = '09' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,5,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,5,2) end Identificador_de_operacion
  ,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,7,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,7,2) end Tipo_de_entidad_origen
  ,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,9,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,9,3) end Clave_entidad_origen
  ,CASE WHEN substr(FTC_LINEA,12,2) = '04' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,12,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,12,2) end Tipo_entidad_destino
  ,CASE WHEN substr(FTC_LINEA,14,3) = '002' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,14,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,14,3) end Clave_entidad_destino
  ,CASE WHEN substr(FTC_LINEA,17,3) = '009' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,17,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,17,3) end Entidad_federativa_de_envio_de_lote
	--,CASE WHEN substr(FTC_LINEA,20,8) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{9,12}.TXT$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,20,8) end Error_Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,28,3) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{9,12}.TXT$', 0), 11, 3) THEN NULL else substr(FTC_LINEA,28,3) end Error_folio_del_lote
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,28,3), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,28,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,28,3) end Consecutivo_de_lote_en_el_dia
	--,CASE WHEN substr(FTC_LINEA,31,2) = REPEAT(' ', 2) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,31,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,31,2) end Filler
	--,CASE WHEN substr(FTC_LINEA,33,2) = REPEAT(' ', 2) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,33,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,33,2) end Codigo_de_resultado_de_la_Operacion
	--,CASE WHEN substr(FTC_LINEA,35,9) = REPEAT(' ', 9) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,35,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,35,9) end Motivo_de_rechazo_de_lote
	--,CASE WHEN substr(FTC_LINEA,44,687) = REPEAT(' ', 687) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,44,687))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,44,687) end Filler
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '01'