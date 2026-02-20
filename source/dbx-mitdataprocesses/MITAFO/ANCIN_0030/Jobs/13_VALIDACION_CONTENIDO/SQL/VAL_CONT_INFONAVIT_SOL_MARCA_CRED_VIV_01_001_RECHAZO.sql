SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL else substr(FTC_LINEA,3,2) end Identificador_de_servicio
	,CASE WHEN substr(FTC_LINEA,5,2) = '09' then NULL else substr(FTC_LINEA,5,2) end Indicador_de_operacion     ------------------- en el archivo se ,meciona que debe ser 06
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL else substr(FTC_LINEA,7,2) end Tipo_de_entidad_origen     ------------------- en el archivo se menciona que debe ser 04
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL else substr(FTC_LINEA,9,3) end Clave_de_entidad_origen   ------------------- en el archivo se menciona que debe ser 002
	,CASE WHEN substr(FTC_LINEA,12,2) = '04' then NULL else substr(FTC_LINEA,12,2) end Tipo_de_entidad_destino  ------------------- en el archivo se menciona que debe ser 01
	,CASE WHEN substr(FTC_LINEA,14,3) = '002' then NULL else substr(FTC_LINEA,14,3) end Clave_de_entidad_destino  ----------------- en el archivo se menciona que debe ser 534
	,CASE WHEN substr(FTC_LINEA,17,3) = '009' then NULL else substr(FTC_LINEA,17,3) end Clave_de_entidad_federativa_lote  ----------------- en el archivo se menciona que debe ser 009
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,20,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,20,8) end fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,20,8) = substr(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{10}.TXT$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,20,8) end Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,28,3) = substr(regexp_extract(#NOMBRE_ARCHIVO#,'_[0-9]{8}_[0-9]{3}_[A-Z0-9]{10}.TXT$', 0), 11, 3) else substr(FTC_LINEA,28,3) end Error_folio_del_lote
	--,CASE WHEN substr(FTC_LINEA,31,700) = REPEAT(' ', 700) then NULL else substr(FTC_LINEA,31,700) end Filler_02
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'