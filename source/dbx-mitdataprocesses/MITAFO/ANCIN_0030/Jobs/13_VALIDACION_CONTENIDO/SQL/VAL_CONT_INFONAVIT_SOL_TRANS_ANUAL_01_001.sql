SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro
	,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_de_servicio
	,CASE WHEN substr(FTC_LINEA,5,2) = '01' then NULL else substr(FTC_LINEA,5,2) end Error_en_Identificador_operacion
	,CASE WHEN substr(FTC_LINEA,7,2) = '04' then NULL else substr(FTC_LINEA,7,2) end Error_en_Tipo_de_entidad_de_origen
	,CASE WHEN substr(FTC_LINEA,9,3) = '002' then NULL else substr(FTC_LINEA,9,3) end Error_en_Clave_de_entidad_origen
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end Error_en_Tipo_de_entidad_destino
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end Error_en_Clave_de_entidad_destino
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,20,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,20,8) end Error_en_Fecha_de_presentacion
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'