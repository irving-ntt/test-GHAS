SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,3,2) = '09' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_del_servicio_diferente_a_09
	,CASE WHEN substr(FTC_LINEA,5,2) = '71' then NULL else substr(FTC_LINEA,5,2) end Error_en_Identificador_de_operacion_diferente_a_71
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL else substr(FTC_LINEA,7,2) end Error_en_Tipo_entidad_origen_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL else substr(FTC_LINEA,9,3) end Error_en_Clave_entidad_origen_diferente_a_534
	,CASE WHEN substr(FTC_LINEA,12,2) = '03' then NULL else substr(FTC_LINEA,12,2) end Error_en_Tipo_entidad_destino_diferente_a_03
	,CASE WHEN substr(FTC_LINEA,14,3) = '001' then NULL else substr(FTC_LINEA,14,3) end Error_en_Clave_entidad_destino_diferente_a_Clave_de_la_AFORE_a_001
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,17,8) end Error_en_Formato_de_Fecha_de_creacion_del_Lote
	,CASE WHEN substr(FTC_LINEA,25,3) = '000' then NULL else substr(FTC_LINEA,25,3) end Error_en_Consecutivo_erroneo_000
	,CASE WHEN substr(FTC_LINEA,28,2) = '02' then NULL else substr(FTC_LINEA,28,2) end Error_en_Modalidad_de_recepcion_erronea
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,30,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,30,8) end Error_en_Formato_de_Fecha_limite_de_respuesta_01
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,38,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,38,8) end Error_en_Formato_de_Fecha_limite_de_respuesta_02
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,46,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,46,8) end Error_en_Formato_de_Fecha_limite_de_respuesta_03
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,54,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,54,8) end Error_en_Formato_de_Fecha_limite_de_respuesta_04
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '01'