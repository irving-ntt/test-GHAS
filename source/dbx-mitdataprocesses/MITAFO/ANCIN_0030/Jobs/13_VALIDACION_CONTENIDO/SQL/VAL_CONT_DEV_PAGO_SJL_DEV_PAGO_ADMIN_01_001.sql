SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_registro_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL else substr(FTC_LINEA,3,2) end Error_en_campo_Identificador_de_servicio_diferente_a__03
	,CASE WHEN substr(FTC_LINEA,5,2) = '56' then NULL else substr(FTC_LINEA,5,2) end Error_en_campo_Identificador_de_operacion_diferente_a_56
	,CASE WHEN substr(FTC_LINEA,7,2) = '03' then NULL else substr(FTC_LINEA,7,2) end Error_en_campo_Tipo_de_entidad_origen_diferente_a_03
	,CASE WHEN substr(FTC_LINEA,9,3) = '001' then NULL else substr(FTC_LINEA,9,3) end Error_en_campo_Clave_entidad_origen_diferente_a__001
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end Error_en_campo_Tipo_entidad_destino_diferente_a__01
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end Error_en_campo_Clave_entidad_destino_diferente_a_534
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,17,8) end Error_en_campo_Fecha_de_creacion_del_lote_diferente_a_Fecha_con_formato_valido_AAAAMMDD
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,25,3), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,25,3) end Error_en_campo_Consecutivo_del_dia_diferente_a_dato_numerico
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,28,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,28,8) end Error_en_campo_Fecha_limite_de_respuesta_diferente_a_fecha_con_formato_valido_AAAAMMDD
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '08'
	AND substr(FTC_LINEA,1,2) != '09'