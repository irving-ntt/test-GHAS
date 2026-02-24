SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL else substr(FTC_LINEA,3,2) end Error_en_campo_Identificador_de_servicio_diferente_a_02
	,CASE WHEN substr(FTC_LINEA,5,2) = '01' then NULL else substr(FTC_LINEA,5,2) end Error_en_campo_Identificador_de_operacion_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,7,2) = '04' then NULL else substr(FTC_LINEA,7,2) end Error_en_campo_Tipo_de_entidad_origen_diferente_a_04
	,CASE WHEN substr(FTC_LINEA,9,3) = '002' then NULL else substr(FTC_LINEA,9,3) end Error_en_campo_Clave_de_entidad_origen_diferente_a_002
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end Error_en_campo_Tipo_de_entidad_destino_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end Error_en_campo_Clave_de_entidad_destino_diferente_a_534
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,20,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,20,8) end Error_en_campo_Fecha_de_Presentacion_diferente_a_formato_valido_AAAAMMDD
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'