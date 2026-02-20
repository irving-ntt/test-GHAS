SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL else substr(FTC_LINEA,3,2) end Error_en_campo_Identificador_del_servicio_diferente_a_02
	,CASE WHEN substr(FTC_LINEA,5,2) in ('55','56') then NULL else substr(FTC_LINEA,5,2) end Error_en_campo_Identificador_de_operacion_diferente_a_55_o_56 
	,CASE WHEN substr(FTC_LINEA,7,2) = '03' then NULL else substr(FTC_LINEA,7,2) end Error_en_campo_Tipo_entidad_origen_diferente_a_03-- este debe ser 03
	,CASE WHEN substr(FTC_LINEA,9,3) = '001' then NULL else substr(FTC_LINEA,9,3) end Error_en_campo_clave_entidad_origen_diferente_a_001 
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end Error_en_campo_tipo_entidad_destino_diferente_a_01 -- este debe ser 01
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end Error_en_campo_Clave_entidad_destino_diferente_a_534 
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8),'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,17,8) end Error_en_campo_Fecha_de_Proceso_formato_de_fecha_invalida_formato_igual_a_AAAAMMDD
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'