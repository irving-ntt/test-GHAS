SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro__diferente_a_01
	,CASE WHEN substr(FTC_LINEA,3,2) = '01' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_del_servicio_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,5,2) = '15' then NULL else substr(FTC_LINEA,5,2) end Error_en_Identificador_de_operacion_diferente_a_15
	,CASE WHEN substr(FTC_LINEA,7,2) = '03' then NULL else substr(FTC_LINEA,7,2) end Error_en_Tipo_entidad_origen_diferente_a__03
	,CASE WHEN substr(FTC_LINEA,9,3) = '001' then NULL else substr(FTC_LINEA,9,3) end Error_en_Clave_entidad_origen_diferente_a__001
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end Error_en_Tipo_entidad_destino_diferente_a__01
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end Error_en_Clave_entidad_destino_diferente_a_Clave_de_la_AFORE_a_534
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,20,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,20,8) end Error_en_Fecha_de_presentacion_erroneo
	,CASE WHEN substr(FTC_LINEA,28,3) != '000' then NULL else substr(FTC_LINEA,28,3) end Error_en_Consecutivo_del_lote_en_el_dia_erroneo_000
	,CASE WHEN substr(FTC_LINEA,31,2) = '02' then NULL else substr(FTC_LINEA,31,2) end Error_en_Modalidad_de_recepcion_erronea
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'