SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end `Error_en_Tipo_de_registro__diferente_a_'01'`
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL else substr(FTC_LINEA,3,2) end `Error_en_Identificador_del_servicio_diferente_a_'03'`
	,CASE WHEN substr(FTC_LINEA,5,2) = '10' then NULL else substr(FTC_LINEA,5,2) end `Error_en_Identificador_de_operacion_diferente_a_'10'`
	,CASE WHEN substr(FTC_LINEA,7,2) = '03' then NULL else substr(FTC_LINEA,7,2) end `Error_en_Tipo_entidad_origen_diferente_a__'03'`
	,CASE WHEN substr(FTC_LINEA,9,3) = '001' then NULL else substr(FTC_LINEA,9,3) end `Error_en_Clave_entidad_origen_diferente_a__'001'`
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end `Error_en_Tipo_entidad_destino_diferente_a__'01'`
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end `Error_en_Clave_entidad_destino_diferente_a_Clave_de_la_AFORE_a_'534'`
	,CASE WHEN TO_DATE(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,17,8) end `Error_en_Formato_de_fecha_erroneo_`
	,CASE WHEN substr(FTC_LINEA,25,3) != '000' then NULL else substr(FTC_LINEA,25,3) end `Error_en_Consecutivo_erroneo_000`
	,CASE WHEN substr(FTC_LINEA,28,2) in ('01','02','05') then NULL else substr(FTC_LINEA,28,2) end `Error_en_Modalidad_de_recepcion_erronea`
	,CASE WHEN TO_DATE(substr(FTC_LINEA,30,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,30,8) end `Error_en_Formato_de_fecha_erroneo`
	,CASE WHEN substr(FTC_LINEA,320,1) = '0' then NULL else substr(FTC_LINEA,320,1) end `Error en Diferente a 0`
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '08'
	AND substr(FTC_LINEA,1,2) != '09'