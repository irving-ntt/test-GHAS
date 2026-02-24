SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente__02
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL else substr(FTC_LINEA,13,2) end Error_en_campo_Tipo_de_entidad_receptora_de_la_cuenta_diferente_a_04
	,CASE WHEN substr(FTC_LINEA,15,3) = '004' then NULL else substr(FTC_LINEA,15,3) end Error_en_campo_Clave_de_entidad_receptora_de_la_cuenta_diferente_a_002
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL else substr(FTC_LINEA,18,2) end Error_en_campo_Tipo_de_entidad_cedente_de_la_cuenta_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL else substr(FTC_LINEA,20,3) end Error_en_campo_Clave_de_entidad_ced_de_la_cuenta_diferente_a_Siempre_534
	,CASE WHEN substr(FTC_LINEA,23,2) = '05' then NULL else substr(FTC_LINEA,23,2) end Error_en_campo_Origen__Tipo_de_la_Transferencia_diferente_a_05
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,25,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,25,8) end Error_en_campo_Fecha_de_presentacion_diferente_a_formato_valido_AAAAMMDD
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,33,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,33,8) end Error_en_campo_Fecha_de_movimiento_diferente_a_formato_valido_AAAAMMDD
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,475,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,475,15) end Error_en_campo_Numero_de_aplicaciones_de_intereses_del_fondo_de_la_vivienda_92_diferente_a_formato_valido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,490,20), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,490,20) end Error_en_campo_Valor_de_la_aplicacion_de_intereses_del_fondo_de_la_vivienda_92_diferente_a_formato_
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,510,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,510,15) end Error_en_campo_Importe_total_del_fondo_de_la_vivienda_92_diferente_a_formato_valido_
	,CASE WHEN substr(FTC_LINEA,540,2) in ('01','02') then NULL else substr(FTC_LINEA,540,2) end Error_en_campo_Tipo_de_movimiento_diferente_a_01__02
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,542,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,542,15) end Error_en_campo_Numero_de_aplicacion_de_intereses_del_fondo_de_la_vivienda_2008_diferente_a_formato_valido_
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,557,20), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,557,20) end Error_en_campo_Valor_de_la_aplicacion_de_intereses_del_fondo_de_la_vivienda_2008_diferente_a_formato_valido_
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,577,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,577,15) end Error_en_campo_Importe_total_del_fondo_de_la_vivienda_2008_diferente_a_formato_valido_
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '09'