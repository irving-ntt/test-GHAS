WITH RES_01 AS
(
	SELECT
		COUNT(1) ID_01
		,SUM(CAST(substr(FTC_LINEA,167,9) AS DOUBLE)) ID_02 -- Importe_cesantia_y_vejes_cuota_patron_a_devolver
		,SUM(CAST(substr(FTC_LINEA,176,9) AS DOUBLE)) ID_03 -- Importe_cesantia_y_vejes_cuota_trabajador_a_devolver
		,SUM(CAST(substr(FTC_LINEA,221,9) AS DOUBLE)) ID_04 -- Importe_de_aportacion_patronal_de_vivienda_a_devolver
		--,SUM(CAST(substr(FTC_LINEA,) AS DOUBLE)) ID_05 -- PENDIENTE
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '02'
)

SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_registro_diferente_a__09
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL else substr(FTC_LINEA,3,2) end Error_en_campo_Identificador_de_servicio_diferente_a__03
	,CASE WHEN substr(FTC_LINEA,5,2) = '56' then NULL else substr(FTC_LINEA,5,2) end Error_en_campo_Identificador_de_operacion_diferente_a_56
	,CASE WHEN substr(FTC_LINEA,7,2) = '03' then NULL else substr(FTC_LINEA,7,2) end Error_en_campo_Tipo_de_entidad_origen_diferente_a_03
	,CASE WHEN substr(FTC_LINEA,9,3) = '001' then NULL else substr(FTC_LINEA,9,3) end Error_en_campo_Clave_de_entidad_origen_diferente_a_001
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end Error_en_campo_Tipo_de_entidad_destino_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end Error_en_campo_Clave_de_entidad_destino_diferente_a_534
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,17,8) end Error_en_campo_Fecha_creacion_de_lote_diferente_a_fecha_con_formato_valido_AAAAMMDD
	
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,28,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,28,15) end Error_en_campo_Importe_total_retiro_cesantia_y_vejez_a_devolver_diferente_a_dato_numerico
	
	
	,CASE WHEN CAST(substr(FTC_LINEA,28,15) AS DOUBLE) = (RES_01.ID_02 + RES_01.ID_03) then NULL else substr(FTC_LINEA,28,15) end Error_en_campo_Importe_total_retiro_cesantia_y_vejez_a_devolver_sumatoria_erronea

	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,43,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,43,15) end Error_en_campo_Importe_total_aportacion_patronal_vivienda_a_devolver_diferente_a_dato_numerico
	,CASE WHEN CAST(substr(FTC_LINEA,43,15) AS DOUBLE) = RES_01.ID_04 then NULL else substr(FTC_LINEA,43,15) end Error_en_campo_Importe_total_aportacion_patronal_vivienda_a_devolver_sumatoria_erronea

	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,58,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,58,15) end Error_en_campo_Numero_total_de_cuotas_gubernamentales_a_devolver_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,73,8), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,73,8) end Error_en_campo_Numero_de_registros_aportaciones_dentro_del_lote_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,81,6), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,81,6) end Error_en_campo_Numero_de_aportaciones_aceptadas_a_devolver_diferente_a_Dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,87,6), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,87,6) end Error_en_campo_Numero_de_aportaciones_rechazadas_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,93,2), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,93,2) end Error_en_campo_Numero_de_aportaciones_pendientes_diferente_a_dato_numerico
	
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,99,6), '[0-9]*', '')) > 0 then NULL else substr(FTC_LINEA,99,6) end Error_en_campo_Numero_de_registros_cuentas_por_pagar
	
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,105,6), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,105,6) end Error_en_campo_Numero_total_de_registros_dentro_del_lote_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,111,19), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,111,19) end Error_en_campo_Numero_de_aplicaciones_de_intereses_de_vivienda_por_aportaciones_de_vivienda_a_devolver_diferente_a_dato_numerico

FROM #DELTA_TABLE_NAME_001#
left join
	RES_01 RES_01
ON
	1=1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '08'
